package gateway

import (
	"context"
	"sync"
	"time"

	cav1 "github.com/iodesystems/gwag/v2/gw/proto/callerauth/v1"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// callerAuthorizerNamespace is the reserved registration namespace for
// the CallerAuthorizer delegate. A service implementing Authorize
// registers under "_caller_auth/v1" and the gateway's delegated
// caller-id extractor routes inbound tokens to it.
const callerAuthorizerNamespace = "_caller_auth"

// callerRevokedSubject is the NATS subject the gateway listens on for
// negative cache invalidation. Delegate-side code publishes a marshaled
// callerauth.v1.TokenRevoked when a token should be evicted across the
// cluster — every gateway drops the matching cache entry.
const callerRevokedSubject = "events.caller_auth.Revoked"

// Delegated caller-id wire field names. The gateway reads these from
// HTTP headers / gRPC metadata, hands the raw token to the
// CallerAuthorizer delegate, and labels metrics with the resolved
// caller_id. Token never lands in metrics — caller_id is the stable
// label across rotations.
const (
	DelegatedCallerIDTokenHeader   = "X-Caller-Token"
	DelegatedCallerIDTokenMetadata = "caller-token"
)

// CallerIDDelegatedOptions configures the delegated caller-id extractor.
//
// The extractor reads an opaque token from the inbound request
// (X-Caller-Token / caller-token), looks it up in a local TTL cache,
// and on a miss calls _caller_auth/v1::Authorize to resolve it. Cache
// hit rate target is >99.9 % under steady-state traffic; misses are
// collapsed via singleflight so a token-rotation thundering herd
// produces one delegate RPC per (gateway, token), not one per request.
//
// Stability: stable
type CallerIDDelegatedOptions struct {
	// TTL is the default cache lifetime for resolved tokens. Zero →
	// 60 s. The delegate can override per-token via
	// AuthorizeResponse.ttl_seconds.
	TTL time.Duration

	// MaxTTL caps the delegate's per-token override so a misbehaving
	// delegate can't pin a stale identity forever. Zero → 30 minutes.
	MaxTTL time.Duration

	// NegativeTTL caches DENIED responses for this long so a bad-token
	// flood doesn't replay through the delegate on every request. Zero
	// → 30 s. Caching is intentional: DENIED stays DENIED until the
	// delegate publishes a TokenRevoked event or the negative entry
	// ages out.
	NegativeTTL time.Duration

	// Timeout caps each Authorize RPC. Zero → 3 s.
	Timeout time.Duration
}

// WithCallerIDDelegated installs the delegated caller-id extractor.
// The gateway pulls an opaque token off X-Caller-Token / caller-token,
// resolves it via the CallerAuthorizer delegate registered under
// _caller_auth/v1, and caches the result for opts.TTL (default 60 s)
// so the hot path stays sub-microsecond after the first call.
//
// Delegate code policy mirrors AdminAuthorizer / SubscriptionAuthorizer:
//   - OK: resp.caller_id is the resolved identity; cached for the TTL.
//   - DENIED: resolves to anonymous now; cached as DENIED for NegativeTTL.
//     Once WithCallerIDEnforce ships, DENIED short-circuits with 401.
//   - UNAVAILABLE / NOT_CONFIGURED / UNSPECIFIED / transport error:
//     resolves to anonymous; no cache write — every request retries.
//
// Cluster-mode bonus: gateways listen on `events.caller_auth.Revoked`
// for cluster-wide cache eviction. Delegate code publishes a marshaled
// callerauth.v1.TokenRevoked to nuke a single token everywhere the
// instant a credential is revoked, without waiting for TTL.
//
// Token presence is the trigger. Anonymous (no X-Caller-Token at all)
// resolves to "" → recorded as "unknown" — matches the Public / HMAC
// flavors' anonymous handling and stays compatible with the eventual
// WithCallerIDEnforce surface.
//
// Plan §Caller-ID delegated mode.
//
// Stability: stable
func WithCallerIDDelegated(opts CallerIDDelegatedOptions) Option {
	return func(cfg *config) { cfg.callerIDDelegated = &opts }
}

// callerAuthDelegate is the per-gateway state for the delegated
// extractor: TTL cache + singleflight + lifecycle bookkeeping for the
// revoke listener. One instance is built in New() when
// WithCallerIDDelegated is set; the extractor is a closure over it.
type callerAuthDelegate struct {
	gw   *Gateway
	opts CallerIDDelegatedOptions

	mu    sync.RWMutex
	cache map[string]callerCacheEntry

	sf singleflight.Group

	// revokeSubDone closes when the revoke-listener goroutine returns;
	// set during startRevokeListener. nil when no cluster is attached
	// (standalone mode — TTL is the only invalidation path).
	revokeSubDone chan struct{}
	revokeSub     *nats.Subscription
}

type callerCacheEntry struct {
	callerID  string
	expiresAt time.Time
	denied    bool
}

// newCallerAuthDelegate wires the delegate state and starts the
// revoke listener if the gateway is in cluster mode. Safe to call
// even when cluster is nil — standalone deployments lose cross-node
// invalidation but everything else works.
func newCallerAuthDelegate(g *Gateway, opts CallerIDDelegatedOptions) *callerAuthDelegate {
	d := &callerAuthDelegate{
		gw:    g,
		opts:  opts,
		cache: make(map[string]callerCacheEntry),
	}
	d.startRevokeListener()
	return d
}

func (d *callerAuthDelegate) ttl() time.Duration {
	if d.opts.TTL > 0 {
		return d.opts.TTL
	}
	return 60 * time.Second
}

func (d *callerAuthDelegate) maxTTL() time.Duration {
	if d.opts.MaxTTL > 0 {
		return d.opts.MaxTTL
	}
	return 30 * time.Minute
}

func (d *callerAuthDelegate) negativeTTL() time.Duration {
	if d.opts.NegativeTTL > 0 {
		return d.opts.NegativeTTL
	}
	return 30 * time.Second
}

func (d *callerAuthDelegate) timeout() time.Duration {
	if d.opts.Timeout > 0 {
		return d.opts.Timeout
	}
	return 3 * time.Second
}

// resolve is the extractor body. Reads the inbound token, consults the
// cache, falls through to the delegate via singleflight, applies the
// configured TTLs. See WithCallerIDDelegated for the surface contract.
func (d *callerAuthDelegate) resolve(ctx context.Context) (string, error) {
	token, remoteAddr := readDelegatedCallerToken(ctx)
	if token == "" {
		// Anonymous request — no delegate call.
		return "", nil
	}
	if entry, ok := d.lookup(token); ok {
		if entry.denied {
			// Cached DENIED. Caller resolves to anonymous today; once
			// enforce mode ships this branch promotes to 401.
			return "", nil
		}
		return entry.callerID, nil
	}
	v, _, _ := d.sf.Do(token, func() (any, error) {
		return d.callDelegate(ctx, token, remoteAddr), nil
	})
	entry := v.(callerCacheEntry)
	if entry.denied || entry.callerID == "" {
		return "", nil
	}
	return entry.callerID, nil
}

func (d *callerAuthDelegate) lookup(token string) (callerCacheEntry, bool) {
	d.mu.RLock()
	entry, ok := d.cache[token]
	d.mu.RUnlock()
	if !ok {
		return callerCacheEntry{}, false
	}
	if time.Now().After(entry.expiresAt) {
		d.mu.Lock()
		// Re-check under write lock; another goroutine may have
		// already refreshed.
		if cur, stillThere := d.cache[token]; stillThere && time.Now().After(cur.expiresAt) {
			delete(d.cache, token)
		}
		d.mu.Unlock()
		return callerCacheEntry{}, false
	}
	return entry, true
}

func (d *callerAuthDelegate) store(token string, entry callerCacheEntry) {
	d.mu.Lock()
	d.cache[token] = entry
	d.mu.Unlock()
}

func (d *callerAuthDelegate) evict(token string) {
	d.mu.Lock()
	delete(d.cache, token)
	d.mu.Unlock()
}

// callDelegate is invoked under singleflight: at most one in-flight
// RPC per (gateway, token). Returns the cache entry to install (caller
// reads .denied to decide on anonymous-vs-resolved). Falls back to a
// non-caching empty entry when the delegate is registered but not
// answering — TTL=0 ensures the next request retries.
func (d *callerAuthDelegate) callDelegate(ctx context.Context, token, remoteAddr string) callerCacheEntry {
	pool, ok := d.gw.lookupPool(callerAuthorizerNamespace, "v1")
	if !ok {
		return callerCacheEntry{}
	}
	rep := pool.pickReplica()
	if rep == nil || rep.conn == nil {
		return callerCacheEntry{}
	}
	client := cav1.NewCallerAuthorizerClient(rep.conn)
	dctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	resp, err := client.Authorize(dctx, &cav1.AuthorizeRequest{
		Token:      token,
		RemoteAddr: remoteAddr,
	})
	if err != nil {
		return callerCacheEntry{}
	}
	switch resp.GetCode() {
	case cav1.CallerAuthCode_CALLER_AUTH_CODE_OK:
		ttl := d.ttl()
		if hint := time.Duration(resp.GetTtlSeconds()) * time.Second; hint > 0 {
			ttl = hint
		}
		if max := d.maxTTL(); ttl > max {
			ttl = max
		}
		entry := callerCacheEntry{
			callerID:  resp.GetCallerId(),
			expiresAt: time.Now().Add(ttl),
		}
		if entry.callerID != "" {
			d.store(token, entry)
		}
		return entry
	case cav1.CallerAuthCode_CALLER_AUTH_CODE_DENIED:
		entry := callerCacheEntry{
			denied:    true,
			expiresAt: time.Now().Add(d.negativeTTL()),
		}
		d.store(token, entry)
		return entry
	default:
		// UNAVAILABLE / NOT_CONFIGURED / UNSPECIFIED — anonymous now,
		// no cache write, retry on the next request.
		return callerCacheEntry{}
	}
}

// startRevokeListener subscribes to events.caller_auth.Revoked when the
// gateway is in cluster mode. Drops the matching cache entry on every
// gateway in the cluster the instant a delegate publishes — no waiting
// for TTL.
func (d *callerAuthDelegate) startRevokeListener() {
	cl := d.gw.cfg.cluster
	if cl == nil || cl.Conn == nil {
		return
	}
	done := make(chan struct{})
	sub, err := cl.Conn.Subscribe(callerRevokedSubject, func(m *nats.Msg) {
		var rev cav1.TokenRevoked
		if err := proto.Unmarshal(m.Data, &rev); err != nil {
			return
		}
		if rev.GetToken() == "" {
			return
		}
		d.evict(rev.GetToken())
	})
	if err != nil {
		close(done)
		return
	}
	d.revokeSub = sub
	d.revokeSubDone = done
	go func() {
		<-d.gw.life.Done()
		_ = sub.Drain()
		close(done)
	}()
}

// readDelegatedCallerToken pulls the opaque caller token from HTTP
// headers or gRPC metadata, plus the inbound RemoteAddr for delegate
// logging. HTTP wins when both are present — same precedence as the
// Public / HMAC extractors.
func readDelegatedCallerToken(ctx context.Context) (token, remoteAddr string) {
	if r := HTTPRequestFromContext(ctx); r != nil {
		if v := r.Header.Get(DelegatedCallerIDTokenHeader); v != "" {
			return v, r.RemoteAddr
		}
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(DelegatedCallerIDTokenMetadata); len(v) > 0 && v[0] != "" {
			return v[0], ""
		}
	}
	return "", ""
}

// PublishCallerRevoked is the delegate-side helper for evicting a
// token across every gateway in the cluster. Operators wire this into
// their token-revocation flow alongside any auth-store update so the
// gateway picks up the change in milliseconds instead of waiting for
// TTL.
//
// Standalone (non-cluster) gateways have no cross-node fanout — this
// helper returns nil and the local TTL is the only invalidation path.
//
// Stability: stable
func PublishCallerRevoked(conn *nats.Conn, token string) error {
	if conn == nil || token == "" {
		return nil
	}
	payload, err := proto.Marshal(&cav1.TokenRevoked{Token: token})
	if err != nil {
		return err
	}
	return conn.Publish(callerRevokedSubject, payload)
}
