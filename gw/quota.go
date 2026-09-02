package gateway

import (
	"context"
	"sync"
	"time"

	qav1 "github.com/iodesystems/gwag/v2/gw/proto/quotaauth/v1"
	"github.com/iodesystems/gwag/v2/gw/ir"
	"golang.org/x/sync/singleflight"
)

// quotaAuthorizerNamespace is the reserved registration namespace for
// the QuotaAuthorizer delegate. A service implementing AcquireBlock
// registers under "_quota_auth/v1" and the gateway routes per-caller
// permit refills to it.
const quotaAuthorizerNamespace = "_quota_auth"

// QuotaOptions configures per-(caller, namespace, version) permit
// gating. When WithQuota is set, every dispatch crosses a local
// permit bucket; an empty bucket triggers an AcquireBlock RPC against
// _quota_auth/v1 (singleflight-collapsed). The delegate's response
// refills the bucket; explicit DENIED surfaces as 429 + Retry-After
// on HTTP/GraphQL ingress, ResourceExhausted on gRPC ingress.
//
// Default-allow: WithQuota not set → middleware is identity, zero
// overhead. Once installed, the gateway gates every dispatch by
// default; per-service opt-out is a followup (plan §Caller-ID +
// quota ladder).
//
// Fail-open posture: UNAVAILABLE / NOT_CONFIGURED / transport error
// grants a small emergency block (EmergencyPermits) so a degraded
// quota service doesn't brick the gateway. WithQuotaEnforce (planned
// followup) flips this to fail-closed.
//
// Stability: stable
type QuotaOptions struct {
	// BlockSize is the permit count the gateway asks the delegate for
	// on each refill. Zero → 100. The delegate may grant fewer.
	BlockSize uint32

	// MaxBlockSize caps the delegate's granted_permits so a
	// misbehaving delegate can't pin a huge block on a caller. Zero →
	// 10_000.
	MaxBlockSize uint32

	// EmergencyPermits is the fallback block size when the delegate
	// is unavailable. Zero → 1 (just enough to keep the gateway alive
	// without burning the delegate hot).
	EmergencyPermits uint32

	// MaxValidUntil caps a delegate's wall-clock window so a stale
	// validUntil far in the future doesn't pin a caller indefinitely.
	// Zero → 1 hour.
	MaxValidUntil time.Duration

	// EmergencyTTL is how long an emergency-granted block stays live
	// before the next refill attempt. Zero → 5 s.
	EmergencyTTL time.Duration

	// Timeout caps each AcquireBlock RPC. Zero → 3 s.
	Timeout time.Duration
}

// WithQuota installs the per-caller permit gate. The gateway crosses
// every dispatch through a local bucket keyed by (caller_id,
// namespace, version); empty buckets trigger a singleflight-
// collapsed AcquireBlock RPC against the QuotaAuthorizer delegate
// registered under _quota_auth/v1.
//
// Plan §Caller-ID + quota ladder.
//
// Stability: stable
func WithQuota(opts QuotaOptions) Option {
	return func(cfg *config) { cfg.quota = &opts }
}

// WithQuotaEnforce flips the WithQuota gate from fail-open to
// fail-closed: a delegate that's UNAVAILABLE / NOT_CONFIGURED /
// unreachable (transport error) / unregistered now surfaces as
// CodeResourceExhausted (HTTP 429 + Retry-After) instead of granting
// an EmergencyPermits-sized block. Use this when the quota delegate
// must be load-bearing — e.g. a paid-tier deployment where bypass-
// on-outage would let traffic through unmetered.
//
// Default (no option): fail-open. WithQuotaEnforce alone is a no-op;
// it must be combined with WithQuota.
//
// Plan §Caller-ID + quota ladder.
//
// Stability: stable
func WithQuotaEnforce() Option {
	return func(cfg *config) { cfg.quotaEnforce = true }
}

// quotaAuthDelegate is the per-gateway state for the permit gate:
// bucket map + singleflight + lifecycle bookkeeping. One instance is
// built in New() when WithQuota is set; the middleware closes over
// it.
type quotaAuthDelegate struct {
	gw   *Gateway
	opts QuotaOptions

	// enforce flips the fail-open paths (UNAVAILABLE / NOT_CONFIGURED
	// / UNSPECIFIED / transport error / no delegate registered) to
	// fail-closed: caller sees CodeResourceExhausted instead of the
	// emergency block. Set by WithQuotaEnforce; read in refill (and
	// the no-delegate short-circuit in check).
	enforce bool

	mu      sync.Mutex
	buckets map[string]*quotaBucket
	sf      singleflight.Group

	// callDelegateFn is the test-injection seam. nil → use the real
	// gRPC dial path against the _quota_auth/v1 pool.
	callDelegateFn quotaCallDelegateFn
}

// quotaBucket is the local permit counter for one (caller, ns, ver).
// permits is the remaining count; validUntil is the wall-clock cap on
// the current block. Both are protected by the bucket's own mutex —
// the delegate's bucket map sits behind the parent mutex, but the
// hot-path Try operates only on the per-bucket lock.
type quotaBucket struct {
	mu         sync.Mutex
	permits    int32
	validUntil time.Time
}

// newQuotaAuthDelegate wires the delegate state. The gateway must
// exist before this runs because the AcquireBlock pool lookup closes
// over *Gateway.
func newQuotaAuthDelegate(g *Gateway, opts QuotaOptions) *quotaAuthDelegate {
	return &quotaAuthDelegate{
		gw:      g,
		opts:    opts,
		buckets: make(map[string]*quotaBucket),
	}
}

func (d *quotaAuthDelegate) blockSize() uint32 {
	if d.opts.BlockSize > 0 {
		return d.opts.BlockSize
	}
	return 100
}

func (d *quotaAuthDelegate) maxBlockSize() uint32 {
	if d.opts.MaxBlockSize > 0 {
		return d.opts.MaxBlockSize
	}
	return 10_000
}

func (d *quotaAuthDelegate) emergencyPermits() uint32 {
	if d.opts.EmergencyPermits > 0 {
		return d.opts.EmergencyPermits
	}
	return 1
}

func (d *quotaAuthDelegate) maxValidUntil() time.Duration {
	if d.opts.MaxValidUntil > 0 {
		return d.opts.MaxValidUntil
	}
	return time.Hour
}

func (d *quotaAuthDelegate) emergencyTTL() time.Duration {
	if d.opts.EmergencyTTL > 0 {
		return d.opts.EmergencyTTL
	}
	return 5 * time.Second
}

func (d *quotaAuthDelegate) timeout() time.Duration {
	if d.opts.Timeout > 0 {
		return d.opts.Timeout
	}
	return 3 * time.Second
}

// bucketKey builds the map key. caller is already "unknown" when
// anonymous; ns/ver are canonical strings.
func quotaBucketKey(caller, ns, ver string) string {
	return caller + "|" + ns + "|" + ver
}

// getOrCreateBucket returns the bucket for (caller, ns, ver),
// creating it if absent. The parent mutex is held briefly; per-bucket
// state lives behind the bucket's own mutex.
func (d *quotaAuthDelegate) getOrCreateBucket(key string) *quotaBucket {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.buckets[key]
	if !ok {
		b = &quotaBucket{}
		d.buckets[key] = b
	}
	return b
}

// tryDebit decrements one permit if the bucket is live (permits > 0
// and validUntil not crossed). Returns true on success, false when
// the bucket needs a refill.
func (b *quotaBucket) tryDebit(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.permits <= 0 {
		return false
	}
	if !b.validUntil.IsZero() && now.After(b.validUntil) {
		b.permits = 0
		return false
	}
	b.permits--
	return true
}

// install replaces the bucket's contents under its own mutex. Called
// after a successful refill (or fail-open emergency grant).
func (b *quotaBucket) install(permits int32, validUntil time.Time) {
	b.mu.Lock()
	b.permits = permits
	b.validUntil = validUntil
	b.mu.Unlock()
}

// permitsLive returns true when the bucket has unspent permits in a
// non-expired window. Used by the singleflight leader to skip the
// delegate RPC if another goroutine refilled while it was queued.
func (b *quotaBucket) permitsLive(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.permits <= 0 {
		return false
	}
	if !b.validUntil.IsZero() && now.After(b.validUntil) {
		return false
	}
	return true
}

// quotaRefillResult is what a singleflight refill returns: either OK
// (with the block installed), or DENIED (with a Retry-After hint).
type quotaRefillResult struct {
	granted    bool
	retryAfter time.Duration
	reason     string
}

// refill is the singleflight body for one (caller, ns, ver). Calls
// the delegate, computes the install, and either returns granted=true
// (caller debits one permit after; the install is already in the
// bucket) or granted=false (DENIED — caller sees Reject).
func (d *quotaAuthDelegate) refill(ctx context.Context, b *quotaBucket, caller, ns, ver string) quotaRefillResult {
	resp, err := d.callDelegate(ctx, caller, ns, ver)
	now := time.Now()
	if err != nil || resp == nil {
		// Delegate unreachable / not registered. Fail-open grants an
		// emergency block; WithQuotaEnforce flips to fail-closed so
		// production deployments can refuse traffic when the quota
		// service is the policy authority.
		if d.enforce {
			return quotaRefillResult{granted: false, retryAfter: d.emergencyTTL(), reason: "quota delegate unavailable"}
		}
		b.install(int32(d.emergencyPermits()), now.Add(d.emergencyTTL()))
		return quotaRefillResult{granted: true}
	}
	switch resp.GetCode() {
	case qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK:
		grant := resp.GetGrantedPermits()
		if grant == 0 {
			// Defensive: OK + 0 grants is treated as DENIED so a
			// misbehaving delegate can't silently freeze a caller.
			retry := d.retryAfterFromResp(resp, now)
			return quotaRefillResult{granted: false, retryAfter: retry, reason: resp.GetReason()}
		}
		if max := d.maxBlockSize(); grant > max {
			grant = max
		}
		validUntil := d.validUntilFromResp(resp, now)
		b.install(int32(grant), validUntil)
		return quotaRefillResult{granted: true}
	case qav1.QuotaAuthCode_QUOTA_AUTH_CODE_DENIED:
		retry := d.retryAfterFromResp(resp, now)
		return quotaRefillResult{granted: false, retryAfter: retry, reason: resp.GetReason()}
	default:
		// UNAVAILABLE / NOT_CONFIGURED / UNSPECIFIED. Fail-open grants
		// emergency permits; WithQuotaEnforce surfaces the
		// delegate-side outage as a RESOURCE_EXHAUSTED rejection.
		if d.enforce {
			reason := resp.GetReason()
			if reason == "" {
				reason = "quota delegate " + resp.GetCode().String()
			}
			return quotaRefillResult{granted: false, retryAfter: d.emergencyTTL(), reason: reason}
		}
		b.install(int32(d.emergencyPermits()), now.Add(d.emergencyTTL()))
		return quotaRefillResult{granted: true}
	}
}

// validUntilFromResp clamps the delegate's window by MaxValidUntil
// and substitutes a default when the delegate didn't set one.
func (d *quotaAuthDelegate) validUntilFromResp(resp *qav1.AcquireBlockResponse, now time.Time) time.Time {
	cap := now.Add(d.maxValidUntil())
	if vu := resp.GetValidUntil(); vu != nil && vu.IsValid() {
		t := vu.AsTime()
		if t.Before(cap) {
			if t.After(now) {
				return t
			}
			// Wall-clock in the past — treat as emergency-sized
			// block expiring soon so we re-consult the delegate
			// rather than installing an immediately-stale block.
			return now.Add(d.emergencyTTL())
		}
	}
	return cap
}

// retryAfterFromResp derives a Retry-After hint from validUntil on
// the delegate response. Falls back to emergencyTTL when the
// delegate didn't set a window so the client doesn't hammer.
func (d *quotaAuthDelegate) retryAfterFromResp(resp *qav1.AcquireBlockResponse, now time.Time) time.Duration {
	if vu := resp.GetValidUntil(); vu != nil && vu.IsValid() {
		t := vu.AsTime()
		if t.After(now) {
			return t.Sub(now)
		}
	}
	return d.emergencyTTL()
}

// callDelegate invokes _quota_auth/v1::AcquireBlock. Returns
// (nil, nil) when no delegate is registered or the dispatch fails;
// the caller treats that as fail-open. Tests inject the call via
// d.callDelegate replacement (the field is a function so the
// real-delegate dial path stays single-file but tests can stub it).
func (d *quotaAuthDelegate) callDelegate(ctx context.Context, caller, ns, ver string) (*qav1.AcquireBlockResponse, error) {
	if d.callDelegateFn != nil {
		return d.callDelegateFn(ctx, caller, ns, ver)
	}
	pool, ok := d.gw.lookupPool(quotaAuthorizerNamespace, "v1")
	if !ok {
		return nil, nil
	}
	rep := pool.pickReplica()
	if rep == nil || rep.conn == nil {
		return nil, nil
	}
	client := qav1.NewQuotaAuthorizerClient(rep.conn)
	dctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	return client.AcquireBlock(dctx, &qav1.AcquireBlockRequest{
		CallerId:         caller,
		Namespace:        ns,
		Version:          ver,
		RequestedPermits: d.blockSize(),
	})
}

// callDelegateFn is set on quotaAuthDelegate for tests that need to
// inject a fake delegate without setting up a full gRPC pool.
// Production code never sets this.
type quotaCallDelegateFn func(ctx context.Context, caller, ns, ver string) (*qav1.AcquireBlockResponse, error)

// check is the middleware hot path: try the bucket, refill on miss,
// surface a rejection on explicit DENIED. Always returns nil on
// fail-open paths so an outage of the delegate doesn't brick the
// gateway.
//
// Refill model: singleflight collapses concurrent refills per key
// to one RPC. The leader's refill installs the block; ALL waiters
// (leader + queued) then try to debit independently. If the block
// drains before a waiter gets a permit, the outer loop reissues
// another singleflight call. Bounded by quotaRefillRetries so a
// pathological caller can't spin forever on a degenerate delegate.
func (d *quotaAuthDelegate) check(ctx context.Context, caller, ns, ver string) error {
	key := quotaBucketKey(caller, ns, ver)
	b := d.getOrCreateBucket(key)
	for attempt := 0; attempt < quotaRefillRetries; attempt++ {
		if b.tryDebit(time.Now()) {
			return nil
		}
		v, _, _ := d.sf.Do(key, func() (any, error) {
			// Another goroutine may have already refilled while we
			// were queued behind sf.Do; check before paying for the
			// delegate RPC.
			if b.permitsLive(time.Now()) {
				return quotaRefillResult{granted: true}, nil
			}
			return d.refill(ctx, b, caller, ns, ver), nil
		})
		res := v.(quotaRefillResult)
		if !res.granted {
			return &rejection{
				Code:       CodeResourceExhausted,
				Msg:        quotaDeniedMsg(caller, ns, ver, res.reason),
				RetryAfter: res.retryAfter,
			}
		}
		// Block is installed; loop back to debit. If the block was
		// drained by other waiters before we got there, the next
		// iteration triggers another refill.
	}
	return &rejection{
		Code: CodeResourceExhausted,
		Msg:  quotaDeniedMsg(caller, ns, ver, "refill retries exhausted"),
	}
}

// quotaRefillRetries is the per-request cap on refill → debit
// roundtrips. 4 is plenty: a refill block under contention drains in
// O(callers per bucket) iterations, and a pathologically tiny block
// (delegate granting 1 permit) becomes ~K-attempt latency rather
// than starvation.
const quotaRefillRetries = 4

func quotaDeniedMsg(caller, ns, ver, reason string) string {
	base := "quota exhausted for caller=" + caller + " " + ns + "/" + ver
	if reason != "" {
		return base + ": " + reason
	}
	return base
}

// quotaMiddleware returns a DispatcherMiddleware that gates dispatch
// on the per-(caller, ns, ver) bucket. Identity when no quota is
// configured.
func (g *Gateway) quotaMiddleware(ns, ver string) ir.DispatcherMiddleware {
	d := g.quotaAuth
	if d == nil {
		return func(next ir.Dispatcher) ir.Dispatcher { return next }
	}
	extractor := g.cfg.callerIDExtractor
	headers := g.cfg.callerHeaders
	return func(next ir.Dispatcher) ir.Dispatcher {
		if appendNext, ok := next.(ir.AppendDispatcher); ok {
			return &quotaAppendWrapper{
				next:      appendNext,
				delegate:  d,
				extractor: extractor,
				headers:   headers,
				ns:        ns,
				ver:       ver,
			}
		}
		return ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
			caller := resolveCallerID(ctx, extractor, headers)
			if err := d.check(ctx, caller, ns, ver); err != nil {
				return nil, err
			}
			return next.Dispatch(ctx, args)
		})
	}
}

// quotaAppendWrapper preserves the AppendDispatcher capability across
// the quota gate. Same check + delegate logic as the Dispatch path;
// just runs on both surfaces so an inner byte-splice dispatcher
// doesn't degrade.
type quotaAppendWrapper struct {
	next      ir.AppendDispatcher
	delegate  *quotaAuthDelegate
	extractor CallerIDExtractor
	headers   []string
	ns        string
	ver       string
}

func (q *quotaAppendWrapper) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	caller := resolveCallerID(ctx, q.extractor, q.headers)
	if err := q.delegate.check(ctx, caller, q.ns, q.ver); err != nil {
		return nil, err
	}
	return q.next.Dispatch(ctx, args)
}

func (q *quotaAppendWrapper) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	caller := resolveCallerID(ctx, q.extractor, q.headers)
	if err := q.delegate.check(ctx, caller, q.ns, q.ver); err != nil {
		return dst, err
	}
	return q.next.DispatchAppend(ctx, args, dst)
}

var _ ir.AppendDispatcher = (*quotaAppendWrapper)(nil)

// callDelegateFn is the test-injection seam (see callDelegate). Kept
// as a value field on the struct (not a closure) so the test helper
// can swap it after construction.
//
//nolint:unused // referenced via reflection-style assignment in tests
var _ quotaCallDelegateFn
