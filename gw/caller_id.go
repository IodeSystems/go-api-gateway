package gateway

import (
	"container/list"
	"context"
	"sync"

	"github.com/iodesystems/gwag/v2/gw/ir"
	"google.golang.org/grpc/metadata"
)

// CallerIDExtractor resolves the caller-id for an in-flight request.
// It is consulted by the dispatch metric / stats recording site on
// every dispatch and is the single seam covering HTTP, WebSocket, and
// gRPC ingress — the three transports the gateway exposes.
//
// Return values:
//   - ("alice", nil): caller identified as "alice".
//   - ("", nil):       anonymous caller. Recorded as "unknown".
//   - ("", err):       extraction failed (e.g. HMAC didn't verify).
//     Recorded as "unknown" today; once
//     WithCallerIDEnforce ships (plan §Caller-ID),
//     err short-circuits the request with 401/16.
//
// Plan §Caller-ID — one seam, four implementations: Public, HMAC,
// Delegated, mTLS-via-proxy. v1 ships Public via WithCallerIDPublic;
// the rest layer on without touching the seam.
//
// Stability: stable
type CallerIDExtractor func(ctx context.Context) (string, error)

// PublicCallerIDHeader is the HTTP / WebSocket header consulted by
// the WithCallerIDPublic extractor.
//
// Stability: stable
const PublicCallerIDHeader = "X-Caller-Id"

// PublicCallerIDMetadata is the gRPC metadata key consulted by the
// WithCallerIDPublic extractor. gRPC normalises keys to lower-case
// per the spec.
//
// Stability: stable
const PublicCallerIDMetadata = "caller-id"

// WithCallerIDExtractor installs a custom caller-id extraction
// function. The extractor runs at dispatch metric / stats recording
// time on every call.
//
// Built-in flavors: WithCallerIDPublic. If both WithCallerIDExtractor
// and WithCallerHeaders are set, the extractor wins; the older
// header-allowlist option stays as the no-extractor fallback so
// pre-seam adopters keep working unchanged.
//
// Stability: stable
func WithCallerIDExtractor(ex CallerIDExtractor) Option {
	return func(cfg *config) { cfg.callerIDExtractor = ex }
}

// WithCallerIDPublic installs the "public" extractor: trust the
// inbound X-Caller-Id HTTP header (also covers WebSocket via the
// upgrade request) and the caller-id gRPC metadata key on the
// incoming context. Forgeable by design — suitable for dev, behind
// an authenticated reverse proxy, or with mTLS-via-proxy terminating
// in front of the gateway. For untrusted-network production, use the
// HMAC flavor (plan §Caller-ID HMAC mode — next todo).
//
// Stability: stable
func WithCallerIDPublic() Option {
	return WithCallerIDExtractor(publicCallerIDExtractor)
}

// publicCallerIDExtractor is the implementation backing
// WithCallerIDPublic. HTTP wins over gRPC when both happen to be
// present (the gateway's GraphQL / huma surfaces are HTTP; the
// gRPC-incoming case is the gRPC ingress entry point).
func publicCallerIDExtractor(ctx context.Context) (string, error) {
	if r := HTTPRequestFromContext(ctx); r != nil {
		if v := r.Header.Get(PublicCallerIDHeader); v != "" {
			return v, nil
		}
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(PublicCallerIDMetadata); len(v) > 0 && v[0] != "" {
			return v[0], nil
		}
	}
	return "", nil
}

// resolveCallerID derives the caller string a metric or stats
// observation should carry. Prefers the configured extractor (the
// seam); falls back to the legacy WithCallerHeaders allowlist when no
// extractor is installed. Always returns a non-empty string —
// "unknown" stands in for anonymous or failed extraction.
func resolveCallerID(ctx context.Context, ex CallerIDExtractor, headers []string) string {
	if ex != nil {
		v, err := ex(ctx)
		if err != nil || v == "" {
			return "unknown"
		}
		return v
	}
	return callerFromContext(ctx, headers)
}

// WithCallerIDEnforce flips the gateway from default-allow to
// reject-anonymous: every dispatch must resolve a non-empty caller-id
// or it short-circuits with CodeUnauthenticated (HTTP 401 / gRPC 16).
// Without this option the gateway records anonymous / failed-
// extraction as caller="unknown" and lets the request through — the
// day-1 posture that keeps adopters from bricking their own traffic
// while wiring the extractor.
//
// Resolution sources are the same as resolveCallerID: a configured
// CallerIDExtractor wins, otherwise the legacy WithCallerHeaders
// allowlist. With neither configured, every dispatch is rejected as
// unauthenticated — the operator opted into enforce without wiring a
// caller source, which is almost always a misconfiguration we want
// loud rather than silent.
//
// Subscription dispatchers are not wrapped (matching the existing
// quotaMiddleware skip); the HMAC channel-auth gate on subscribe is
// the per-subscription auth seam.
//
// Plan §Caller-ID + quota ladder.
//
// Stability: stable
func WithCallerIDEnforce() Option {
	return func(cfg *config) { cfg.callerIDEnforce = true }
}

// enforceCallerID returns the rejection error a dispatch should
// short-circuit on when WithCallerIDEnforce is in play. nil means the
// request carries a usable caller-id. The error is always a
// *rejection with CodeUnauthenticated so the ingress error mapper
// renders 401 / Unauthenticated.
func enforceCallerID(ctx context.Context, ex CallerIDExtractor, headers []string) error {
	if ex != nil {
		v, err := ex(ctx)
		if err != nil {
			return Reject(CodeUnauthenticated, "caller-id required: "+err.Error())
		}
		if v == "" {
			return Reject(CodeUnauthenticated, "caller-id required: anonymous caller")
		}
		return nil
	}
	v := callerFromContext(ctx, headers)
	if v == "" || v == "unknown" {
		return Reject(CodeUnauthenticated, "caller-id required: no extractor or matching header")
	}
	return nil
}

// callerIDEnforceMiddleware returns a DispatcherMiddleware that
// short-circuits with CodeUnauthenticated when no caller-id resolves.
// Identity middleware when WithCallerIDEnforce is not set.
func (g *Gateway) callerIDEnforceMiddleware() ir.DispatcherMiddleware {
	if !g.cfg.callerIDEnforce {
		return func(next ir.Dispatcher) ir.Dispatcher { return next }
	}
	ex := g.cfg.callerIDExtractor
	headers := g.cfg.callerHeaders
	return func(next ir.Dispatcher) ir.Dispatcher {
		if appendNext, ok := next.(ir.AppendDispatcher); ok {
			return &callerIDEnforceAppendWrapper{next: appendNext, extractor: ex, headers: headers}
		}
		return ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
			if err := enforceCallerID(ctx, ex, headers); err != nil {
				return nil, err
			}
			return next.Dispatch(ctx, args)
		})
	}
}

// callerIDEnforceAppendWrapper preserves AppendDispatcher across the
// caller-id enforcement gate so an inner byte-splice dispatcher keeps
// its fast path. Same enforce-then-call shape; just on both surfaces.
type callerIDEnforceAppendWrapper struct {
	next      ir.AppendDispatcher
	extractor CallerIDExtractor
	headers   []string
}

func (c *callerIDEnforceAppendWrapper) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	if err := enforceCallerID(ctx, c.extractor, c.headers); err != nil {
		return nil, err
	}
	return c.next.Dispatch(ctx, args)
}

func (c *callerIDEnforceAppendWrapper) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	if err := enforceCallerID(ctx, c.extractor, c.headers); err != nil {
		return dst, err
	}
	return c.next.DispatchAppend(ctx, args, dst)
}

var _ ir.AppendDispatcher = (*callerIDEnforceAppendWrapper)(nil)

// OtherCallerID is the bucket label every caller past the
// WithCallerIDMetricsTopK cap is folded into. Operators can scrape it
// directly to see overflow volume; bounded label cardinality keeps
// Prometheus from blowing up on a flood of unique callers.
//
// Stability: stable
const OtherCallerID = "__other__"

// WithCallerIDMetricsTopK caps the number of distinct caller-id values
// that appear as a label on the Prometheus dispatch histogram and as a
// dimension on the in-process stats registry. Once k distinct callers
// are admitted, additional callers are folded into a single
// "__other__" bucket (OtherCallerID).
//
// Admission is LRU-ordered: each observed caller bumps its
// recently-used position; eviction picks the least-recently-used
// admitted caller. A burst of one-off callers won't permanently
// displace steady traffic from real services.
//
// k <= 0 disables the cap (default — every distinct caller becomes a
// label, matching pre-v0.x behaviour).
//
// Plan §Caller-ID — guards against Prometheus scrape blowups when an
// untrusted ingress / public-mode deployment sees high-cardinality
// X-Caller-Id values.
//
// Stability: stable
func WithCallerIDMetricsTopK(k int) Option {
	return func(cfg *config) { cfg.callerIDMetricsTopK = k }
}

// callerLimiter caps the set of caller-id strings that get through to
// the metrics surface. Returns the input string when admitted (already
// in the set, or freshly inserted under cap); returns OtherCallerID
// when the set is full and the input isn't admitted. LRU eviction is
// driven by the doubly-linked list; the head is most-recently-used.
type callerLimiter struct {
	mu       sync.Mutex
	k        int
	order    *list.List
	admitted map[string]*list.Element
}

func newCallerLimiter(k int) *callerLimiter {
	if k <= 0 {
		return nil
	}
	return &callerLimiter{
		k:        k,
		order:    list.New(),
		admitted: make(map[string]*list.Element, k),
	}
}

// Apply runs the caller string through the cap. nil receiver is the
// pre-option default — every caller passes through unchanged. The
// receiver locks once per call; that's measured against the existing
// Prometheus WithLabelValues / sync.Mutex cost on the same hot path,
// not a fresh contention point.
func (l *callerLimiter) Apply(caller string) string {
	if l == nil || l.k <= 0 {
		return caller
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.admitted[caller]; ok {
		l.order.MoveToFront(el)
		return caller
	}
	if len(l.admitted) >= l.k {
		return OtherCallerID
	}
	el := l.order.PushFront(caller)
	l.admitted[caller] = el
	return caller
}

// snapshot returns the admitted callers in MRU → LRU order. Test-only;
// production reads admission state through Apply.
func (l *callerLimiter) snapshot() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, l.order.Len())
	for e := l.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(string))
	}
	return out
}
