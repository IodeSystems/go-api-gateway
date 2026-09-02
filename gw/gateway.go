// Package gateway is a small library for fronting gRPC services with a
// GraphQL surface. The zero-config path takes a list of .proto files and
// gRPC destinations and exposes them as a namespaced GraphQL API; the
// power features are layered on as middleware.
package gateway

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IodeSystems/graphql-go/v2"
	"github.com/IodeSystems/graphql-go/v2/gqlerrors"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/gw/ir"
)

// Gateway is the primary library type. It fronts one or more upstream
// services under a single GraphQL surface. Create with New; register
// services with AddProto / AddOpenAPI / AddGraphQL; mount Handler on
// your HTTP server.
//
// Stability: stable
type Gateway struct {
	mu sync.Mutex

	// stableChanged is a broadcast channel signaled whenever stableVN
	// changes. Tests select on it with a context deadline to wait for
	// logical convergence instead of polling with time.Sleep.
	// Unbuffered — the broadcast helper drains safely.
	stableChanged chan struct{}

	// slots is the §4 single-occupant index per (namespace, version).
	// Every registration goes through `registerSlotLocked` first to
	// enforce the tier policy (unstable swap / vN immutability /
	// cross-kind reject); the per-kind map below it (pools /
	// openAPISources / graphQLSources) holds the dispatch state.
	// Phase 2 of the slot-registry refactor (plan §4) pre-distills
	// `*ir.Service` onto the slot and drops the per-kind maps so
	// schema rebuild can iterate slots directly.
	slots map[poolKey]*slot

	internal   map[string]bool // namespaces hidden from the public schema
	transforms []Transform
	schema     atomic.Pointer[graphql.Schema]
	// schemaAnnotations holds the SDL annotation index for the cached
	// schema, rebuilt in lockstep by assembleLocked so /schema/graphql
	// can decorate the served SDL with directives.
	schemaAnnotations atomic.Pointer[ir.AnnotationIndex]

	// stableVN tracks the highest-ever-seen "vN" per namespace.
	// Monotonic; only advances. Read at schema-build time so the
	// renderer can emit a `stable` sub-field aliasing the matching
	// vN's content. See `gw/stable.go`. Cluster-wide convergence is
	// reconciler-driven today; KV-persisted recovery for fresh nodes
	// joining mid-life is a plan §4 follow-up.
	stableVN map[string]int

	// stats is the in-process rolling stats registry — 1m / 1h / 24h
	// per (namespace, version, method) windowed call counts +
	// throughput + p50/p95 latency. The admin UI reads it directly
	// (no Prometheus dependency); Prometheus stays canonical for
	// long-window history. See gw/stats.go.
	stats *statsRegistry

	// deprecation is the side-state mirror of the cluster
	// `gwag-deprecated` KV bucket. The watch loop
	// populates it; registerSlotLocked reads it to stamp newly-
	// joining slots that already have a deprecation set (covers the
	// reconciler-arrives-after-watch race). Plan §5.
	deprecation map[poolKey]string

	// mcpConfig mirrors the cluster `gwag-mcp-config` KV
	// bucket — the operator-curated MCP surface allowlist consumed
	// by the schema_list / schema_search / schema_expand tools. The
	// watch loop on peerTracker populates it in cluster mode;
	// SetMCPConfig writes through KV (cluster) or direct (standalone).
	// nil = no config observed yet (default-zero MCPConfig). Plan §2
	// MCP integration.
	mcpConfig *mcpConfigState
	cfg        *config
	cp         *controlPlane
	peers      *peerTracker
	broker     *subBroker

	// tracer wraps the configured OpenTelemetry TracerProvider plus a
	// W3C TraceContext propagator. Always non-nil — when WithTracer is
	// unset it adapts a noop provider so ingress / dispatch sites can
	// open spans unconditionally.
	tracer *tracer

	// dispatchers holds one ir.Dispatcher per (namespace, version,
	// flat-op-name) populated by the per-format field builders during
	// schema rebuild. Resolver closures fetch by SchemaID instead of
	// capturing the dispatcher pointer, which keeps the closure
	// independent of dispatcher identity — non-GraphQL ingress paths
	// (planned: HTTP/JSON, gRPC) can find the same dispatcher by
	// SchemaID without re-walking pools/sources.
	//
	// Dispatchers are registered fresh on each schema rebuild
	// (assembleLocked → buildSchemaLocked clears + repopulates).
	// Caller holds g.mu during rebuild.
	dispatchers *ir.DispatchRegistry

	// ingressRoutes is the (METHOD, path) → dispatcher table consumed
	// by IngressHandler. Rebuilt on every assembleLocked from the
	// freshly-populated dispatchers map. Atomic-swapped so the
	// handler doesn't take g.mu on the hot path.
	ingressRoutes atomic.Pointer[ingressTable]

	// grpcIngressRoutes is the gRPC analogue of ingressRoutes:
	// "/<svcFullName>/<methodName>" → proto-native chained Handler
	// (skipping canonical-args round trip). Consumed by
	// GRPCUnknownHandler. Rebuilt + atomic-swapped on every
	// assembleLocked.
	grpcIngressRoutes atomic.Pointer[grpcIngressTable]

	// streamGlobalSem caps simultaneous subscription streams across
	// every pool — the gateway-wide MaxStreamsTotal ceiling. nil when
	// disabled (0).
	streamGlobalSem chan struct{}
	streamGlobal    atomic.Int32 // active count
	streamGlobalQ   atomic.Int32 // waiting count

	// draining flips to true on Drain(). Health returns 503; new
	// WebSocket upgrades and new GraphQL queries reject; existing
	// connections receive a `complete` and are then closed.
	draining atomic.Bool

	// wsConns is the registry of live WebSocket subscription
	// connections. Each entry is a cancel func bound to the
	// connection's serveWebSocket lifetime; Drain cancels all to
	// force-close active subscriptions.
	wsMu    sync.Mutex
	wsConns map[uintptr]context.CancelFunc

	// wsLimit enforces WithWSLimit. nil when no cap is configured;
	// the Upgrade path nil-checks before acquiring.
	wsLimit *wsLimiter

	// planCache caches parsed + validated + planned query state so
	// the hot path skips graphql-go's parse + ValidateDocument +
	// PlanQuery cost. With WithDocNormalization(), literal-baked
	// queries that differ only in arg values share one plan, with
	// the per-call literals carried via PlanResult.SynthArgs.
	// Always non-nil; size 0 means uncached fall-through (the
	// underlying PlanCache handles that).
	planCache *graphql.PlanCache

	// graphiqlHandler is the cached GraphiQL UI server used to render
	// the in-browser IDE. Built once per assembleLocked (schema is
	// bound at construction). nil when WithoutGraphiQL() is set —
	// browser requests fall through to the JSON path. Backed by the
	// vendored graphiqlServer in gw/graphiql.go.
	graphiqlHandler atomic.Pointer[graphiqlServer]

	// life is cancelled by Close to stop background goroutines.
	life       context.Context
	lifeCancel context.CancelFunc

	// warnedEventsAuth tracks (namespace, version) tuples we've already
	// emitted a deprecation warning for, so heartbeat-driven re-registers
	// don't spam logs. Keyed on "<ns>:<ver>". The SubscriptionAuthorizer
	// delegate path under _events_auth/v1 is being collapsed in favor of
	// the signer-as-API model — see plan §2.
	warnedEventsAuth sync.Map

	// injectPathStates is the per-rule activation tracker for
	// InjectPath transforms. Keyed by path string ("ns.op.arg"); value
	// is the last known InjectorState observed by
	// evalInjectPathStatesLocked. Populated on Use(...) and refreshed
	// on every assembleLocked so dormant→active and active→dormant
	// transitions log exactly once. Caller holds g.mu.
	injectPathStates map[string]injectorState

	// callerAuth is the per-gateway state for WithCallerIDDelegated —
	// TTL cache + singleflight + the revoke-listener subscription. nil
	// when the delegated extractor isn't installed. Plan §Caller-ID.
	callerAuth *callerAuthDelegate

	// quotaAuth is the per-gateway permit pool for WithQuota — bucket
	// map keyed by (caller_id, namespace, version) + singleflight on
	// refill. nil when no quota gate is configured (default-allow).
	// Plan §Caller-ID + quota ladder.
	quotaAuth *quotaAuthDelegate

	// rejectedJoins counts and timestamps registerSlotLocked
	// rejections per slot key, so /admin/services can surface "this
	// slot rejected N joins with caps X (currently running Y)" before
	// an operator has to profile a stale-config bug. See plan Tier 3
	// "Loud surface for slot-policy mismatches on `vN` joins". Caller
	// holds g.mu.
	rejectedJoins map[poolKey]*rejectedJoinSummary

	// channelBindingIndex aggregates channel→payload-type bindings
	// across all slots. Rebuilt on every assembleLocked; atomic-
	// swapped so the psPub hot path reads without g.mu.
	channelBindingIndex atomic.Pointer[channelBindingIndex]
}

// rejectedJoinSummary captures the most recent rejection plus a
// running count for one slot. The latest reason / caps are sufficient
// for triage — pre-fork reproduction showed the rejection is usually
// uniform within a stale-config window (every replica binary sends
// the same wrong caps).
type rejectedJoinSummary struct {
	Count                            uint32
	LastReason                       string
	LastUnixMs                       int64
	LastMaxConcurrency               int
	LastMaxConcurrencyPerInstance    int
	CurrentMaxConcurrency            int
	CurrentMaxConcurrencyPerInstance int
}

// Option is a gateway-wide configuration function. Pass to New.
//
// Stability: stable
type Option func(*config)
type config struct {
	cluster        *Cluster
	tls            *tls.Config
	metrics        Metrics
	tracerProvider oteltrace.TracerProvider
	backpressure   BackpressureOptions
	subAuth      SubscriptionAuthOptions
	adminToken   []byte
	adminDataDir string
	// destructiveReads are request paths whose GET/HEAD has side effects
	// (or exposes sensitive data); AdminMiddleware gates them with admin
	// auth instead of letting them through as public reads. A trailing
	// "*" makes an entry a prefix match.
	destructiveReads []string
	signerSecret     []byte
	openAPIHTTP  *http.Client
	pprof        bool
	requestLog   io.Writer

	// docCache* control the plan cache exposed via gw.Handler(). 0 →
	// defaults; docCacheDisabled bypasses the cache entirely;
	// docCacheNormalize enables literal→variable rewriting so
	// literal-baked queries that differ only in arg values reuse a
	// single plan.
	docCacheSize      int
	docCacheMaxQuery  int
	docCacheDisabled  bool
	docCacheNormalize bool

	// disableGraphiQL skips construction of the cached GraphiQL UI
	// handler and routes browser requests through the JSON path.
	disableGraphiQL bool

	// uploadStore backs the Upload scalar (multipart-spec inline and
	// tus.io chunked endpoints both stage bytes here). nil = no store
	// configured; the tus endpoints return 503 and the multipart-spec
	// parser stages to an ephemeral default. See WithUploadStore /
	// WithUploadDataDir.
	uploadStore UploadStore

	// uploadDataDir, if non-empty, asks New() to build a default
	// FilesystemUploadStore at the directory. Mutually exclusive with
	// WithUploadStore — whichever option ran last wins.
	uploadDataDir string

	// uploadStoreOwned is true when the gateway created the store
	// itself via WithUploadDataDir (so Close() must shut it down).
	// WithUploadStore-supplied stores stay owned by the caller.
	uploadStoreOwned bool

	// uploadLimit is the per-upload byte cap, enforced at tus
	// Upload-Length declaration and at multipart-spec parser. 0 =
	// unlimited (gateway adds no constraint beyond the store / edge).
	uploadLimit int64

	// wsLimit configures per-IP caps on GraphQL WebSocket
	// subscription upgrades. Zero-valued = no cap (the upgrade path
	// stays uncapped, which is the right answer when an upstream
	// proxy / CDN already limits it). See WithWSLimit.
	wsLimit WSLimitOptions

	// grpcConnPoolSize is the number of grpc.ClientConn instances
	// the gateway dials per upstream replica address. 0 means
	// defaultGRPCConnPoolSize. HTTP/2 caps concurrent streams per
	// conn (default 100); past that, streams queue on the conn's
	// transport mutex. A small per-replica pool spreads the
	// per-stream lock load — see gw/grpc_conn_pool.go.
	grpcConnPoolSize int

	// allowedTiers gates which version tiers the gateway accepts at
	// registration and renders in the schema. nil → default policy
	// (all three: unstable / stable / vN). See WithAllowTier.
	allowedTiers *AllowedTiers

	// callerHeaders is the inbound HTTP-header allowlist consulted to
	// derive a caller-id label per dispatch (plan §5). Empty → every
	// dispatch gets caller="unknown". The caller string lands as a
	// dimension on the in-process stats registry and as a Prometheus
	// label on go_api_gateway_dispatch_duration_seconds; operators
	// own the cardinality by choosing bounded headers
	// (X-Caller-Service) or accept the User-Agent blast radius
	// explicitly.
	callerHeaders []string

	// callerIDExtractor is the caller-id seam (plan §Caller-ID). When
	// set, it overrides the legacy callerHeaders allowlist. See
	// WithCallerIDExtractor / WithCallerIDPublic.
	callerIDExtractor CallerIDExtractor

	// callerIDMetricsTopK caps the distinct caller-id values that
	// reach the Prometheus dispatch histogram and the stats registry;
	// see WithCallerIDMetricsTopK. 0 = uncapped.
	callerIDMetricsTopK int

	// callerIDDelegated holds the WithCallerIDDelegated option payload
	// until New() can wire it up against the live *Gateway. The
	// extractor closes over the gateway so it can resolve the
	// _caller_auth/v1 pool at call time.
	callerIDDelegated *CallerIDDelegatedOptions

	// quota holds the WithQuota option payload until New() can build
	// the per-gateway permit pool. nil → no quota gate (default-allow).
	quota *QuotaOptions

	// callerIDEnforce makes dispatches without a resolvable caller-id
	// reject with CodeUnauthenticated. Default false (anonymous
	// allowed). See WithCallerIDEnforce.
	callerIDEnforce bool

	// quotaEnforce flips WithQuota from fail-open to fail-closed: the
	// delegate being UNAVAILABLE / NOT_CONFIGURED / transport-broken
	// rejects with CodeResourceExhausted instead of granting an
	// emergency permit block. Default false. See WithQuotaEnforce.
	quotaEnforce bool

	// channelAuth is the operator-declared (pattern → tier) rule set
	// gating ps.pub / ps.sub. Stored in declaration order; first-hit-
	// wins at Pub entry, strictest-wins for wildcard Sub. nil → every
	// channel falls under the default tier (ChannelAuthHMAC). See
	// WithChannelAuth.
	channelAuth []channelAuthRule

	// channelBindings are runtime-declared channel→payload-type pairs
	// (non-proto adopters, gateway-shipped defaults). Applied to the
	// gateway-internal ps slot during New() so they ride through the
	// same tier/uniqueness policy as proto-declarative bindings. See
	// WithChannelBinding.
	channelBindings []ir.ChannelBinding

	// psBindingEnforce enables shape strictness for ps.pub: when a
	// channel has a binding, the payload is parsed as the bound proto
	// message and rejected with InvalidArgument on mismatch. Default
	// false. See WithChannelBindingEnforce.
	psBindingEnforce bool

	// psStrictPayloadTypes enables coverage strictness for ps.pub:
	// rejects publishes to channels with no matching binding pattern.
	// Default false. See WithStrictPayloadTypes.
	psStrictPayloadTypes bool

	// mcpSeed seeds the MCP allowlist at construction. Applied once at
	// New() before any control-plane / cluster watch overrides. Runtime
	// admin edits via /api/admin/mcp/* still take effect after the
	// seed. mcpSeedSet records whether any WithMCP* option ran so
	// callers can tell "default zero" from "explicitly empty."
	// See WithMCPInclude / WithMCPExclude / WithMCPAutoInclude.
	mcpSeed    MCPConfig
	mcpSeedSet bool
}

// AllowedTiers expresses which §4 version tiers a gateway will accept
// at registration time and surface in the rendered schema. The zero
// value rejects everything; use WithAllowTier (or default — i.e. no
// option) to populate it.
//
//   - Unstable: registrations with version="unstable" accepted.
//   - VN: registrations with version="v<N>" accepted.
//   - Stable: the schema-render `<ns>.stable` alias is emitted. Stable
//     itself is not a registerable version (it's a computed alias),
//     so this gates rendering only.
//
// Stability: stable
type AllowedTiers struct {
	Unstable bool
	Stable   bool
	VN       bool
}

// describe returns a human-readable summary of the allow set, for
// rejection error messages. Order matches the canonical order tiers
// are listed in the plan and the --allow-tier flag.
func (t AllowedTiers) describe() string {
	parts := make([]string, 0, 3)
	if t.Unstable {
		parts = append(parts, "unstable")
	}
	if t.Stable {
		parts = append(parts, "stable")
	}
	if t.VN {
		parts = append(parts, "vN")
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ",")
}

// WithCallerHeaders configures the inbound HTTP-header allowlist used
// to derive a caller-id label on every dispatch. Headers are checked
// in order; the first non-empty value wins. When no header matches —
// or no list is configured — caller defaults to "unknown".
//
// Cardinality is operator-controlled:
//   - X-Caller-Service is bounded by your service registry → safe.
//   - User-Agent is unbounded for public-internet traffic → only safe
//     behind authenticated ingress where caller = service name.
//
// Plan §5: the caller string lands as a dimension on the in-process
// stats registry (admin UI consumes it) and as a Prometheus label on
// go_api_gateway_dispatch_duration_seconds.
//
// Stability: stable
func WithCallerHeaders(headers ...string) Option {
	return func(cfg *config) {
		cfg.callerHeaders = append([]string(nil), headers...)
	}
}

// WithAllowTier restricts the version tiers this gateway accepts at
// registration and renders in the schema. Plan §4: production
// deployments restrict to "stable","vN" or "vN" alone; dev gateways
// accept all three.
//
// Pass any combination of "unstable", "stable", "vN". Unknown values
// are silently ignored — operators control the spelling at the boot
// flag boundary (`--allow-tier`). Calling with no arguments yields a
// gateway that rejects every registration; calling with all three
// matches the default-when-unset behavior.
//
// Effect on registration:
//   - "unstable" missing: registrations with version="unstable" are
//     rejected with a clear error.
//   - "vN" missing: registrations with numbered versions are rejected.
//
// Effect on schema render:
//   - "stable" missing: the `<ns>.stable` alias is omitted even when
//     a `vN` cut is registered. Callers wanting evergreen-without-
//     recodegen must move to a numbered cut.
//
// "stable" itself is never a registerable version — `parseVersion`
// rejects it. The flag controls only whether the alias surfaces.
//
// Stability: stable
func WithAllowTier(tiers ...string) Option {
	return func(cfg *config) {
		t := AllowedTiers{}
		for _, s := range tiers {
			switch s {
			case "unstable":
				t.Unstable = true
			case "stable":
				t.Stable = true
			case "vN":
				t.VN = true
			}
		}
		cfg.allowedTiers = &t
	}
}

// effectiveAllowedTiers returns the tier policy in force. nil-on-cfg
// means the operator never called WithAllowTier; the default is
// permissive (all three).
func (g *Gateway) effectiveAllowedTiers() AllowedTiers {
	if g.cfg.allowedTiers == nil {
		return AllowedTiers{Unstable: true, Stable: true, VN: true}
	}
	return *g.cfg.allowedTiers
}

// checkVersionTierAllowed returns an error if `version` (canonical
// post-parseVersion form: "unstable" or "v<N>") is not in the
// gateway's --allow-tier policy. Plan §4 boot gate: the single hot
// path every registration crosses (registerSlotLocked) calls this
// before allocating a slot, and the control plane Register RPC
// double-checks before writing to the registry KV in cluster mode.
func (g *Gateway) checkVersionTierAllowed(version string) error {
	t := g.effectiveAllowedTiers()
	if version == "unstable" {
		if !t.Unstable {
			return fmt.Errorf("tier %q is not in --allow-tier policy (allowed: %s)", "unstable", t.describe())
		}
		return nil
	}
	if !t.VN {
		return fmt.Errorf("tier %q is not in --allow-tier policy (allowed: %s)", "vN", t.describe())
	}
	return nil
}

// SubscriptionAuthOptions configures HMAC verification for ps.sub
// (the gateway pub/sub primitive). The ps.sub field carries hmac,
// timestamp, and ts args that the gateway verifies against these
// secrets. Proto server-streaming subscriptions no longer go through
// gateway HMAC auth — they open a direct gRPC stream to the upstream,
// which handles its own authentication.
//
// Stability: stable
type SubscriptionAuthOptions struct {
	// Insecure bypasses HMAC verification entirely. Auth args are
	// accepted (for SDL compatibility) but not checked. Dev/local only.
	// Mutually exclusive with Secret / Secrets.
	Insecure bool

	// Secret is the legacy single shared HMAC key. Tokens minted with
	// no kid (or kid=="") verify against this. Signed payload is
	// "<channel>\n<timestamp_unix>" with HMAC-SHA256, base64-encoded.
	// Either Secret, Secrets, or both may be set; if both supply the
	// empty kid, Secret wins.
	Secret []byte

	// Secrets is the keyed-secret set used for token rotation. Each
	// entry maps a key id (kid) to its HMAC secret. Subscribers send
	// their kid alongside hmac/timestamp; the verifier looks it up
	// here, returning UNKNOWN_KID when absent. Tokens carrying a
	// non-empty kid sign over "<kid>\n<channel>\n<timestamp_unix>"
	// so swapping kid can't replay a token across keys. To rotate:
	// add a new (kid,secret) entry, switch signers to it, drop the
	// old entry once outstanding tokens have aged out.
	Secrets map[string][]byte

	// SkewWindow caps acceptable clock drift between the signer and
	// the gateway. 0 → 5 minutes default.
	SkewWindow time.Duration
}

// WithSubscriptionAuth enables HMAC verification on subscribes.
//
// Stability: stable
func WithSubscriptionAuth(o SubscriptionAuthOptions) Option {
	return func(cfg *config) { cfg.subAuth = o }
}

// WithoutSubscriptionAuth marks subscriptions insecure. Equivalent to
// passing WithSubscriptionAuth(SubscriptionAuthOptions{Insecure: true}).
//
// Stability: stable
func WithoutSubscriptionAuth() Option {
	return func(cfg *config) { cfg.subAuth = SubscriptionAuthOptions{Insecure: true} }
}

// BackpressureOptions controls concurrency caps and the wait budget.
//
// Unary dispatches use a per-pool cap (MaxInflight) because each pool
// is one backend service with finite throughput — slow service X
// shouldn't gate dispatches to service Y.
//
// Subscriptions are different: backed by NATS pub/sub, the per-message
// cost on the backend is essentially zero (one NATS sub serves N
// WebSockets via in-process fanout). The scarce resource is the
// gateway itself (file descriptors, RAM, goroutines). MaxStreamsTotal
// caps the gateway as a whole; MaxStreams stays per-pool for operators
// who want fine-grained throttling.
//
// The MaxWaitTime budget applies to all three caps.
//
// Stability: stable
type BackpressureOptions struct {
	// MaxInflight is the per-pool ceiling on simultaneous unary
	// dispatches. 0 disables.
	MaxInflight int

	// MaxStreams is the per-pool ceiling on simultaneous active
	// subscription streams. 0 disables. Useful for fine-grained
	// per-channel throttling. Most deployments leave this generous
	// and rely on MaxStreamsTotal for resource protection.
	MaxStreams int

	// MaxStreamsTotal is the gateway-wide ceiling on simultaneous
	// active subscription streams. 0 disables. Sized to the
	// gateway's resource budget — file descriptors, RAM, goroutines.
	// Beyond ~50-100k per node, scale horizontally with more
	// gateways behind a load balancer.
	MaxStreamsTotal int

	// MaxWaitTime is the per-dispatch wait budget: a dispatch that
	// cannot acquire its slot within this window is rejected with
	// Reject(ResourceExhausted, "wait timeout"). Applies to unary
	// and stream slot acquisition (per-pool and gateway-wide). 0
	// disables the timeout.
	MaxWaitTime time.Duration
}

// DefaultBackpressure is what gateway.New uses unless overridden.
//
// 256 unary in-flight per pool is sized for moderate-throughput
// services. 10,000 streams per pool accommodates typical per-channel
// fan-out without micro-throttling. 100,000 streams gateway-wide is
// the per-node ceiling — beyond this, scale horizontally.
//
// Stability: stable
var DefaultBackpressure = BackpressureOptions{
	MaxInflight:     256,
	MaxStreams:      10_000,
	MaxStreamsTotal: 100_000,
	MaxWaitTime:     10 * time.Second,
}

// WithBackpressure overrides the default per-pool concurrency caps.
//
// Stability: stable
func WithBackpressure(b BackpressureOptions) Option {
	return func(cfg *config) { cfg.backpressure = b }
}

// WithoutBackpressure removes per-pool limits entirely. Dispatches
// proceed without queueing or waiting; useful for tests and dev.
//
// Stability: stable
func WithoutBackpressure() Option {
	return func(cfg *config) { cfg.backpressure = BackpressureOptions{} }
}

// WithMetrics swaps the default Prometheus-backed metrics sink for a
// caller-supplied implementation. Pass nil or use WithoutMetrics to
// disable metrics entirely.
//
// Stability: stable
func WithMetrics(m Metrics) Option {
	return func(cfg *config) { cfg.metrics = m }
}

// WithoutMetrics disables metrics. /metrics returns 404; dispatches
// still run normally with no per-call instrumentation.
//
// Stability: stable
func WithoutMetrics() Option {
	return func(cfg *config) { cfg.metrics = noopMetrics{} }
}

// WithTracer installs an OpenTelemetry TracerProvider for distributed
// tracing. When set, the gateway opens one server-kind span per ingress
// request (GraphQL / HTTP / gRPC) with `gateway.ingress`, plus
// `gateway.namespace` and `gateway.method` on the typed ingresses, and
// one client-kind child span per outbound dispatch. Inbound traceparent
// headers (W3C TraceContext) join the caller's trace; outbound calls
// inject traceparent on HTTP headers and gRPC metadata so downstream
// services see the same trace.
//
// Pass any trace.TracerProvider — the SDK at go.opentelemetry.io/otel/sdk
// is the canonical choice; the no-op default applies when WithTracer is
// not called (tracing-related cost stays near zero).
//
// Stability: stable
func WithTracer(tp oteltrace.TracerProvider) Option {
	return func(cfg *config) { cfg.tracerProvider = tp }
}

// WithCluster binds the gateway to an embedded NATS cluster. When set,
// the gateway uses JetStream KV for the service registry and peer
// tracking (replacing the in-memory map). Without it, the gateway falls
// back to the single-node in-memory path.
//
// Stability: stable
func WithCluster(c *Cluster) Option {
	return func(cfg *config) { cfg.cluster = c }
}

// WithTLS configures mTLS for outbound gRPC dials made by the
// reconciler when it talks to registered services. Pass the same
// config used for the embedded NATS cluster routes (build it with
// LoadMTLSConfig) so one cert covers both surfaces.
//
// Stability: stable
func WithTLS(c *tls.Config) Option {
	return func(cfg *config) { cfg.tls = c }
}

// WithAdminToken pins the gateway's admin bearer token to a
// caller-supplied value (raw bytes; AdminTokenHex returns the
// presentation form). If unset, the gateway generates a 32-byte token
// at New — persisted to <adminDataDir>/admin-token if WithAdminDataDir
// was set, otherwise in-memory only.
//
// The token gates non-public /admin/* HTTP routes and, transitively,
// admin_* GraphQL mutations dispatched through them. It does not
// authenticate services calling each other through the gateway.
//
// Stability: stable
func WithAdminToken(token []byte) Option {
	return func(cfg *config) { cfg.adminToken = token }
}

// WithAdminDataDir is the directory under which the gateway persists
// (and reloads) its boot admin token. Pairs naturally with the
// JetStream data dir on a clustered gateway, but standalone gateways
// can pass any writable path.
//
// Stability: stable
func WithAdminDataDir(dir string) Option {
	return func(cfg *config) { cfg.adminDataDir = dir }
}

// WithDestructiveReads marks request paths whose GET / HEAD has side
// effects or exposes sensitive data, so AdminMiddleware gates them with
// admin auth instead of treating them as public reads. Match is exact
// against the request URL path; a trailing "*" makes an entry a prefix
// match (e.g. "/admin/export/*"). Repeatable.
//
// The gate applies wherever the path is reached, including the GraphQL
// admin_* field that dispatches a GET to it — so a UI reading such a
// field must present the admin bearer.
//
// Stability: experimental
func WithDestructiveReads(paths ...string) Option {
	return func(cfg *config) { cfg.destructiveReads = append(cfg.destructiveReads, paths...) }
}

// WithSignerSecret installs a sign-specific bearer for the gRPC
// SignSubscriptionToken RPC. When set, remote callers must present
// it in `authorization: Bearer <hex>` metadata; the admin/boot token
// is the always-works fallback. When unset, gRPC SignSubscriptionToken
// requires the admin token only. In-process callers (no gRPC peer in
// ctx, e.g. the huma /admin/sign handler or library embedders calling
// cp.SignSubscriptionToken directly) bypass the gate — they're past
// the trust boundary.
//
// Pass raw bytes; the operator-presented form is hex.
//
// Stability: stable
func WithSignerSecret(secret []byte) Option {
	return func(cfg *config) { cfg.signerSecret = secret }
}

// WithOpenAPIClient sets the *http.Client every OpenAPI dispatch
// uses by default. Use this to plug in a transport with mTLS, a
// custom RoundTripper, signed-URL injection, retry policy, or any
// out-of-band auth scheme. Per-source overrides via the
// `OpenAPIClient(...)` ServiceOption beat this default.
//
// When unset, dispatches use http.DefaultClient.
//
// Stability: stable
func WithOpenAPIClient(c *http.Client) Option {
	return func(cfg *config) { cfg.openAPIHTTP = c }
}

// WithMCPInclude declares which operations are exposed via the MCP
// surface (the four tools schema_list / schema_search / schema_expand
// / query mounted by MCPHandler). Each glob is dot-segmented: `*`
// matches one segment, `**` matches any number of segments including
// zero. Examples: "greeter.**" (everything under the greeter
// namespace), "*.list*" (any namespace, methods starting with
// "list"), "admin.**".
//
// Internal `_*` namespaces (admin auth, quota, ps auth) are filtered
// before MCPConfig; neither Include nor AutoInclude can override
// that. AutoInclude=false (the default; flip with WithMCPAutoInclude)
// means the MCP surface is exactly what Include matches.
//
// Runtime admin edits via /api/admin/mcp/include override the seed;
// the seed is the starting state for an unconfigured gateway.
//
// Stability: stable
func WithMCPInclude(globs ...string) Option {
	return func(cfg *config) {
		cfg.mcpSeed.Include = append(cfg.mcpSeed.Include, globs...)
		cfg.mcpSeedSet = true
	}
}

// WithMCPExclude declares glob patterns subtracted from the MCP
// surface. Only meaningful when AutoInclude=true — the surface is
// (every public op) − Exclude. Each glob is dot-segmented, same
// shape as WithMCPInclude.
//
// Stability: stable
func WithMCPExclude(globs ...string) Option {
	return func(cfg *config) {
		cfg.mcpSeed.Exclude = append(cfg.mcpSeed.Exclude, globs...)
		cfg.mcpSeedSet = true
	}
}

// WithMCPAutoInclude flips the MCP allowlist to opt-out mode: every
// public operation appears on the MCP surface except those matched
// by Exclude. Internal `_*` namespaces stay hidden regardless. Off
// by default (the surface is exactly what Include matches).
//
// Stability: stable
func WithMCPAutoInclude() Option {
	return func(cfg *config) {
		cfg.mcpSeed.AutoInclude = true
		cfg.mcpSeedSet = true
	}
}

// WithPprof opts the gateway into exposing the standard net/http/pprof
// profiling endpoints via PprofMux. Disabled by default — pprof leaks
// goroutine + heap state and must never be public. Pair the resulting
// mux with whatever auth the operator chooses (in examples/multi it
// rides under AdminMiddleware at /debug/pprof).
//
// When unset, PprofMux returns nil so the operator's wiring code can
// detect the disabled state without consulting a separate flag.
//
// Stability: stable
func WithPprof() Option {
	return func(cfg *config) { cfg.pprof = true }
}

// WithRequestLog directs one JSON line per request to `w`, useful
// for low-load eyeballing in local dev. Each line is the same
// shape across all three ingress paths (graphql / http / grpc):
//
//	{"ts":"...","ingress":"...","path":"...","total_us":...,"self_us":...,"dispatch_count":N}
//
// Not auto-enabled — at production-scale rps the log is MB/s. Pair
// with `os.Stderr` or a bounded io.Writer (rotating file, ring
// buffer) to keep volume under control. Lines are written
// asynchronously after the response finishes; write failures are
// swallowed (the log is non-critical).
//
// Pass `nil` to disable (matches the default state).
//
// Stability: stable
func WithRequestLog(w io.Writer) Option {
	return func(cfg *config) { cfg.requestLog = w }
}

// WithDocCacheSize sets the maximum number of distinct query strings
// retained in the plan cache (LRU eviction past it). Default 1024.
// Pass 0 to disable; equivalent to WithoutDocCache().
//
// Stability: stable
func WithDocCacheSize(n int) Option {
	return func(cfg *config) {
		cfg.docCacheSize = n
		if n == 0 {
			cfg.docCacheDisabled = true
		}
	}
}

// WithDocCacheMaxQueryBytes sets the per-query byte cap above which a
// query bypasses the cache. Default 64 KiB. 0 → no per-query cap.
//
// Stability: stable
func WithDocCacheMaxQueryBytes(n int) Option {
	return func(cfg *config) { cfg.docCacheMaxQuery = n }
}

// WithoutDocCache disables the plan cache. Use when memory headroom
// matters more than throughput, or when every request has a unique
// query string and normalization is also off (in which case the
// cache only adds bookkeeping with no payback).
//
// Stability: stable
func WithoutDocCache() Option {
	return func(cfg *config) { cfg.docCacheDisabled = true }
}

// WithDocNormalization turns on literal→variable normalization in
// the plan cache. With it on, two queries that differ only in
// literal field-argument values share one cached plan; the per-call
// literals ride along as PlanResult.SynthArgs and are merged into
// the request's variables before ExecutePlan. Costs ~50 µs/call
// for parse + AST walk + fingerprint hash; off by default because
// well-behaved clients use GraphQL variables already.
//
// Stability: stable
func WithDocNormalization() Option {
	return func(cfg *config) { cfg.docCacheNormalize = true }
}

// WithoutGraphiQL disables the in-browser GraphiQL UI. Browser
// requests that would otherwise render the UI fall through to the
// JSON path. The cached GraphiQL handler is never built, so
// operators who only ship machine clients pay zero overhead for it.
//
// Stability: stable
func WithoutGraphiQL() Option {
	return func(cfg *config) { cfg.disableGraphiQL = true }
}

// WithUploadStore plugs a custom UploadStore implementation. The
// store backs both the graphql-multipart-request-spec inline parser
// and the tus.io HTTP endpoints; resolvers receive an *Upload whose
// File reader is opened from the store on demand. The gateway does
// not Close the supplied store on shutdown — its lifetime is the
// caller's. Use WithUploadDataDir for the default filesystem
// implementation.
//
// Stability: stable
func WithUploadStore(s UploadStore) Option {
	return func(cfg *config) {
		cfg.uploadStore = s
		cfg.uploadDataDir = ""
		cfg.uploadStoreOwned = false
	}
}

// WithUploadDataDir installs the default filesystem-backed
// UploadStore rooted at dir, with a 24h TTL on staged uploads. New()
// constructs the store and Close() shuts it down; if dir cannot be
// created, New() panics with the underlying error. Conflicting with
// WithUploadStore is fine — the last option wins.
//
// Stability: stable
func WithUploadDataDir(dir string) Option {
	return func(cfg *config) {
		cfg.uploadDataDir = dir
		cfg.uploadStore = nil
	}
}

// WithUploadLimit caps the per-upload byte total enforced at both the
// graphql-multipart-request-spec parser and the tus.io PATCH path.
// 0 means unlimited (the gateway adds no constraint beyond the
// underlying store). Adopters behind reverse proxies / edge load
// balancers should set this in concert with their LB's request-body
// limit to fail fast at the right layer.
//
// Stability: stable
func WithUploadLimit(maxBytes int64) Option {
	return func(cfg *config) { cfg.uploadLimit = maxBytes }
}

// defaultGRPCConnPoolSize is the per-replica pool fan-out used
// when WithGRPCConnPoolSize is not set. Empirically 4 conns is
// enough to spread the per-conn transport-mutex contention seen
// in profiles at 25k+ RPS without dialing more sockets than a
// loopback deployment can reasonably keep healthy.
const defaultGRPCConnPoolSize = 4

// defaultOutboundIdleConnsPerHost sizes the per-host idle pool on
// the gateway-managed http.Client for OpenAPI / downstream-GraphQL
// dispatches. Go's stdlib default of 2 collapses keep-alive at any
// real RPS — the bench client tripped on the same default earlier
// and lost ~2× throughput to per-request TCP handshakes. 1024 is
// generous enough that a single hot host never spills past idle
// without being so wide we burn fds on a thousand quiet hosts.
const defaultOutboundIdleConnsPerHost = 1024

// newDefaultOutboundHTTPClient returns the *http.Client baked into
// cfg.openAPIHTTP when WithOpenAPIClient is not set. Operators who
// want different settings (proxy, mTLS, custom RoundTripper) pass
// their own client; this is just a sane default that doesn't strand
// keep-alive on the standard library's tiny defaults.
func newDefaultOutboundHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = defaultOutboundIdleConnsPerHost * 4
	tr.MaxIdleConnsPerHost = defaultOutboundIdleConnsPerHost
	tr.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: tr}
}

// WithGRPCConnPoolSize configures the number of grpc.ClientConn
// instances the gateway dials per upstream replica. Default is
// defaultGRPCConnPoolSize (4); set to 1 to restore the original
// single-conn behaviour. HTTP/2 caps streams per conn (default
// 100) and the transport's send mutex serialises framing — under
// high RPS this becomes a contention point that a small pool
// dissolves. Values ≤ 0 fall back to the default.
//
// Stability: stable
func WithGRPCConnPoolSize(n int) Option {
	return func(cfg *config) {
		if n <= 0 {
			n = defaultGRPCConnPoolSize
		}
		cfg.grpcConnPoolSize = n
	}
}

// New creates a Gateway with the supplied options.
// AddProto / AddOpenAPI / AddGraphQL register services; call Handler
// to get the GraphQL HTTP handler.
//
// Stability: stable
func New(opts ...Option) *Gateway {
	cfg := &config{
		backpressure: DefaultBackpressure,
	}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.metrics == nil {
		cfg.metrics = newPrometheusMetrics()
	}
	if cfg.openAPIHTTP == nil {
		// Default outbound HTTP client for OpenAPI / downstream-GraphQL
		// dispatches. Go's http.DefaultClient ships
		// MaxIdleConnsPerHost=2, which collapses to per-request
		// connection churn at any meaningful RPS — same bug class the
		// bench client tripped on. Pre-bake a sane default; operators
		// who want their own client still use WithOpenAPIClient.
		cfg.openAPIHTTP = newDefaultOutboundHTTPClient()
	}
	if len(cfg.adminToken) == 0 {
		tok, err := loadOrGenerateAdminToken(cfg.adminDataDir)
		if err != nil {
			panic(fmt.Sprintf("gateway: admin token: %v", err))
		}
		cfg.adminToken = tok
	}
	if cfg.uploadStore == nil && cfg.uploadDataDir != "" {
		s, err := NewFilesystemUploadStore(cfg.uploadDataDir, 24*time.Hour)
		if err != nil {
			panic(fmt.Sprintf("gateway: upload store: %v", err))
		}
		cfg.uploadStore = s
		cfg.uploadStoreOwned = true
	}
	life, cancel := context.WithCancel(context.Background())
	// One shared limiter across both metric sinks so the Prometheus
	// label set and the in-process stats dimension agree on which
	// callers are admitted (otherwise the admin UI and the scrape
	// would show different __other__ rollups).
	limiter := newCallerLimiter(cfg.callerIDMetricsTopK)
	stats := newStatsRegistry()
	pm, _ := cfg.metrics.(*prometheusMetrics)
	if pm != nil {
		pm.callerHeaders = cfg.callerHeaders
		pm.callerLimiter = limiter
	}
	recording := &statsRecordingMetrics{
		Metrics:       cfg.metrics,
		stats:         stats,
		callerHeaders: cfg.callerHeaders,
		callerLimiter: limiter,
	}
	cfg.metrics = recording
	g := &Gateway{
		cfg:         cfg,
		slots:       map[poolKey]*slot{},
		internal:    map[string]bool{},
		wsConns:     map[uintptr]context.CancelFunc{},
		wsLimit:     newWSLimiter(cfg.wsLimit),
		life:        life,
		lifeCancel:  cancel,
		dispatchers: ir.NewDispatchRegistry(),
		stats:       stats,
		stableChanged: make(chan struct{}, 16),
		tracer:      newTracer(cfg.tracerProvider),
	}
	// WithCallerIDDelegated needs the live *Gateway to look up the
	// _caller_auth/v1 pool at call time, so the extractor wrapper is
	// built here (after g exists) and wins over any extractor set
	// directly. WithCallerIDExtractor / WithCallerIDPublic /
	// WithCallerIDHMAC remain the per-request seam.
	if cfg.callerIDDelegated != nil {
		g.callerAuth = newCallerAuthDelegate(g, *cfg.callerIDDelegated)
		cfg.callerIDExtractor = g.callerAuth.resolve
	}
	if cfg.quota != nil {
		g.quotaAuth = newQuotaAuthDelegate(g, *cfg.quota)
		g.quotaAuth.enforce = cfg.quotaEnforce
	}
	if pm != nil {
		pm.callerExtractor = cfg.callerIDExtractor
	}
	recording.callerExtractor = cfg.callerIDExtractor
	if cfg.backpressure.MaxStreamsTotal > 0 {
		g.streamGlobalSem = make(chan struct{}, cfg.backpressure.MaxStreamsTotal)
	}
	// Seed the MCP allowlist from WithMCPInclude / WithMCPExclude /
	// WithMCPAutoInclude. Standalone gateways: this is the in-process
	// state until an operator POSTs to /admin/mcp/*. Cluster gateways:
	// the seed lands locally; the KV watch loop (startMCPConfigWatcher
	// once the cluster is bound) reconciles with the cluster-wide
	// record on next Put.
	if cfg.mcpSeedSet {
		g.mcpConfig = &mcpConfigState{cfg: cloneMCPConfig(cfg.mcpSeed)}
	}
	if !cfg.docCacheDisabled {
		g.planCache = graphql.NewPlanCache(graphql.PlanCacheOptions{
			MaxEntries:    cfg.docCacheSize,
			MaxQueryBytes: cfg.docCacheMaxQuery,
			Normalize:     cfg.docCacheNormalize,
		})
		// Surface the cache's hit/miss counters as Prometheus
		// counters at scrape time. Skipped silently when the
		// operator plugged in a custom Metrics impl — those callers
		// own their own collectors.
		if pm := unwrapPrometheusMetrics(cfg.metrics); pm != nil {
			pm.registry.MustRegister(newPlanCacheCollector(g.planCache))
		}
	}
	// Auto-install gwag.ps.v1.PubSub when a cluster is bound — the
	// pub/sub primitive needs NATS to do anything useful. Standalone
	// gateways skip the install (schema doesn't surface ps.pub/ps.sub),
	// matching how ps.pub/ps.sub error out when called without a
	// cluster.
	if cfg.cluster != nil {
		if err := g.installPubSubSlot(); err != nil {
			panic(fmt.Sprintf("gateway: install pubsub slot: %v", err))
		}
		if cfg.subAuth.Insecure {
			cfg.cluster.Server.Warnf("gateway: subscriptions are in insecure mode — any client can publish and subscribe without HMAC verification")
		}
	}
	// Apply runtime channel bindings (WithChannelBinding) to the
	// gateway-internal ps slot. Runs through the same cross-slot
	// uniqueness policy as proto-declarative bindings.
	if len(cfg.channelBindings) > 0 {
		if err := g.applyRuntimeBindingsLocked(cfg.channelBindings); err != nil {
			panic(fmt.Sprintf("gateway: apply channel bindings: %v", err))
		}
	}
	return g
}

// Close stops background goroutines (peer tracker, janitor). Safe to
// call multiple times. Does not close the bound *Cluster — owners of
// the cluster shut it down themselves.
//
// Stability: stable
func (g *Gateway) Close() {
	g.mu.Lock()
	tracker := g.peers
	g.peers = nil
	store := g.cfg.uploadStore
	owned := g.cfg.uploadStoreOwned
	mcpClients := g.closeMCPClientsLocked()
	g.mu.Unlock()
	for _, c := range mcpClients {
		_ = c.Close()
	}
	tracker.stop()
	g.lifeCancel()
	if owned {
		if fs, ok := store.(*FilesystemUploadStore); ok {
			fs.Close()
		}
	}
}

// Cluster returns the bound cluster, or nil if running standalone.
//
// Stability: stable
func (g *Gateway) Cluster() *Cluster {
	return g.cfg.cluster
}

// ServiceOption is a per-registration configuration function.
// Pass to AddProto / AddOpenAPI / AddGraphQL.
//
// Stability: stable
type ServiceOption func(*serviceConfig)
type serviceConfig struct {
	namespace      string
	version        string // canonical "vN"; empty → defaults to v1
	conn           grpc.ClientConnInterface
	internal       bool
	forwardHeaders []string
	httpClient     *http.Client

	// maxConcurrency caps simultaneous unary dispatches against this
	// service (pool / openAPISource). 0 → fall back to the gateway-
	// wide default (BackpressureOptions.MaxInflight). Negative is
	// rejected at registration.
	maxConcurrency int

	// maxConcurrencyPerInstance caps simultaneous unary dispatches
	// against any single replica. 0 → unbounded per replica
	// (concurrency limited only by the service-level cap above).
	// Negative is rejected at registration.
	maxConcurrencyPerInstance int

	// protoImports carries transitive .proto import bytes for
	// AddProtoBytes (multi-file .protos). Keyed by import path; nil
	// for single-file .protos.
	protoImports map[string][]byte
}

// isInternal reports whether ns is hidden from the public GraphQL
// schema. A namespace is internal if either:
//   - the operator passed AsInternal() at registration (recorded in
//     g.internal), or
//   - the namespace starts with "_". This blanket convention covers
//     reserved namespaces like _admin_auth / _events_auth /
//     _admin_events without requiring every caller to remember
//     AsInternal(). Internal namespaces still register in the
//     dispatch registry (so hooks can call them); they just don't
//     surface in the SDL.
//
// Callers must hold g.mu (or be in a path that doesn't race with
// concurrent registrations).
func (g *Gateway) isInternal(ns string) bool {
	if g.internal[ns] {
		return true
	}
	return len(ns) > 0 && ns[0] == '_'
}

// As overrides the default namespace (filename stem) for a registered
// proto. Collisions across registered protos are an error.
//
// Stability: stable
func As(namespace string) ServiceOption {
	return func(c *serviceConfig) { c.namespace = namespace }
}

// Version pins the (namespace, version) coordinate the registration
// joins. Accepts "vN" or "N". Empty / unset defaults to "v1". For
// AddProto / AddProtoBytes this is informational — proto's own
// version comes from the package; for AddOpenAPI / AddOpenAPIBytes /
// AddGraphQL it identifies the source within a namespace, mirroring
// the proto pool model: latest-flat under the namespace + every
// version addressable as `<ns>.<vN>` with @deprecated on older.
//
// Stability: stable
func Version(v string) ServiceOption {
	return func(c *serviceConfig) { c.version = v }
}

// ProtoImports passes transitive .proto import bytes to AddProtoBytes
// for multi-file protos (e.g. user.proto importing auth.proto).
// Keys are the import paths exactly as they appear in `import "..."`
// statements; values are the raw .proto bytes. Single-file .protos
// don't need this option. Well-known imports (google/protobuf/*)
// resolve automatically.
//
// Stability: stable
func ProtoImports(m map[string][]byte) ServiceOption {
	return func(c *serviceConfig) { c.protoImports = m }
}

// To wires the gRPC destination for a registered proto. Accepts either
// a host:port string (sugar for grpc.NewClient with insecure creds, for
// dev) or a caller-managed *grpc.ClientConn.
//
// Stability: stable
func To(dest any) ServiceOption {
	return func(c *serviceConfig) {
		switch v := dest.(type) {
		case grpc.ClientConnInterface:
			c.conn = v
		case string:
			// Boot-time To("host:port") uses the default pool size
			// without consulting cfg — the gateway isn't visible from
			// inside the ServiceOption closure. Operators who care
			// about per-conn fan-out drive registrations through the
			// control plane (the reconciler-side path consults
			// cfg.grpcConnPoolSize per WithGRPCConnPoolSize).
			c.conn = newBootLazyConn(v)
		default:
			panic(fmt.Sprintf("gateway.To: unsupported destination type %T", dest))
		}
	}
}

// AsInternal registers the proto in the callable registry but hides its
// services from the external GraphQL surface. Use for infrastructure
// services (auth, policy, lookup) that hooks call but external clients
// should not see.
//
// Stability: stable
func AsInternal() ServiceOption {
	return func(c *serviceConfig) { c.internal = true }
}

// ForwardHeaders sets the per-source allowlist of HTTP headers
// forwarded from the inbound GraphQL request to outbound OpenAPI
// dispatches. Replaces the default ([]string{"Authorization"}) when
// supplied. Pass an empty list to forward nothing.
//
// Distributed-tracing / correlation headers (W3C traceparent +
// tracestate + baggage, B3, x-request-id, the AWS / GCP trace headers)
// ride along automatically whenever forwarding is active — they don't
// need to be listed here. An empty list opts out of those too.
//
// Currently a no-op for AddProto / AddProtoBytes — gRPC dispatch
// uses ctx propagation, not HTTP headers.
//
// Stability: stable
func ForwardHeaders(headers ...string) ServiceOption {
	return func(c *serviceConfig) {
		c.forwardHeaders = append([]string(nil), headers...)
	}
}

// MaxConcurrency caps simultaneous unary dispatches against this
// service. Overrides the gateway-wide
// BackpressureOptions.MaxInflight default for this one registration
// — a heavy backend can declare 64 in-flight while light backends
// stay at the gateway default. 0 → fall back to the gateway default.
//
// Applies to AddProto / AddProtoBytes (per-pool sem) and
// AddOpenAPI / AddOpenAPIBytes (per-source sem). No-op for
// AddGraphQL (downstream-GraphQL stitching has no per-source sem).
//
// Stability: stable
func MaxConcurrency(n int) ServiceOption {
	return func(c *serviceConfig) { c.maxConcurrency = n }
}

// MaxConcurrencyPerInstance caps simultaneous unary dispatches
// against any single replica behind a service. New axis: the
// service-level cap (MaxConcurrency / gateway default) bounds the
// pool, this bounds each replica individually, so a 3-replica
// service with MaxConcurrencyPerInstance(50) holds at most
// 150 in-flight even if MaxConcurrency / the gateway default is
// higher. 0 → unbounded per replica (only the service cap applies).
//
// Same applicability as MaxConcurrency.
//
// Stability: stable
func MaxConcurrencyPerInstance(n int) ServiceOption {
	return func(c *serviceConfig) { c.maxConcurrencyPerInstance = n }
}

// OpenAPIClient overrides the *http.Client used for outbound
// dispatches against this OpenAPI source. Beats the gateway-wide
// default set by `WithOpenAPIClient(...)`.
//
// Use this when one backend needs a different transport (mTLS to a
// specific service, a per-source RoundTripper that injects a
// service-account token, etc.). No-op for AddProto /
// AddProtoDescriptor.
//
// Stability: stable
func OpenAPIClient(c *http.Client) ServiceOption {
	return func(sc *serviceConfig) { sc.httpClient = c }
}

// AddProtoBytes registers a service from raw .proto entrypoint
// bytes. Sibling of AddProto(path) and AddOpenAPIBytes — same shape:
// callers ship raw source, the gateway compiles via protocompile
// (SourceInfoStandard) so leading / trailing comments survive into
// the GraphQL SDL and MCP search corpus.
//
// `entry` is the virtual filename used in error messages and as the
// import-resolution anchor; `body` is the raw .proto bytes.
// Multi-file .protos with `import "..."` statements pass the
// transitive import map via the ProtoImports(map) option, or use
// AddProtoFS for the fs.FS-walk shape.
//
// Namespace defaults to the entry filename stem; override with As().
//
// Required ServiceOption: gateway.To(grpc.ClientConnInterface).
//
// Stability: stable
func (g *Gateway) AddProtoBytes(entry string, body []byte, opts ...ServiceOption) error {
	sc := &serviceConfig{}
	for _, o := range opts {
		o(sc)
	}
	if sc.conn == nil {
		return fmt.Errorf("gateway: AddProtoBytes(%s): missing To(...)", entry)
	}
	fd, err := compileProtoBytes(entry, body, sc.protoImports)
	if err != nil {
		return fmt.Errorf("gateway: AddProtoBytes(%s): %w", entry, err)
	}
	if sc.namespace == "" {
		base := entry
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		sc.namespace = strings.TrimSuffix(base, filepath.Ext(base))
	}
	return g.addProtoFromDescriptor(fd, sc, fmt.Sprintf("protobytes:%s", entry), fmt.Sprintf("AddProtoBytes(%s)", entry))
}

// resolveProtoFSBytes walks fsys and returns the bytes of `entry`
// plus a map of every other .proto under the FS as imports. Mirrors
// controlclient.resolveProtoFS — kept private to gw, since the
// public surface is AddProtoFS.
func resolveProtoFSBytes(fsys fs.FS, entry string) ([]byte, map[string][]byte, error) {
	src, err := fs.ReadFile(fsys, entry)
	if err != nil {
		return nil, nil, fmt.Errorf("read entry %q: %w", entry, err)
	}
	imports := map[string][]byte{}
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".proto") {
			return nil
		}
		key := path.Clean(p)
		if key == path.Clean(entry) {
			return nil
		}
		body, err := fs.ReadFile(fsys, p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		imports[key] = body
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(imports) == 0 {
		imports = nil
	}
	return src, imports, nil
}

// AddProtoFS is the multi-file ergonomic shape: pass any fs.FS
// (embed.FS, os.DirFS(...), tar/zip wrappers) and the entrypoint
// filename within it. The gateway reads `entry` as the proto source,
// every other .proto under fsys becomes a transitive import.
//
// Mirrors controlclient.Service{ProtoFS, ProtoEntry} on the
// SelfRegister side. Same compile pipeline as AddProtoBytes.
//
// Stability: stable
func (g *Gateway) AddProtoFS(fsys fs.FS, entry string, opts ...ServiceOption) error {
	src, imports, err := resolveProtoFSBytes(fsys, entry)
	if err != nil {
		return fmt.Errorf("gateway: AddProtoFS(%s): %w", entry, err)
	}
	opts = append([]ServiceOption{ProtoImports(imports)}, opts...)
	return g.AddProtoBytes(entry, src, opts...)
}

// addProtoFromDescriptor is the shared tail used by AddProto and
// AddProtoBytes after compile: resolves namespace, hashes the
// descriptor, joins the pool. Caller's `sc` carries any explicit
// namespace override; this function fills in a default only when
// sc.namespace is still empty.
func (g *Gateway) addProtoFromDescriptor(fd protoreflect.FileDescriptor, sc *serviceConfig, addr, label string) error {
	ns := sc.namespace
	if ns == "" {
		base := string(fd.Path())
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		ns = strings.TrimSuffix(base, ".proto")
	}
	hash, err := hashFromFileDescriptor(fd)
	if err != nil {
		return fmt.Errorf("gateway: hash %s: %w", label, err)
	}
	ver, _, err := parseVersion(sc.version)
	if err != nil {
		return fmt.Errorf("gateway: %s: %w", label, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if sc.internal {
		g.internal[ns] = true
	}
	return g.joinPoolLocked(poolEntry{
		namespace:                 ns,
		version:                   ver,
		hash:                      hash,
		file:                      fd,
		addr:                      addr,
		conn:                      sc.conn,
		owner:                     "",
		maxConcurrency:            sc.maxConcurrency,
		maxConcurrencyPerInstance: sc.maxConcurrencyPerInstance,
	})
}

// AddProto parses a .proto file and registers it as a single replica
// under (namespace, version=v1). Bodies of services are routed to the
// destination set by To(). Namespace defaults to the filename stem;
// override with As().
//
// Safe to call after Handler() — the schema rebuilds and atomically
// replaces the live one, so dynamic add at runtime works the same way
// boot-time registration does.
//
// Stability: stable
func (g *Gateway) AddProto(path string, opts ...ServiceOption) error {
	sc := &serviceConfig{}
	for _, o := range opts {
		o(sc)
	}
	if sc.conn == nil {
		return fmt.Errorf("gateway: AddProto(%s): missing To(...)", path)
	}
	fd, err := loadProto(path)
	if err != nil {
		return err
	}
	ns := sc.namespace
	if ns == "" {
		ns = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	hash, err := hashFromFileDescriptor(fd)
	if err != nil {
		return fmt.Errorf("gateway: hash %s: %w", path, err)
	}
	ver, _, err := parseVersion(sc.version)
	if err != nil {
		return fmt.Errorf("gateway: AddProto(%s): %w", path, err)
	}
	addr := fmt.Sprintf("addproto:%s", path)
	g.mu.Lock()
	defer g.mu.Unlock()
	if sc.internal {
		g.internal[ns] = true
	}
	return g.joinPoolLocked(poolEntry{
		namespace:                 ns,
		version:                   ver,
		hash:                      hash,
		file:                      fd,
		addr:                      addr,
		conn:                      sc.conn,
		owner:                     "", // boot-time, never evicted
		maxConcurrency:            sc.maxConcurrency,
		maxConcurrencyPerInstance: sc.maxConcurrencyPerInstance,
	})
}

// poolEntry is the input to joinPoolLocked — what a single
// service-binding registration provides.
type poolEntry struct {
	namespace string
	version   string // canonical "vN"
	hash      [32]byte
	file      protoreflect.FileDescriptor
	addr      string
	conn      grpc.ClientConnInterface
	owner     string
	replicaID string // KV-side replica id, "" for boot-time AddProto

	// maxConcurrency / maxConcurrencyPerInstance are captured on the
	// FIRST registration of a (namespace, version) pool — sizing the
	// service-level sem and the per-replica sem respectively. Later
	// joins must agree on both, like proto hash. 0 means "use the
	// gateway default" (service) or "unbounded" (instance). Negative
	// is rejected upstream.
	maxConcurrency            int
	maxConcurrencyPerInstance int
}

// joinPoolLocked finds or creates the pool for (namespace, version) and
// adds a replica. Pool creation triggers a schema rebuild; replica
// churn within an existing pool does not. Caller must hold g.mu.
//
// Tier policy (unstable swap, vN immutability, cross-kind reject)
// is centralized in `registerSlotLocked` — see slot.go.
func (g *Gateway) joinPoolLocked(e poolEntry) error {
	if e.maxConcurrency < 0 {
		return fmt.Errorf("gateway: pool %s/%s: max_concurrency must be ≥ 0", e.namespace, e.version)
	}
	if e.maxConcurrencyPerInstance < 0 {
		return fmt.Errorf("gateway: pool %s/%s: max_concurrency_per_instance must be ≥ 0", e.namespace, e.version)
	}
	g.warnSubscribeDelegateDeprecated(e.namespace, e.version)
	key := poolKey{namespace: e.namespace, version: e.version}
	bindings := extractChannelBindings(e.file)
	existed, err := g.registerSlotLocked(slotKindProto, key, e.hash, e.maxConcurrency, e.maxConcurrencyPerInstance, bindings)
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	s := g.slots[key]
	if existed {
		p := s.proto
		p.addReplica(g.newReplica(p, e))
		g.publishServiceChange(adminEventsActionRegistered, e.namespace, e.version, e.addr, uint32(p.replicaCount()))
		return nil
	}
	_, n, err := parseVersion(e.version)
	if err != nil {
		// parseVersion already accepted this version on its first run
		// (registerSlotLocked uses the same key) — defensive: roll the
		// slot back so the gateway state stays in sync.
		delete(g.slots, key)
		return fmt.Errorf("gateway: %w", err)
	}
	p := &pool{
		key:                       key,
		versionN:                  n,
		file:                      e.file,
		hash:                      e.hash,
		maxConcurrency:            e.maxConcurrency,
		maxConcurrencyPerInstance: e.maxConcurrencyPerInstance,
	}
	// Service-level cap: registration override beats gateway default.
	// 0 → fall back to default; default of 0 → unbounded.
	semSize := e.maxConcurrency
	if semSize == 0 {
		semSize = g.cfg.backpressure.MaxInflight
	}
	if semSize > 0 {
		p.sem = make(chan struct{}, semSize)
	}
	if g.cfg.backpressure.MaxStreams > 0 {
		p.streamSem = make(chan struct{}, g.cfg.backpressure.MaxStreams)
	}
	p.addReplica(g.newReplica(p, e))
	s.proto = p
	g.bakeSlotIRLocked(s)
	g.advanceStableLocked(e.namespace, n)
	g.publishServiceChange(adminEventsActionRegistered, e.namespace, e.version, e.addr, uint32(p.replicaCount()))
	if g.schema.Load() != nil {
		// Pool creation always rebuilds — covers all three cases:
		// namespace appeared, new version under existing namespace, or
		// latest changed (highest versionN moved).
		return g.assembleLocked()
	}
	return nil
}

// newReplica constructs a replica with its per-instance sem sized
// against the pool's MaxConcurrencyPerInstance setting (nil sem when
// unset → unbounded per replica).
func (g *Gateway) newReplica(p *pool, e poolEntry) *replica {
	r := &replica{
		id:    e.replicaID,
		addr:  e.addr,
		owner: e.owner,
		conn:  e.conn,
	}
	if p.maxConcurrencyPerInstance > 0 {
		r.sem = make(chan struct{}, p.maxConcurrencyPerInstance)
	}
	return r
}

// removeReplicaByIDLocked finds and drops the single replica matching
// the given (ns,ver,replicaID). If the pool empties, it's deleted.
// Rebuilds the schema if a pool was destroyed. Caller holds g.mu.
// Returns the removed replica (for conn refcount cleanup) or nil if
// not found.
func (g *Gateway) removeReplicaByIDLocked(ns, ver, replicaID string) (*replica, error) {
	key := poolKey{namespace: ns, version: ver}
	p := g.protoSlot(key)
	if p == nil {
		return nil, nil
	}
	r := p.removeReplicaByID(replicaID)
	if r == nil {
		return nil, nil
	}
	g.publishServiceChange(adminEventsActionDeregistered, ns, ver, r.addr, uint32(p.replicaCount()))
	if p.replicaCount() == 0 {
		g.releaseSlotLocked(key)
		if g.schema.Load() != nil {
			return r, g.assembleLocked()
		}
	}
	return r, nil
}

// removeReplicasByOwnerLocked walks all pools removing replicas with
// the given owner. If any pool empties, drop it. Rebuilds the schema
// if any pool was created or destroyed (replica churn within a still-
// populated pool doesn't change the schema).
func (g *Gateway) removeReplicasByOwnerLocked(owner string) (removed int, err error) {
	rebuild := false
	for key, s := range g.slots {
		if s.kind != slotKindProto {
			continue
		}
		p := s.proto
		n := p.removeReplicasByOwner(owner)
		if n == 0 {
			continue
		}
		removed += n
		// One ServiceChange per removed pool — replica-level granularity
		// here would spam during cluster reconciliation. Use addr=""
		// because the replica list (and addrs) is already gone.
		g.publishServiceChange(adminEventsActionDeregistered, key.namespace, key.version, "", uint32(p.replicaCount()))
		if p.replicaCount() == 0 {
			g.releaseSlotLocked(key)
			rebuild = true
		}
	}
	if rebuild && g.schema.Load() != nil {
		return removed, g.assembleLocked()
	}
	return removed, nil
}

// Use appends Transforms to the gateway. Each Transform may carry any
// combination of schema-rewrite rules (Schema) and runtime middleware
// (Runtime); empty halves no-op.
//
// Schema-half rewrites (HideType / HidePath / NullableType /
// NullablePath — the IR-mutating injectors) are baked into each
// slot's cached IR at registration time, so adding new ones via
// Use(...) requires re-baking every existing slot. Runtime-half
// middleware and header injectors are captured into dispatcher
// closures at the next schema rebuild — those track via the same
// path they did before.
//
// Stability: stable
func (g *Gateway) Use(transforms ...Transform) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.transforms = append(g.transforms, transforms...)
	// Re-bake every slot so the new schema-half rewrites take effect
	// in slot.ir without waiting for a register/dereg event. Use(...)
	// itself does NOT trigger a schema rebuild — the existing
	// "transforms apply on next rebuild" semantics are preserved;
	// what changes is that slot.ir already reflects the new injectors
	// when the next rebuild reads it.
	g.rebakeAllSlotsLocked()
	// Evaluate InjectPath landings against the live IR so newly
	// registered rules surface a one-time dormant warning and any
	// state transitions emit exactly once. Same evaluator runs at
	// schema rebuild (assembleLocked).
	transitions := g.evalInjectPathStatesLocked()
	g.logInjectPathTransitions(transitions)
}

// Handler returns the http.Handler that serves the GraphQL schema.
// First call assembles the schema and starts hot-swap mode; subsequent
// AddProto / control-plane registrations rebuild the schema in place.
//
// Mount it directly on the GraphQL path (e.g. "/graphql"); pair with
// SchemaHandler at "/schema" if you want a codegen-friendly export.
//
// Stability: stable
func (g *Gateway) Handler() http.Handler {
	g.mu.Lock()
	if g.schema.Load() == nil {
		if err := g.assembleLocked(); err != nil {
			g.mu.Unlock()
			return errorHandler(err)
		}
	}
	g.mu.Unlock()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isWebSocketUpgrade(r) {
			release, reason, ok := g.wsLimit.acquire(r)
			if !ok {
				g.cfg.metrics.RecordWSRejected(reason)
				http.Error(w, "ws limit exceeded: "+reason, http.StatusTooManyRequests)
				return
			}
			defer release()
			// WebSocket subscriptions live as long as the client stays
			// connected; request_*_seconds is a per-request histogram,
			// not a per-stream-lifetime one. Skip recording.
			g.serveWebSocket(w, r)
			return
		}
		schema := g.schema.Load()
		ctx := g.tracer.extractHTTP(r.Context(), r.Header)
		ctx, span := g.tracer.startIngressSpan(ctx, "gateway.graphql",
			ingressAttr("graphql"),
			httpMethodAttr(r.Method),
			httpTargetAttr(r.URL.Path),
		)
		defer span.End()
		ctx = withTracer(ctx, g.tracer)
		ctx, accum := withDispatchAccumulator(ctx)
		ctx = withInjectCache(ctx)
		ctx = withHTTPRequest(ctx, r)
		start := time.Now()
		// Browser UI render — rare, never on the hot path. The cached
		// graphql-go/handler is built once per assembleLocked so this
		// branch costs no per-request alloc. WithoutGraphiQL() drops
		// the cache and falls through to the JSON path.
		if gh := g.graphiqlHandler.Load(); gh != nil && isGraphiQLRequest(r) {
			gh.ServeHTTP(w, r.WithContext(ctx))
		} else {
			g.serveGraphQLJSON(ctx, schema, w, r)
		}
		total := time.Since(start)
		dispatchSum := time.Duration(accum.Sum.Load())
		g.cfg.metrics.RecordRequest("graphql", total, total-dispatchSum)
		g.logRequestLine("graphql", r.URL.Path, total, dispatchSum, int(accum.Count.Load()))
	})
}

// isGraphiQLRequest mirrors graphql-go/handler's GraphiQL trigger:
// non-raw browser requests asking for text/html and not application/json.
// We detect this up front so the JSON hot path doesn't need to thread
// through handler.Handler.
func isGraphiQLRequest(r *http.Request) bool {
	if _, raw := r.URL.Query()["raw"]; raw {
		return false
	}
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		return false
	}
	return strings.Contains(accept, "text/html")
}

// serveGraphQLJSON is the GraphQL execute + JSON response path with
// the parsed+validated AST cache inserted between request parsing and
// graphql.Execute. Equivalent in behavior to graphql-go/handler.New
// with Pretty: true on a hit; on a miss we go through parser.Parse +
// graphql.ValidateDocument once and store the result.
func (g *Gateway) serveGraphQLJSON(ctx context.Context, schema *graphql.Schema, w http.ResponseWriter, r *http.Request) {
	// Cap inline multipart bodies at WithUploadLimit so a misbehaving
	// client can't push an unbounded body through the parser. The cap
	// is enforced via MaxBytesReader (the reader returns an error once
	// the limit is exceeded; the multipart machinery surfaces that
	// error to the GraphQL ingress, which renders the JSON error
	// envelope downstream). MaxBytesReader writes a 413 to w on
	// overshoot, which the existing parseErr handling then overrides
	// with our richer envelope — that's fine since headers haven't
	// been flushed yet.
	if g.cfg.uploadLimit > 0 && r.Body != nil {
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "multipart/") {
			r.Body = http.MaxBytesReader(w, r.Body, g.cfg.uploadLimit)
		}
	}
	opts, parseErr := parseGraphqlRequest(r)
	if parseErr != nil {
		// Today this fires only on a malformed multipart/form-data
		// body — the JSON / form parsers still degrade silently to an
		// empty options struct, matching the upstream handler. Surface
		// the multipart parse failure as a GraphQL errors envelope so
		// uploaders get a recognisable message instead of "no query".
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(&graphql.Result{
			Errors: []gqlerrors.FormattedError{gqlerrors.NewFormattedError(parseErr.Error())},
		})
		return
	}

	pr := g.planCache.Get(schema, opts.Query, opts.OperationName)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	switch {
	case len(pr.Errors) > 0:
		// Pre-execution errors (parse / validate). Rare path; small
		// payload; the encoder is fine here.
		_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: pr.Errors})
	case pr.Plan != nil:
		args := opts.Variables
		if len(pr.SynthArgs) > 0 {
			merged := make(map[string]interface{}, len(args)+len(pr.SynthArgs))
			for k, v := range args {
				merged[k] = v
			}
			for k, v := range pr.SynthArgs {
				merged[k] = v
			}
			args = merged
		}
		// Append-mode hot path: walk the cached plan, emit JSON
		// straight into a pooled buffer, write bytes to the wire.
		// Skips the `map[string]any` result tree + scalar boxing +
		// final json.Encode at the egress — the fork's ExecutePlan
		// vs ExecutePlanAppend benchmarks show ~42% alloc reduction
		// on this codepath.
		buf := graphqlBufPool.Get().(*[]byte)
		body, errs := graphql.ExecutePlanAppend(pr.Plan, graphql.ExecuteParams{
			Schema:        *schema,
			OperationName: opts.OperationName,
			Args:          args,
			Context:       ctx,
		}, (*buf)[:0])
		if len(errs) > 0 {
			// Spec-level errors (variable coercion, etc.) occurred
			// before data assembly. ExecutePlanAppend's doc says the
			// caller decides whether to surface them separately or
			// fold them into a fresh response — we emit a clean
			// errors envelope so the partial bytes (if any) don't
			// confuse downstream clients.
			_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: errs})
		} else {
			_, _ = w.Write(body)
		}
		// Pool the (possibly grown) backing array unless it ballooned
		// — a one-off MB response shouldn't pin a fat alloc forever.
		if cap(body) <= graphqlBufPoolMax {
			*buf = body[:0]
			graphqlBufPool.Put(buf)
		}
	default:
		// Plan == nil with no errors shouldn't happen, but stay
		// safe by deferring to graphql.Do (parse + validate +
		// plan + execute).
		_ = json.NewEncoder(w).Encode(graphql.Do(graphql.Params{
			Schema:         *schema,
			RequestString:  opts.Query,
			RootObject:     nil,
			VariableValues: opts.Variables,
			OperationName:  opts.OperationName,
			Context:        ctx,
		}))
	}
}

// graphqlBufPool reuses []byte buffers across GraphQL responses to
// keep allocs out of the hot path. New buffers start at 4 KB —
// enough for most responses; ExecutePlanAppend grows the slice as
// needed. graphqlBufPoolMax caps the size we'll hand back to the
// pool so a one-off megabyte response doesn't pin a fat allocation.
const graphqlBufPoolMax = 64 * 1024

var graphqlBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// SchemaHandler returns an http.Handler that exports the GraphQL
// schema as SDL or introspection JSON. Mount it at /schema/graphql
// for the canonical path; SchemaProtoHandler and SchemaOpenAPIHandler
// are siblings:
//
//	GET /schema/graphql                          → SDL (default)
//	GET /schema/graphql?format=json              → introspection JSON
//	GET /schema/graphql?service=ns:ver           → SDL filtered to ns:ver
//	GET /schema/proto?service=ns:ver             → FileDescriptorSet (binary)
//	GET /schema/openapi?service=ns               → re-emit ingested OpenAPI specs
//
// Selector grammar (shared across the /schema/* family):
//   - ns        → all versions of ns
//   - ns:vN     → just that version of ns (proto pools only; OpenAPI
//     / downstream-GraphQL sources have no version axis)
//   - missing   → no filter, full schema
//
// Filtered requests build a fresh schema per call (cached g.schema is
// the unfiltered one). Codegen pipelines that always want the whole
// thing pay no extra cost.
//
// Stability: stable
func (g *Gateway) SchemaHandler() http.Handler {
	g.mu.Lock()
	if g.schema.Load() == nil {
		if err := g.assembleLocked(); err != nil {
			g.mu.Unlock()
			return errorHandler(err)
		}
	}
	g.mu.Unlock()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		selectors, err := parseProtoSelectors(r.URL.Query().Get("service"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var schema *graphql.Schema
		var annotations *ir.AnnotationIndex
		if len(selectors) == 0 {
			schema = g.schema.Load()
			if schema == nil {
				http.Error(w, "schema not assembled", http.StatusServiceUnavailable)
				return
			}
			annotations = g.schemaAnnotations.Load()
		} else {
			g.mu.Lock()
			built, builtAnn, err := g.buildSchemaLocked(schemaFilter{selectors: selectors})
			g.mu.Unlock()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			schema = built
			annotations = builtAnn
		}
		switch r.URL.Query().Get("format") {
		case "json":
			result := graphql.Do(graphql.Params{Schema: *schema, RequestString: ir.IntrospectionQuery})
			w.Header().Set("Content-Type", "application/json")
			_ = ir.WriteJSON(w, result)
		default:
			w.Header().Set("Content-Type", "application/graphql; charset=utf-8")
			_, _ = w.Write([]byte(ir.PrintSchemaSDL(schema, annotations)))
		}
	})
}

type httpRequestCtxKey struct{}

// withHTTPRequest stores r on ctx so middleware and resolvers can read
// the inbound HTTP request (headers, cookies, remote addr) without
// having to plumb it as a separate argument.
func withHTTPRequest(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, httpRequestCtxKey{}, r)
}

// HTTPRequestFromContext returns the HTTP request that originated this
// gateway call, or nil if ctx wasn't created by the gateway handler.
//
// Stability: stable
func HTTPRequestFromContext(ctx context.Context) *http.Request {
	r, _ := ctx.Value(httpRequestCtxKey{}).(*http.Request)
	return r
}

func errorHandler(err error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	})
}

// ---------------------------------------------------------------------
// Middleware primitives
// ---------------------------------------------------------------------

// Handler is the dispatch function type used in runtime middleware chains.
// A Handler receives the inbound proto message and returns the outbound one.
// Middleware wraps a Handler to intercept, modify, or short-circuit the call.
//
// Stability: stable
type Handler func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error)

// Middleware wraps a Handler to build a chain of interceptors.
// The canonical pattern: func(next Handler) Handler { return func(...) { ... } }
// Set on Transform.Runtime and passed to Use.
//
// Stability: stable
type Middleware func(next Handler) Handler

// Transform bundles every reshaping concern that lands per gateway-Use
// call: a list of typed schema rewrites (Schema, e.g. HideType), a
// runtime middleware (Runtime), and a list of outbound header
// injectors (Headers, e.g. InjectHeader). Empty halves no-op.
//
// Inventory records the high-level injector(s) this Transform
// represents, captured at construction so the admin inventory endpoint
// can surface what the operator registered without re-deriving it from
// the SchemaRewrite / Headers shapes.
//
// Stability: stable
type Transform struct {
	Schema    []SchemaRewrite
	Runtime   Middleware
	headers   []headerInjector
	inventory []injectorRecord
}

// headerInjector stamps one outbound header (HTTP, OpenAPI dispatch)
// or gRPC metadata key (proto dispatch) on every dispatch the gateway
// sends. Constructed via InjectHeader. ForwardHeaders' inbound
// allowlist is unaffected — header injection writes through directly.
//
// Hide(true): Fn always sees current=nil. Hide(false): Fn sees the
// inbound HTTP request's value for Name (nil when absent or when the
// request isn't HTTP). Returning "" skips the header for this
// dispatch.
type headerInjector struct {
	Name string
	Hide bool
	Fn   func(ctx context.Context, current *string) (string, error)
}

// SchemaRewrite mutates IR services in place to reshape the external
// surface — strip fields, flip nullability, etc. Concrete rewrites are
// constructed via HideType and friends; renderers that need rewrite-
// specific data (e.g. the proto FDS exporter pulling out hidden type
// names) type-assert to the concrete struct.
//
// Stability: stable
type SchemaRewrite interface {
	apply(svcs []*ir.Service)
}

// ---------------------------------------------------------------------
// Reject — short-circuit error
// ---------------------------------------------------------------------

// Reject constructs a short-circuit error for use in resolvers.
// The code is mapped to a gRPC / HTTP status by the gateway ingress.
//
// Stability: stable
func Reject(code Code, msg string) error {
	return &rejection{Code: code, Msg: msg}
}

// Code is the numeric error code passed to Reject.
// Resolver middleware uses these to express well-known failure modes
// (auth, quota, not-found) without importing gRPC status codes.
//
// Stability: stable
type Code int

// Error codes for use with Reject.
//
// Stability: stable
const (
	CodeUnauthenticated Code = iota + 1
	CodePermissionDenied
	CodeResourceExhausted
	CodeInvalidArgument
	CodeNotFound
	CodeInternal
)

type rejection struct {
	Code Code
	Msg  string
	// RetryAfter is surfaced as the HTTP `Retry-After` header on
	// ingress error envelopes (writeIngressDispatchError). Zero means
	// no header — only quota-exhaustion rejections set it today.
	RetryAfter time.Duration
}

func (r *rejection) Error() string { return r.Msg }
func (r *rejection) Extensions() map[string]any {
	ext := map[string]any{"code": r.Code.String()}
	if r.RetryAfter > 0 {
		ext["retryAfterSeconds"] = int(r.RetryAfter / time.Second)
	}
	return ext
}

// String is a method on Code.
//
// Stability: stable
func (c Code) String() string {
	switch c {
	case CodeUnauthenticated:
		return "UNAUTHENTICATED"
	case CodePermissionDenied:
		return "PERMISSION_DENIED"
	case CodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case CodeInvalidArgument:
		return "INVALID_ARGUMENT"
	case CodeNotFound:
		return "NOT_FOUND"
	case CodeInternal:
		return "INTERNAL"
	default:
		return "UNKNOWN"
	}
}

// ---------------------------------------------------------------------
// lazyConn defers gRPC dials until first use, so To("host:port")
// doesn't error at registration if the destination isn't dialable yet.
// Pool fan-out comes from cfg.grpcConnPoolSize (see WithGRPCConnPoolSize).
// ---------------------------------------------------------------------

// lazyConn is now a thin wrapper around lazyConnPool that fixes
// the dial settings (insecure creds + a fan-out pool size). Kept
// as a separate type so external imports referring to `*lazyConn`
// (the openapi.go ForwardHeaders path checks it structurally) keep
// compiling.
type lazyConn struct {
	*lazyConnPool
}

// newBootLazyConn is the boot-time To("host:port") path — picks
// the default pool size because the gateway config isn't visible
// inside the ServiceOption closure. Reconciler-side dials get the
// operator-configurable size via WithGRPCConnPoolSize.
func newBootLazyConn(addr string) *lazyConn {
	return newLazyConnWithSize(addr, defaultGRPCConnPoolSize)
}

func newLazyConnWithSize(addr string, size int) *lazyConn {
	if size <= 0 {
		size = defaultGRPCConnPoolSize
	}
	return &lazyConn{
		lazyConnPool: &lazyConnPool{
			addr: addr,
			size: size,
			dial: func(addr string, n int) (*connPool, error) {
				return dialConnPool(addr, n,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
			},
		},
	}
}
