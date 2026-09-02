package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IodeSystems/graphql-go/v2/language/ast"
	"github.com/getkin/kin-openapi/openapi3"

	"github.com/iodesystems/gwag/gw/ir"
)

// openAPIDispatcher implements ir.Dispatcher for one OpenAPI source
// + one operation. Inner work mirrors the inline body in
// gw/openapi.go (pre-cutover): pickReplica → dispatchOpenAPI →
// RecordDispatch. backpressureMiddleware wraps the outside.
//
// One per (source, operation) at schema build time. The captured
// op pointer / method / pathTemplate / forwardHeaders / metrics
// labels live for the lifetime of the source — the dispatcher is
// short-lived only across schema rebuilds.
type openAPIDispatcher struct {
	src             *openAPISource
	op              *openapi3.Operation
	method          string
	pathTemplate    string
	forwardHeaders  []string
	headerInjectors []headerInjector
	metrics         Metrics
	bp              BackpressureOptions
	ns              string
	ver             string
	label           string // "<METHOD> <pathTemplate>" — proto-side metric parity

	// Set by registerOpenAPIDispatchersLocked after construction; used
	// only by the AppendDispatcher path to enable type-aware projection
	// (union discrimination, __typename emission with local prefix).
	// Nil-safe — appendOpenAPIValueTyped falls back to untyped walking
	// when svc/irOp are nil.
	svc        *ir.Service
	irOp       *ir.Operation
	typePrefix string
}

func newOpenAPIDispatcher(src *openAPISource, op *openapi3.Operation, method, pathTemplate string, headers []headerInjector, metrics Metrics, bp BackpressureOptions) *openAPIDispatcher {
	return &openAPIDispatcher{
		src:             src,
		op:              op,
		method:          method,
		pathTemplate:    pathTemplate,
		forwardHeaders:  src.forwardHeaders,
		headerInjectors: headers,
		metrics:         metrics,
		bp:              bp,
		ns:              src.namespace,
		ver:             src.version,
		label:           method + " " + pathTemplate,
	}
}

func (d *openAPIDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.openapi",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
		httpMethodAttr(d.method),
		httpRouteAttr(d.pathTemplate),
	)
	defer span.End()
	start := time.Now()
	r := d.src.pickReplica()
	if r == nil {
		err := Reject(CodeInternal, fmt.Sprintf("openapi: no live replicas for %s/%s", d.ns, d.ver))
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return nil, err
	}
	releaseInstance, err := acquireOpenAPIReplicaSlot(ctx, r, d.src, d.label, d.metrics, d.bp)
	if err != nil {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return nil, err
	}
	defer releaseInstance()

	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	resp, err := dispatchOpenAPI(ctx, d.method, r.baseURL, d.pathTemplate, d.op, args, d.forwardHeaders, d.headerInjectors, r.httpClient, d.src.uploadStore)
	elapsed := time.Since(start)
	d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
	addDispatchTime(ctx, elapsed)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DispatchAppend is the byte-splice variant. dispatchOpenAPIRaw
// returns the upstream HTTP body without json.Unmarshal; we then
// walk the bytes in lockstep with the local selection AST via
// appendOpenAPIValue, emitting only the requested fields. Skips both
// the full-tree map allocation and graphql-go's per-leaf-resolver
// projection.
//
// Limitation: scalar type coercion (e.g. upstream `"42"` → local
// Int) does not happen on this path. OpenAPI specs that publish
// loose JSON types relative to the local GraphQL schema should stay
// on the Dispatch path (which goes through graphql-go's Serialize).
// The dispatcher implements both methods; the renderer's
// AppendDispatcher capability check installs ResolveAppend
// unconditionally when this dispatcher is wired — tighten the spec
// or fall back via a wrapping middleware to opt out.
func (d *openAPIDispatcher) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.openapi",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
		httpMethodAttr(d.method),
		httpRouteAttr(d.pathTemplate),
	)
	defer span.End()
	start := time.Now()
	r := d.src.pickReplica()
	if r == nil {
		err := Reject(CodeInternal, fmt.Sprintf("openapi: no live replicas for %s/%s", d.ns, d.ver))
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return dst, err
	}
	releaseInstance, err := acquireOpenAPIReplicaSlot(ctx, r, d.src, d.label, d.metrics, d.bp)
	if err != nil {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return dst, err
	}
	defer releaseInstance()

	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	respBytes, err := dispatchOpenAPIRaw(ctx, d.method, r.baseURL, d.pathTemplate, d.op, args, d.forwardHeaders, d.headerInjectors, r.httpClient, d.src.uploadStore)
	elapsed := time.Since(start)
	d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
	addDispatchTime(ctx, elapsed)
	if err != nil {
		return dst, err
	}
	if len(respBytes) == 0 {
		return append(dst, "null"...), nil
	}

	info := graphQLForwardInfoFrom(ctx)
	var sel *ast.SelectionSet
	if info != nil && len(info.FieldASTs) > 0 {
		sel = info.FieldASTs[0].SelectionSet
	}
	walker := openAPIWalker{svc: d.svc, prefix: d.typePrefix}
	var outRef *ir.TypeRef
	if d.irOp != nil {
		outRef = d.irOp.Output
	}
	return walker.appendValue(dst, json.RawMessage(respBytes), sel, outRef)
}

var _ ir.AppendDispatcher = (*openAPIDispatcher)(nil)

// acquireOpenAPIReplicaSlot is the per-instance counterpart to
// acquireBackpressureSlot for OpenAPI sources. Same shape as the
// proto-side acquireReplicaSlot (kept distinct because pool and
// openAPISource have separate sem fields and metric namespaces).
func acquireOpenAPIReplicaSlot(ctx context.Context, r *openAPIReplica, src *openAPISource, label string, metrics Metrics, bp BackpressureOptions) (release func(), err error) {
	if r.sem == nil {
		return func() {}, nil
	}
	cfg := backpressureConfig{
		Sem:         r.sem,
		Queueing:    &r.queueing,
		MaxWaitTime: bp.MaxWaitTime,
		Metrics:     metrics,
		Namespace:   src.namespace,
		Version:     src.version,
		Label:       label,
		Kind:        "unary_instance",
		Replica:     r.baseURL,
	}
	return acquireBackpressureSlot(ctx, cfg)
}

// openAPIBackpressureConfig bundles a source's per-dispatch knobs
// for backpressureMiddleware. Sibling of poolBackpressureConfig in
// gw/proto_dispatcher.go — kept separate because openAPISource and
// pool are separate types with separate sem / queueing fields.
func openAPIBackpressureConfig(src *openAPISource, label string, metrics Metrics, bp BackpressureOptions) backpressureConfig {
	return backpressureConfig{
		Sem:         src.sem,
		Queueing:    &src.queueing,
		MaxWaitTime: bp.MaxWaitTime,
		Metrics:     metrics,
		Namespace:   src.namespace,
		Version:     src.version,
		Label:       label,
		Kind:        "unary",
	}
}

var _ ir.Dispatcher = (*openAPIDispatcher)(nil)
