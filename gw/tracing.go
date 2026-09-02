package gateway

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/metadata"
)

const tracerName = "github.com/iodesystems/gwag/v2/gw"

// Canonical span attributes. Three keys are shared across every
// ingress + dispatch span (gateway.ingress / gateway.namespace /
// gateway.method); the http.* / rpc.* keys follow OpenTelemetry
// semantic conventions so existing dashboards can render them.
const (
	attrIngress    = "gateway.ingress"
	attrNamespace  = "gateway.namespace"
	attrMethod     = "gateway.method"
	attrVersion    = "gateway.version"
	attrOperation  = "gateway.graphql.operation"
	attrHTTPMethod = "http.method"
	attrHTTPTarget = "http.target"
	attrHTTPRoute  = "http.route"
	attrRPCSystem  = "rpc.system"
	attrRPCMethod  = "rpc.method"
)

func ingressAttr(kind string) attribute.KeyValue   { return attribute.String(attrIngress, kind) }
func namespaceAttr(ns string) attribute.KeyValue   { return attribute.String(attrNamespace, ns) }
func methodAttr(m string) attribute.KeyValue       { return attribute.String(attrMethod, m) }
func versionAttr(v string) attribute.KeyValue      { return attribute.String(attrVersion, v) }
func httpMethodAttr(m string) attribute.KeyValue   { return attribute.String(attrHTTPMethod, m) }
func httpTargetAttr(t string) attribute.KeyValue   { return attribute.String(attrHTTPTarget, t) }
func httpRouteAttr(r string) attribute.KeyValue    { return attribute.String(attrHTTPRoute, r) }
func rpcMethodAttr(m string) attribute.KeyValue    { return attribute.String(attrRPCMethod, m) }
func grpcSystemAttr() attribute.KeyValue           { return attribute.String(attrRPCSystem, "grpc") }
func operationAttr(name string) attribute.KeyValue { return attribute.String(attrOperation, name) }

// tracer wraps the configured TracerProvider with a W3C TraceContext
// propagator. When WithTracer is unset, the underlying provider is a
// noop so callers can start spans unconditionally without nil checks
// or branch cost beyond an interface call.
type tracer struct {
	t       trace.Tracer
	prop    propagation.TextMapPropagator
	enabled bool
}

func newTracer(tp trace.TracerProvider) *tracer {
	if tp == nil {
		return &tracer{
			t:       noop.NewTracerProvider().Tracer(tracerName),
			prop:    propagation.TraceContext{},
			enabled: false,
		}
	}
	return &tracer{
		t:       tp.Tracer(tracerName),
		prop:    propagation.TraceContext{},
		enabled: true,
	}
}

// extractHTTP folds an inbound request's traceparent into ctx so child
// spans become continuations of the caller's trace.
func (tr *tracer) extractHTTP(ctx context.Context, h http.Header) context.Context {
	if !tr.enabled {
		return ctx
	}
	return tr.prop.Extract(ctx, propagation.HeaderCarrier(h))
}

// extractGRPC mirrors extractHTTP for gRPC incoming metadata.
func (tr *tracer) extractGRPC(ctx context.Context) context.Context {
	if !tr.enabled {
		return ctx
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return tr.prop.Extract(ctx, mdCarrier(md))
}

// injectHTTP writes the current span's traceparent onto outbound
// request headers before dispatch.
func (tr *tracer) injectHTTP(ctx context.Context, h http.Header) {
	if !tr.enabled {
		return
	}
	tr.prop.Inject(ctx, propagation.HeaderCarrier(h))
}

// injectGRPC writes the current span's traceparent onto outbound
// gRPC metadata before dispatch. Returns a context with the injected
// metadata appended to the outgoing pairs.
func (tr *tracer) injectGRPC(ctx context.Context) context.Context {
	if !tr.enabled {
		return ctx
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}
	tr.prop.Inject(ctx, mdCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

// bridgeTraceMetadata copies the inbound trace-propagation headers
// (traceForwardHeaders) onto the outgoing gRPC metadata, so a trace
// survives a proto dispatch even without an OTel TracerProvider — the
// gRPC counterpart of forwardOpenAPIHeaders. The inbound source is the
// originating HTTP request (GraphQL / REST ingress) or, for the gRPC
// ingress, the incoming gRPC metadata. injectGRPC, called right after,
// overwrites traceparent with the gateway's span context when a provider
// is wired. Returns ctx unchanged when no trace headers are present.
func bridgeTraceMetadata(ctx context.Context) context.Context {
	var get func(string) string
	if in := HTTPRequestFromContext(ctx); in != nil {
		get = in.Header.Get
	} else if md, ok := metadata.FromIncomingContext(ctx); ok {
		get = func(h string) string {
			if vs := md.Get(h); len(vs) > 0 {
				return vs[0]
			}
			return ""
		}
	} else {
		return ctx
	}
	var kvs []string
	for _, h := range traceForwardHeaders {
		if v := get(h); v != "" {
			kvs = append(kvs, h, v)
		}
	}
	if len(kvs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, kvs...)
}

// startIngressSpan opens a server-kind span for an ingress request.
// Returns the new ctx (carrying the span) and the span itself; caller
// must End() it. When tracing is disabled, returns the existing ctx
// and a non-recording span — bypasses otel's option machinery, which
// allocates a tracer.SpanStartConfig + an attribute slice copy even
// for the noop tracer.
func (tr *tracer) startIngressSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !tr.enabled {
		return ctx, noopSpanInst
	}
	return tr.t.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
}

// startDispatchSpan opens a client-kind span for an outbound dispatch
// (proto/openapi/graphql forwarder).
func (tr *tracer) startDispatchSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !tr.enabled {
		return ctx, noopSpanInst
	}
	return tr.t.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

// noopSpanInst is a process-wide instance of the otel/trace/noop span,
// returned by the tracer hot-paths when WithTracer is unset. Avoids
// boxing a fresh noop.Span into the trace.Span interface on every
// call.
var noopSpanInst trace.Span = noop.Span{}

// tracerCtxKey is the type used to stash the gateway's *tracer on
// per-request contexts. Dispatchers retrieve it via tracerFromContext
// so they can open child spans + inject traceparent on outbound calls
// without taking a Gateway pointer.
type tracerCtxKey struct{}

// withTracer returns ctx with tr installed. Called from the three
// ingress sites once the ingress span is open.
func withTracer(ctx context.Context, tr *tracer) context.Context {
	return context.WithValue(ctx, tracerCtxKey{}, tr)
}

// tracerFromContext returns the tracer installed on ctx, or a noop
// fallback. Always non-nil.
func tracerFromContext(ctx context.Context) *tracer {
	if v, ok := ctx.Value(tracerCtxKey{}).(*tracer); ok && v != nil {
		return v
	}
	return noopTracer
}

// noopTracer is the fallback tracer used by dispatchers that run
// outside an ingress (background reconcilers, tests calling Dispatch
// directly). It carries an enabled=false flag so inject helpers
// short-circuit cheaply.
var noopTracer = newTracer(nil)

// mdCarrier adapts metadata.MD to TextMapCarrier. gRPC metadata keys
// are lowercased on the wire; the W3C TraceContext propagator emits
// "traceparent" / "tracestate" which already conform.
type mdCarrier metadata.MD

func (c mdCarrier) Get(k string) string {
	v := metadata.MD(c).Get(k)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (c mdCarrier) Set(k, v string) { metadata.MD(c).Set(k, v) }

func (c mdCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}
