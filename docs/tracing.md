# Distributed tracing (OpenTelemetry)

gwag emits OpenTelemetry spans for every ingress request and every
upstream dispatch when `WithTracer` is set. Inbound `traceparent`
headers join the caller's trace; outbound calls propagate
`traceparent` on HTTP headers and gRPC metadata so downstream services
see one continuous trace from the user agent through every hop.

```
[client] ──traceparent──▶ [gwag]
                            ├─ gateway.graphql / .http / .grpc                  (server span)
                            │   └─ gateway.dispatch.proto / .openapi / .graphql (client span)
                            │       └─ ──traceparent──▶ upstream service
                            └─ ...
```

When `WithTracer` is unset, the gateway wires a no-op tracer; spans are
not created, attributes are not allocated, and per-request cost stays
near zero. There is no separate "disable tracing" knob — omit the
option.

## Wiring

```go
import (
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/sdk/resource"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

    gateway "github.com/iodesystems/gwag/v2/gw"
)

exp, err := otlptracegrpc.New(ctx,
    otlptracegrpc.WithEndpoint("otel-collector:4317"),
    otlptracegrpc.WithInsecure(),
)
if err != nil { panic(err) }

tp := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(exp),
    sdktrace.WithResource(resource.NewWithAttributes(
        semconv.SchemaURL,
        semconv.ServiceName("gwag"),
    )),
)

gw := gateway.New(
    gateway.WithTracer(tp),
    // ...other options
)
```

Any `trace.TracerProvider` works — the SDK at
`go.opentelemetry.io/otel/sdk` is the canonical choice. Common collectors
and backends are wired the same way:

| Backend | Exporter import |
|---|---|
| Jaeger | `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` (Jaeger speaks OTLP since 1.35) |
| Honeycomb | `otlptracegrpc` with endpoint `api.honeycomb.io:443`, header `x-honeycomb-team: <key>` |
| OTel Collector | `otlptracegrpc` or `otlptracehttp` against the collector's receiver address |
| stdout (dev) | `go.opentelemetry.io/otel/exporters/stdout/stdouttrace` |

Tracer shutdown belongs to the operator — flush + close `tp` on
graceful exit so buffered spans reach the exporter:

```go
defer func() {
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = tp.Shutdown(shutdownCtx)
}()
```

## Span reference

### Server-kind (ingress)

One per request. Lifetime spans the full ingress handler — from the
moment the gateway extracts inbound headers to the moment the response
is written. WebSocket subscriptions are excluded (the per-request
histograms also skip them); the subscription's open-side dispatch
still emits its own span under the request that initiated it.

| Span name | Ingress shape |
|---|---|
| `gateway.graphql` | GraphQL POST + GraphiQL — `gw.Handler()` |
| `gateway.http` | HTTP/JSON proto-style + OpenAPI passthrough — `gw.IngressHandler()` |
| `gateway.http.subscription` | SSE subscription over the HTTP ingress |
| `gateway.grpc` | gRPC unary unknown-service handler — `gw.GRPCUnknownHandler()` |
| `gateway.grpc.subscription` | gRPC server-streaming unknown-service handler |

Attributes:

| Key | Value |
|---|---|
| `gateway.ingress` | `graphql` / `http` / `grpc` |
| `gateway.namespace` | namespace of the operation (HTTP / gRPC only — GraphQL doesn't know at request start) |
| `gateway.version` | version string (`unstable` / `v1` / etc.) |
| `gateway.method` | flat operation name |
| `http.method` | `POST` / `GET` (HTTP + GraphQL) |
| `http.target` | request URL path (HTTP + GraphQL) |
| `http.route` | template path with `{placeholders}` (HTTP only) |
| `rpc.system` | `grpc` (gRPC only) |
| `rpc.method` | full gRPC method path (gRPC only) |

### Client-kind (dispatch)

One per upstream call, nested under the ingress span. Lifetime spans
the dispatcher entry — including backpressure wait, replica pick,
header injection, and the upstream call — until the response is
decoded or an error is returned.

| Span name | Dispatcher |
|---|---|
| `gateway.dispatch.proto` | Proto unary (gRPC outbound) |
| `gateway.dispatch.openapi` | OpenAPI passthrough (HTTP outbound) |
| `gateway.dispatch.graphql` | Downstream GraphQL forwarder (HTTP outbound) |
| `gateway.dispatch.internal` | In-process internal-proto handlers (no outbound) |
| `gateway.dispatch.proto.subscription` | Proto server-streaming open-side (gRPC outbound) |

Attributes:

| Key | Value |
|---|---|
| `gateway.namespace`, `gateway.version`, `gateway.method` | same shape as ingress |
| `http.method`, `http.route` | OpenAPI only |
| `rpc.system`, `rpc.method` | proto + proto subscription |

### Propagation

`traceparent` (W3C TraceContext) is the only propagator wired —
gwag does not currently support B3 / Jaeger native / OT propagation
formats. The propagator is symmetric: every inbound shape extracts,
every outbound shape injects.

| Direction | Carrier | Where |
|---|---|---|
| Inbound HTTP / GraphQL | request headers | `r.Header` |
| Inbound gRPC | incoming metadata | `metadata.FromIncomingContext` |
| Outbound HTTP (OpenAPI) | request headers | injected after `forwardOpenAPIHeaders` |
| Outbound HTTP (GraphQL forwarder) | request headers | same point in `dispatchGraphQL` |
| Outbound gRPC (proto unary) | outgoing metadata | injected before `r.conn.Invoke` |
| Outbound gRPC (proto subscription) | outgoing metadata | injected before `r.conn.NewStream` |

A client that already started a trace upstream of the gateway sees one
continuous trace. A client that did not is the trace's root; the
gateway opens a fresh root span.

## What gwag does NOT instrument

- **Background reconcilers / cluster watch loops / NATS housekeeping.**
  Background work uses `noopTracer` so trace volume tracks request
  volume rather than cluster size. Cluster-reconciler spans are a
  followup if "I can't tell where a registration is stuck" surfaces
  from an operator.
- **Per-event subscription frames.** A subscription's open-side gets a
  dispatch span; per-frame spans would balloon trace cardinality
  without payoff. Operators who need per-frame visibility add their
  own instrumentation in the downstream service.
- **Internal admin operations (huma router).** Admin endpoints
  dogfood the OpenAPI ingest path — they go through
  `gateway.dispatch.internal` like any in-process handler. There's no
  separate "admin span" namespace.
- **Plan-cache / schema-rebuild internals.** These are gateway-state
  transitions, not per-request work; they don't take a span.

## Perf overhead

When `WithTracer` is unset, the gateway wires a no-op tracer at
construction and the per-request hot path stays branch-free
(`tr.enabled` short-circuits the propagator extract + inject calls).
The cost is one extra interface call per ingress + per dispatch — too
small to measure outside microbench noise.

Measured overhead from `BenchmarkTracing_GraphQLIngress_*` (Ryzen 9
3900X, 24 logical cores, loopback gRPC backend, `-benchtime=3s
-count=3`):

| Config | ns/op (range) | B/op | allocs/op |
|---|---|---|---|
| `WithTracer` unset (noop) | 386k–424k | ~37.7 KB | 359 |
| `WithTracer` on, sync exporter | 373k–391k | ~44.4 KB | 380 |
| `WithTracer` on, batching exporter | 377k–382k | ~44.5 KB | 376 |

Read the table for **+21 allocs and +6.7 KB per request when tracing
is enabled** — the two spans (ingress + dispatch), attribute marshal,
and propagator carrier work. Wall-time delta is below the
HTTP-loopback noise floor on this host (the sync exporter run
overlaps the noop baseline). The batching exporter is the production
shape: spans land in a queue, the exporter drains async, so the wire
time stays off the request path.

These numbers do **not** include the operator-side cost of the
sampler + exporter you choose. The SDK's batch processor adds one
mutex acquire + one slice append per span; OTLP/gRPC exporters take
~1 KB/span over the wire. Use `TraceIDRatioBased(0.01)` (or your
sampling rate) on the `TracerProvider` for production volumes — the
sampler runs before the propagator extract so unsampled requests skip
the per-span allocs.

Reproduce:

```
go test ./gw/ -bench=BenchmarkTracing_GraphQLIngress -benchmem -run=^$ -benchtime=3s -count=3
```

## Notes

- **Co-existing with an upstream OpenTelemetry interceptor.** Wrap
  `gw.GRPCUnknownHandler()` in
  `grpc.UnaryServerInterceptor(otelgrpc.UnaryServerInterceptor())`;
  the interceptor's span becomes the parent of the gateway's
  `gateway.grpc` span via propagator extract.
- **`gateway.namespace` is empty on `gateway.graphql`.** The GraphQL
  parser hasn't run when the ingress span opens; the per-dispatch
  span carries the namespace.
- **Custom span attributes.** Fish the ingress span with
  `trace.SpanFromContext(ctx)` from middleware mounted around
  `gw.Handler()`. No gwag-side hook today.
- **Sampling.** Owned by the `TracerProvider`. Use
  `sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.01))` for
  production rates.
