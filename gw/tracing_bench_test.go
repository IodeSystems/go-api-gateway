// Tracing overhead microbench. Compares the GraphQL ingress hot path
// with WithTracer unset (noop tracer) vs with WithTracer set against an
// in-memory SDK provider — the worst case for SDK overhead since every
// span is sampled and synchronously dispatched to the exporter.
//
// Real deployments use a ratio-based sampler + a batching exporter, so
// the SetSyncer numbers here over-estimate the in-process portion of
// the cost. They underestimate network egress (exporter wire time
// happens off the hot path).
//
// Baseline (Ryzen 9 3900X, loopback backend, -benchtime=2s):
//
//   BenchmarkProtoSchemaExec_TracingOff     ~430 µs/op   972 allocs/op  (baseline)
//   BenchmarkProtoSchemaExec_TracingOn      see numbers below           (with WithTracer + sync exporter)
//
// The delta is the gateway's tracing surface — two spans per request
// (one ingress + one dispatch), one extract + one inject, attribute
// marshal + queue overhead through the SDK.
package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// newProtoHTTPBenchGateway is a bench-shaped fixture that mounts the
// gateway's GraphQL handler on an httptest server, so the bench
// exercises the full ingress path (extract → ingress span → dispatch
// span → inject → upstream invoke) end-to-end.
func newProtoHTTPBenchGateway(b *testing.B, opts ...Option) (*httptest.Server, func()) {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{}
	grpcSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(grpcSrv, greeter)
	go func() { _ = grpcSrv.Serve(lis) }()

	all := append([]Option{
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("ignored")),
	}, opts...)
	gw := New(all...)
	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(b, "greeter.proto"),
		To(lis.Addr().String()), As("greeter"),
	); err != nil {
		grpcSrv.Stop()
		gw.Close()
		b.Fatalf("AddProtoBytes: %v", err)
	}
	srv := httptest.NewServer(gw.Handler())
	cleanup := func() {
		srv.Close()
		gw.Close()
		grpcSrv.Stop()
	}
	return srv, cleanup
}

func runGraphQLBench(b *testing.B, srv *httptest.Server) {
	body := `{"query":"{ greeter { hello(name:\"x\") { greeting } } }"}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(srv.URL, "application/json", strings.NewReader(body))
		if err != nil {
			b.Fatalf("post: %v", err)
		}
		_ = resp.Body.Close()
	}
}

// BenchmarkTracing_GraphQLIngress_Off is the baseline — no
// TracerProvider configured. The noop tracer's enabled=false flag
// short-circuits propagator extract / inject; spans go through the
// otel noop path which the compiler can inline.
func BenchmarkTracing_GraphQLIngress_Off(b *testing.B) {
	srv, cleanup := newProtoHTTPBenchGateway(b)
	defer cleanup()
	runGraphQLBench(b, srv)
}

// BenchmarkTracing_GraphQLIngress_On installs the OTel SDK with a
// sync InMemoryExporter — worst-case for in-process overhead since
// every span flushes synchronously. Real prod uses a batcher.
func BenchmarkTracing_GraphQLIngress_On(b *testing.B) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	srv, cleanup := newProtoHTTPBenchGateway(b, WithTracer(tp))
	defer cleanup()
	runGraphQLBench(b, srv)
}

// BenchmarkTracing_GraphQLIngress_OnBatcher mirrors the prod-realistic
// case: spans land in the batcher's queue and the exporter drains them
// asynchronously. Per-request cost is the queue enqueue, not the
// exporter wire time.
func BenchmarkTracing_GraphQLIngress_OnBatcher(b *testing.B) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
	b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	srv, cleanup := newProtoHTTPBenchGateway(b, WithTracer(tp))
	defer cleanup()
	runGraphQLBench(b, srv)
}
