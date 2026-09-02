package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// requestMetricsRecorder captures RecordRequest observations so the
// ingress-wiring tests can assert one observation per inbound request.
// Embeds noopMetrics for the rest of the surface.
type requestMetricsRecorder struct {
	noopMetrics
	mu      sync.Mutex
	entries []requestMetricsEntry
}

type requestMetricsEntry struct {
	ingress string
	total   time.Duration
	self    time.Duration
}

func (r *requestMetricsRecorder) RecordRequest(ingress string, total, self time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, requestMetricsEntry{ingress: ingress, total: total, self: self})
}

func (r *requestMetricsRecorder) snapshot() []requestMetricsEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]requestMetricsEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

// requestMetricsGW spins a backend greeter + a gateway pointed at it
// with the supplied recorder installed via WithMetrics. Returns the
// gateway and the backend listener address so the caller can mount
// whichever ingress handler it's exercising.
func requestMetricsGW(t *testing.T, rec *requestMetricsRecorder) *Gateway {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{}
	grpcSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(grpcSrv, greeter)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	gw := New(WithMetrics(rec), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(lis.Addr().String()),
		As("greeter"),
	); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}
	return gw
}

// gw.Handler() records ingress="graphql" with self ≤ total. WebSocket
// upgrades are intentionally skipped (long-lived streams), so this
// covers the regular query path.
func TestRequestMetrics_GraphQLIngress(t *testing.T) {
	rec := &requestMetricsRecorder{}
	gw := requestMetricsGW(t, rec)
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/graphql", "application/json",
		strings.NewReader(`{"query":"{ greeter { hello(name:\"world\") { greeting } } }"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("RecordRequest called %d times, want 1: %+v", len(got), got)
	}
	e := got[0]
	if e.ingress != "graphql" {
		t.Fatalf("ingress=%q want=graphql", e.ingress)
	}
	if e.total <= 0 {
		t.Fatalf("total=%v not advancing", e.total)
	}
	if e.self < 0 || e.self > e.total {
		t.Fatalf("self=%v out of [0, total=%v]", e.self, e.total)
	}
}

// IngressHandler unary path records ingress="http". SSE subscriptions
// are skipped — see serveIngress.
func TestRequestMetrics_HTTPIngress(t *testing.T) {
	rec := &requestMetricsRecorder{}
	gw := requestMetricsGW(t, rec)
	srv := httptest.NewServer(gw.IngressHandler())
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/greeter.v1.GreeterService/Hello",
		"application/json", strings.NewReader(`{"name":"world"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("RecordRequest called %d times, want 1: %+v", len(got), got)
	}
	e := got[0]
	if e.ingress != "http" {
		t.Fatalf("ingress=%q want=http", e.ingress)
	}
	if e.total <= 0 {
		t.Fatalf("total=%v not advancing", e.total)
	}
	if e.self < 0 || e.self > e.total {
		t.Fatalf("self=%v out of [0, total=%v]", e.self, e.total)
	}
}

// GRPCUnknownHandler unary path records ingress="grpc". Server-
// streaming subscriptions are skipped — see serveGRPCUnknown.
func TestRequestMetrics_GRPCIngress(t *testing.T) {
	rec := &requestMetricsRecorder{}
	gw := requestMetricsGW(t, rec)

	feLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("frontend listen: %v", err)
	}
	feSrv := grpc.NewServer(grpc.UnknownServiceHandler(gw.GRPCUnknownHandler()))
	go func() { _ = feSrv.Serve(feLis) }()
	t.Cleanup(feSrv.Stop)

	conn, err := grpc.NewClient(feLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cli := greeterv1.NewGreeterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Hello(ctx, &greeterv1.HelloRequest{Name: "world"}); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("RecordRequest called %d times, want 1: %+v", len(got), got)
	}
	e := got[0]
	if e.ingress != "grpc" {
		t.Fatalf("ingress=%q want=grpc", e.ingress)
	}
	if e.total <= 0 {
		t.Fatalf("total=%v not advancing", e.total)
	}
	if e.self < 0 || e.self > e.total {
		t.Fatalf("self=%v out of [0, total=%v]", e.self, e.total)
	}
}
