package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// fakeGreeterServer implements the unary Hello and the server-streaming
// Greetings. Echo (bidi) is left at the unimplemented embed.
type fakeGreeterServer struct {
	greeterv1.UnimplementedGreeterServiceServer

	helloCalls atomic.Int32
	lastReq    atomic.Pointer[greeterv1.HelloRequest]
	helloFn    func(context.Context, *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error)

	// Greetings handler: if set, called instead of the default
	// behavior. Receives the filter and the server stream to write responses.
	greetingsFn func(context.Context, *greeterv1.GreetingsFilter, grpc.ServerStreamingServer[greeterv1.Greeting]) error
}

func (f *fakeGreeterServer) Hello(ctx context.Context, req *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
	f.helloCalls.Add(1)
	f.lastReq.Store(req)
	if f.helloFn != nil {
		return f.helloFn(ctx, req)
	}
	return &greeterv1.HelloResponse{Greeting: "hello " + req.GetName()}, nil
}

func (f *fakeGreeterServer) Greetings(req *greeterv1.GreetingsFilter, stream grpc.ServerStreamingServer[greeterv1.Greeting]) error {
	if f.greetingsFn != nil {
		return f.greetingsFn(stream.Context(), req, stream)
	}
	// Default: send one greeting and close.
	return stream.Send(&greeterv1.Greeting{
		Greeting: "hello " + req.GetName(),
		ForName:  req.GetName(),
	})
}

type grpcE2EFixture struct {
	gw       *Gateway
	server   *httptest.Server
	greeter  *fakeGreeterServer
	grpcAddr string
}

// newGRPCE2EFixture spins an in-process grpc.Server on
// 127.0.0.1:<ephemeral>, registers greeter via AddProtoDescriptor
// pointing at it, and exposes gw.Handler() over httptest. Mirrors
// the openapi_test.go fixture shape.
func newGRPCE2EFixture(t *testing.T) *grpcE2EFixture {
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

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(lis.Addr().String()),
		As("greeter"),
	); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	return &grpcE2EFixture{
		gw:       gw,
		server:   srv,
		greeter:  greeter,
		grpcAddr: lis.Addr().String(),
	}
}

func (f *grpcE2EFixture) postGraphQL(t *testing.T, query string) (status int, body string) {
	t.Helper()
	resp, err := http.Post(f.server.URL+"/graphql", "application/json", strings.NewReader(query))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	rr := httptest.NewRecorder()
	rr.Code = resp.StatusCode
	if _, err := rr.Body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return rr.Code, rr.Body.String()
}

func TestGRPCE2E_UnaryHappyPath(t *testing.T) {
	f := newGRPCE2EFixture(t)

	status, body := f.postGraphQL(t, `{"query":"{ greeter { hello(name:\"world\") { greeting } } }"}`)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}

	var out struct {
		Data struct {
			Greeter struct {
				Hello map[string]any `json:"hello"`
			} `json:"greeter"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v: %s", err, body)
	}
	if len(out.Errors) > 0 {
		t.Fatalf("graphql errors: %v", out.Errors)
	}
	if got := out.Data.Greeter.Hello["greeting"]; got != "hello world" {
		t.Fatalf("greeting=%v want %q", got, "hello world")
	}
	if got := f.greeter.helloCalls.Load(); got != 1 {
		t.Fatalf("Hello called %d times, want 1", got)
	}
	if got := f.greeter.lastReq.Load().GetName(); got != "world" {
		t.Fatalf("backend got name=%q want world", got)
	}
}

func TestGRPCE2E_LatestNamespaceFlat(t *testing.T) {
	// The schema also exposes the latest version's RPCs flat under the
	// namespace alongside the v1 sub-object. Confirm both shapes
	// dispatch to the same gRPC call.
	f := newGRPCE2EFixture(t)

	_, body := f.postGraphQL(t, `{"query":"{ greeter { v1 { hello(name:\"v1\") { greeting } } } }"}`)
	var out struct {
		Data struct {
			Greeter struct {
				V1 struct {
					Hello map[string]any `json:"hello"`
				} `json:"v1"`
			} `json:"greeter"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v: %s", err, body)
	}
	if got := out.Data.Greeter.V1.Hello["greeting"]; got != "hello v1" {
		t.Fatalf("v1 greeting=%v want %q", got, "hello v1")
	}
	if got := f.greeter.helloCalls.Load(); got != 1 {
		t.Fatalf("Hello called %d times, want 1 (v1 sub-object reuses the same dispatch)", got)
	}
}

func TestGRPCE2E_BackendErrorSurfaces(t *testing.T) {
	f := newGRPCE2EFixture(t)
	f.greeter.helloFn = func(ctx context.Context, req *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
		return nil, errors.New("simulated backend failure")
	}

	_, body := f.postGraphQL(t, `{"query":"{ greeter { hello(name:\"x\") { greeting } } }"}`)
	if !strings.Contains(body, "errors") {
		t.Fatalf("expected graphql errors block, got %s", body)
	}
	if !strings.Contains(body, "simulated backend failure") {
		t.Fatalf("backend error not surfaced, got %s", body)
	}
}

// TestGRPCE2E_InjectHeader_StampsOutgoingMetadata confirms an
// InjectHeader registered before AddProtoDescriptor reaches the
// upstream gRPC server as outgoing metadata. The fakeGreeterServer
// reads metadata.FromIncomingContext on the inbound side.
func TestGRPCE2E_InjectHeader_StampsOutgoingMetadata(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{}
	var seenSourceIP atomic.Pointer[string]
	var seenCaller atomic.Pointer[string]
	greeter.helloFn = func(ctx context.Context, req *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if v := md.Get("x-source-ip"); len(v) > 0 {
			s := v[0]
			seenSourceIP.Store(&s)
		}
		if v := md.Get("x-caller"); len(v) > 0 {
			s := v[0]
			seenCaller.Store(&s)
		}
		return &greeterv1.HelloResponse{Greeting: "hi"}, nil
	}
	grpcSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(grpcSrv, greeter)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)

	gw.Use(InjectHeader("X-Source-IP", func(_ context.Context, _ *string) (string, error) {
		return "10.0.0.1", nil
	}))
	gw.Use(InjectHeader("X-Caller", func(_ context.Context, current *string) (string, error) {
		if current == nil {
			return "anonymous", nil
		}
		return "verified:" + *current, nil
	}, Hide(false)))

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"), To(lis.Addr().String()), As("greeter")); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/graphql",
		strings.NewReader(`{"query":"{ greeter { hello(name:\"world\") { greeting } } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Caller", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if got := seenSourceIP.Load(); got == nil || *got != "10.0.0.1" {
		t.Fatalf("upstream X-Source-IP=%v, want 10.0.0.1", got)
	}
	if got := seenCaller.Load(); got == nil || *got != "verified:alice" {
		t.Fatalf("upstream X-Caller=%v, want verified:alice", got)
	}
}

func TestGRPCE2E_NoLiveReplicasAfterRemoval(t *testing.T) {
	// Drop all replicas and confirm the dispatch error mentions the
	// (ns/ver). This covers the error path in pool.pickReplica.
	f := newGRPCE2EFixture(t)

	// Drain replicas without deleting the pool — we want the
	// dispatch closure to see "no live replicas" rather than a
	// missing pool error. atomic Pointer to empty slice mirrors how
	// removeReplicasByOwner stores its result.
	f.gw.mu.Lock()
	empty := []*replica{}
	for _, slot := range f.gw.slots {
		if slot.kind == slotKindProto && slot.proto != nil {
			slot.proto.replicas.Store(&empty)
		}
	}
	f.gw.mu.Unlock()

	_, body := f.postGraphQL(t, `{"query":"{ greeter { hello(name:\"x\") { greeting } } }"}`)
	if !strings.Contains(body, "no live replicas") {
		t.Fatalf("expected 'no live replicas' error, got %s", body)
	}
}
