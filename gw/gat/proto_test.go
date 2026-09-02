package gat_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
	"github.com/iodesystems/gwag/v2/gw/gat"
)

// minimal greeter.proto for the gat ingest test. Self-contained so the
// test doesn't depend on a generated FileDescriptor anywhere — gat
// compiles the source itself via protocompile.
//
// The server side uses the existing generated greeterv1 stubs purely
// as a convenient way to satisfy grpc.Server; gat dispatches via the
// dynamic descriptor it compiled and never sees those generated types.
const greeterProtoSrc = `syntax = "proto3";

package greeter.v1;

service GreeterService {
  rpc Hello(HelloRequest) returns (HelloResponse);
}

message HelloRequest {
  string name = 1;
}

message HelloResponse {
  string greeting = 1;
}
`

type protoTestGreeter struct {
	greeterv1.UnimplementedGreeterServiceServer
}

func (s *protoTestGreeter) Hello(ctx context.Context, req *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
	return &greeterv1.HelloResponse{Greeting: "hello " + req.GetName()}, nil
}

func startGreeterUpstream(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(srv, &protoTestGreeter{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestProtoSource_UnaryThroughGraphQL(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrc), nil, target)
	if err != nil {
		t.Fatalf("ProtoSource: %v", err)
	}
	if len(regs) != 1 {
		t.Fatalf("expected 1 registration, got %d", len(regs))
	}
	g, err := gat.New(regs...)
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}

	srv := httptest.NewServer(g.Handler())
	t.Cleanup(srv.Close)

	resp := mustGraphQL(t, srv.URL,
		`{ greeter { v1 { hello(name: "alice") { greeting } } } }`)
	hello := digPath(resp, "data", "greeter", "v1", "hello")
	if hello == nil {
		t.Fatalf("expected hello payload, got %v", resp)
	}
	m, ok := hello.(map[string]any)
	if !ok {
		t.Fatalf("hello not a map: %T %v", hello, hello)
	}
	if got, want := m["greeting"], "hello alice"; got != want {
		t.Errorf("greeting: got %q, want %q", got, want)
	}
}

func TestProtoSource_UnreachableTargetSurfacesError(t *testing.T) {
	// Pick an arbitrary unused port — :1 always rejects locally.
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrc), nil, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("ProtoSource: %v (dial should be lazy)", err)
	}
	g, err := gat.New(regs...)
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}

	srv := httptest.NewServer(g.Handler())
	t.Cleanup(srv.Close)

	resp := mustGraphQL(t, srv.URL,
		`{ greeter { v1 { hello(name: "alice") { greeting } } } }`)
	errs, _ := resp["errors"].([]any)
	if len(errs) == 0 {
		t.Fatalf("expected GraphQL errors for unreachable upstream, got %v", resp)
	}
}

func TestProtoSource_NamespaceAndVersionFromPackage(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrc), nil, target)
	if err != nil {
		t.Fatalf("ProtoSource: %v", err)
	}
	if got := regs[0].Service.Namespace; got != "greeter" {
		t.Errorf("namespace: got %q, want %q (proto package 'greeter.v1' splits on trailing vN)", got, "greeter")
	}
	if got := regs[0].Service.Version; got != "v1" {
		t.Errorf("version: got %q, want %q", got, "v1")
	}
}

// silence unused stdlib import lint when only one of net/http types is referenced.
var _ = http.MethodGet

func TestSplitProtoPackage(t *testing.T) {
	cases := []struct {
		pkg     string
		wantNS  string
		wantVer string
	}{
		{"", "default", "v1"},
		{"greeter", "greeter", "v1"},
		{"greeter.v1", "greeter", "v1"},
		{"greeter.v2", "greeter", "v2"},
		{"a.b.c.v3", "a_b_c", "v3"},
		{"a.b.c", "a_b_c", "v1"},
		{"v1", "v1", "v1"}, // single-segment "v1" is the namespace, not a stripped suffix
	}
	for _, tc := range cases {
		gotNS, gotVer := gat.ExportSplitProtoPackage(tc.pkg)
		if gotNS != tc.wantNS || gotVer != tc.wantVer {
			t.Errorf("splitProtoPackage(%q) = (%q, %q), want (%q, %q)",
				tc.pkg, gotNS, gotVer, tc.wantNS, tc.wantVer)
		}
	}
}
