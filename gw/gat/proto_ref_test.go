package gat_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

// greeterProtoSrcWithRef carries `@ref` source-of-truth markers as
// leading comments on the rpc and the response message. ProtoSource
// compiles with SourceInfoStandard so the comments survive into the IR,
// and the gateway re-emits them into the served GraphQL SDL.
const greeterProtoSrcWithRef = `syntax = "proto3";

package greeter.v1;

service GreeterService {
  // Greet a caller by name.
  //
  // @ref server/greeter.go:Hello
  rpc Hello(HelloRequest) returns (HelloResponse);
}

message HelloRequest {
  string name = 1;
}

// @ref server/greeter.go:HelloResponse
message HelloResponse {
  string greeting = 1;
}
`

func TestProtoSource_RefMarkerInSDL(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrcWithRef), nil, target)
	if err != nil {
		t.Fatalf("ProtoSource: %v", err)
	}
	g, err := gat.New(regs...)
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}
	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, "/"); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/schema/graphql")
	if err != nil {
		t.Fatalf("GET schema: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	sdl := string(body)

	for _, want := range []string{
		"@ref server/greeter.go:Hello",
		"@ref server/greeter.go:HelloResponse",
	} {
		if !strings.Contains(sdl, want) {
			t.Errorf("served SDL missing %q\n--- SDL ---\n%s", want, sdl)
		}
	}
	// The marker must be lifted out of the raw description, not left
	// inline as a duplicate alongside the re-emitted one.
	if strings.Contains(sdl, "@ref @ref") {
		t.Errorf("marker doubled in SDL\n--- SDL ---\n%s", sdl)
	}
}

// greeterProtoSrcWithGql carries `@gql` annotation directives as leading
// comments. ProtoSource ingests them and the gateway re-emits them as
// real GraphQL directives (with synthesized declarations) in the SDL.
const greeterProtoSrcWithGql = `syntax = "proto3";

package greeter.v1;

service GreeterService {
  // Greet a caller by name.
  //
  // @gql hasRole(role: "ADMIN")
  // @gql audited
  rpc Hello(HelloRequest) returns (HelloResponse);
}

message HelloRequest {
  string name = 1;
}

message HelloResponse {
  string greeting = 1;
}
`

func TestProtoSource_AnnotationsInSDL(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrcWithGql), nil, target)
	if err != nil {
		t.Fatalf("ProtoSource: %v", err)
	}
	g, err := gat.New(regs...)
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}
	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, "/"); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/schema/graphql")
	if err != nil {
		t.Fatalf("GET schema: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	sdl := string(body)

	for _, want := range []string{
		`hello(name: String): Greeter_V1_HelloResponse @audited @hasRole(role: "ADMIN")`,
		"directive @hasRole(role: String) on FIELD_DEFINITION",
		"directive @audited on FIELD_DEFINITION",
	} {
		if !strings.Contains(sdl, want) {
			t.Errorf("served SDL missing %q\n--- SDL ---\n%s", want, sdl)
		}
	}
}
