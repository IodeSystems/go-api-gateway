package gat_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

func TestRegisterHTTP_MountsAllFour(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, err := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrc), nil, target)
	if err != nil {
		t.Fatalf("ProtoSource: %v", err)
	}
	g, err := gat.New(regs...)
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}

	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, "/api"); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	t.Run("graphql", func(t *testing.T) {
		resp := mustGraphQL(t, srv.URL+"/api/graphql",
			`{ greeter { v1 { hello(name: "alice") { greeting } } } }`)
		hello := digPath(resp, "data", "greeter", "v1", "hello")
		if hello == nil {
			t.Fatalf("missing hello: %v", resp)
		}
	})

	t.Run("schema_graphql_sdl", func(t *testing.T) {
		body := mustGetBody(t, srv.URL+"/api/schema/graphql")
		if !strings.Contains(string(body), "type Query") {
			t.Errorf("expected SDL, got %s", body)
		}
	})

	t.Run("schema_graphql_introspection", func(t *testing.T) {
		body := mustGetBody(t, srv.URL+"/api/schema/graphql?format=json")
		// Introspection has a __schema root.
		if !strings.Contains(string(body), "__schema") {
			t.Errorf("expected introspection JSON, got %s", body)
		}
	})

	// graphql-codegen introspects with a POST query. The schema URL must
	// answer it (delegating to the GraphQL handler) so codegen can point at
	// the documented schema endpoint instead of 405-ing on it.
	t.Run("schema_graphql_introspection_post", func(t *testing.T) {
		resp := mustGraphQL(t, srv.URL+"/api/schema/graphql",
			`{ __schema { queryType { name } } }`)
		if name := digPath(resp, "data", "__schema", "queryType", "name"); name == nil {
			t.Fatalf("POST introspection to schema URL returned no queryType: %v", resp)
		}
	})

	t.Run("schema_proto", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/schema/proto")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status: %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != "application/protobuf" {
			t.Errorf("content-type: got %q", got)
		}
	})

	t.Run("schema_openapi", func(t *testing.T) {
		body := mustGetBody(t, srv.URL+"/api/schema/openapi")
		if !strings.Contains(string(body), "openapi") {
			t.Errorf("expected openapi doc, got %s", body[:min(200, len(body))])
		}
	})
}

func TestRegisterHTTP_RequiresBuiltGateway(t *testing.T) {
	g, err := gat.New() // empty — not built
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}
	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, "/api"); err == nil {
		t.Fatalf("expected error for unbuilt gateway")
	}
}

func TestRegisterHTTP_PrefixTrailingSlash(t *testing.T) {
	target := startGreeterUpstream(t)
	regs, _ := gat.ProtoSource("greeter.proto", []byte(greeterProtoSrc), nil, target)
	g, _ := gat.New(regs...)

	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, "/api/"); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Trailing slash should be normalized — the endpoint is at /api/graphql.
	resp := mustGraphQL(t, srv.URL+"/api/graphql",
		`{ greeter { v1 { hello(name: "x") { greeting } } } }`)
	if digPath(resp, "data", "greeter", "v1", "hello") == nil {
		t.Fatalf("missing hello: %v", resp)
	}
}

func mustGetBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	return b
}
