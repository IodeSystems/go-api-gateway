package main

import (
	"reflect"
	"strings"
	"testing"

	gateway "github.com/iodesystems/gwag/v2/gw"
)

func TestParseAllowTier(t *testing.T) {
	t.Run("default-all", func(t *testing.T) {
		got, err := parseAllowTier("unstable,stable,vN")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		want := []string{"unstable", "stable", "vN"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("vN-only", func(t *testing.T) {
		got, err := parseAllowTier("vN")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !reflect.DeepEqual(got, []string{"vN"}) {
			t.Errorf("got %v, want [vN]", got)
		}
	})

	t.Run("whitespace-tolerated", func(t *testing.T) {
		got, err := parseAllowTier(" stable , vN ")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !reflect.DeepEqual(got, []string{"stable", "vN"}) {
			t.Errorf("got %v, want [stable vN]", got)
		}
	})

	t.Run("rejects-unknown", func(t *testing.T) {
		_, err := parseAllowTier("vN,prod")
		if err == nil {
			t.Fatalf("expected error for unknown tier")
		}
		if !strings.Contains(err.Error(), "unknown tier") {
			t.Errorf("error %q missing 'unknown tier'", err.Error())
		}
	})

	t.Run("rejects-empty", func(t *testing.T) {
		if _, err := parseAllowTier(""); err == nil {
			t.Errorf("expected error for empty")
		}
		if _, err := parseAllowTier(",,,"); err == nil {
			t.Errorf("expected error for all-empty entries")
		}
	})
}

func TestParseMCPUpstream(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		ns, tr, target, err := parseMCPUpstream("weather:http:https://mcp.example.com/mcp")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if ns != "weather" || tr != gateway.MCPHTTP || target != "https://mcp.example.com/mcp" {
			t.Errorf("got (%q, %q, %q)", ns, tr, target)
		}
	})

	t.Run("stdio-command-with-spaces", func(t *testing.T) {
		ns, tr, target, err := parseMCPUpstream("files:stdio:npx -y @modelcontextprotocol/server-filesystem /tmp")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if ns != "files" || tr != gateway.MCPStdio {
			t.Errorf("ns/transport wrong: %q %q", ns, tr)
		}
		if target != "npx -y @modelcontextprotocol/server-filesystem /tmp" {
			t.Errorf("target = %q", target)
		}
	})

	t.Run("rejects-bad-shape", func(t *testing.T) {
		if _, _, _, err := parseMCPUpstream("nocolons"); err == nil {
			t.Error("expected error for missing fields")
		}
		if _, _, _, err := parseMCPUpstream(":http:url"); err == nil {
			t.Error("expected error for empty namespace")
		}
	})

	t.Run("rejects-unknown-transport", func(t *testing.T) {
		_, _, _, err := parseMCPUpstream("x:grpc:host:1")
		if err == nil || !strings.Contains(err.Error(), "unknown transport") {
			t.Errorf("expected unknown-transport error, got %v", err)
		}
	})
}

func TestSplitNSURL(t *testing.T) {
	cases := []struct{ in, ns, url string }{
		{"pets@http://pets-svc:8080", "pets", "http://pets-svc:8080"},
		{"http://pets-svc:8080", "", "http://pets-svc:8080"},
		{"https://reviews/graphql", "", "https://reviews/graphql"},
		{"reviews@https://reviews/graphql", "reviews", "https://reviews/graphql"},
		// '@' after the scheme is userinfo, not a namespace separator.
		{"http://user@host/graphql", "", "http://user@host/graphql"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			ns, url := splitNSURL(c.in)
			if ns != c.ns || url != c.url {
				t.Errorf("splitNSURL(%q) = (%q, %q), want (%q, %q)", c.in, ns, url, c.ns, c.url)
			}
		})
	}
}

func TestOpenapiFlagSet(t *testing.T) {
	var f openapiFlag
	if err := f.Set("petstore.json=pets@http://pets-svc:8080"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := f.Set("http://svc/openapi.json=http://svc"); err != nil {
		t.Fatalf("Set (no ns): %v", err)
	}
	want := openapiFlag{
		{spec: "petstore.json", ns: "pets", url: "http://pets-svc:8080"},
		{spec: "http://svc/openapi.json", ns: "", url: "http://svc"},
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("got %+v, want %+v", f, want)
	}
	for _, bad := range []string{"no-equals", "=http://svc", "spec.json="} {
		var g openapiFlag
		if err := g.Set(bad); err == nil {
			t.Errorf("Set(%q) expected error", bad)
		}
	}
}

func TestGraphqlFlagSet(t *testing.T) {
	var f graphqlFlag
	if err := f.Set("reviews@https://reviews/graphql"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := f.Set("https://bare/graphql"); err != nil {
		t.Fatalf("Set (no ns): %v", err)
	}
	want := graphqlFlag{
		{ns: "reviews", url: "https://reviews/graphql"},
		{ns: "", url: "https://bare/graphql"},
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("got %+v, want %+v", f, want)
	}
	var g graphqlFlag
	if err := g.Set(""); err == nil {
		t.Error("Set(\"\") expected error")
	}
}
