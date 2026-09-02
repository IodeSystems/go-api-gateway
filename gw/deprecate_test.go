package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/iodesystems/gwag/v2/gw/ir"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// Manual deprecation (plan §5): operator-driven via Deprecate /
// Undeprecate; OR-combines with auto-deprecation of older `vN` cuts
// at render time.

func newDeprecateGateway(t *testing.T) (*Gateway, *controlPlane) {
	t.Helper()
	g := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(g.Close)
	_ = g.Handler() // force assemble
	return g, g.ControlPlane().(*controlPlane)
}

func registerGreeterVersion(t *testing.T, g *Gateway, version string) {
	t.Helper()
	if err := g.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version(version),
	); err != nil {
		t.Fatalf("register %s: %v", version, err)
	}
}

func TestDeprecate_SetsSlotReason(t *testing.T) {
	g, cp := newDeprecateGateway(t)
	registerGreeterVersion(t, g, "v1")

	if _, err := cp.Deprecate(context.Background(), &cpv1.DeprecateRequest{
		Namespace: "greeter",
		Version:   "v1",
		Reason:    "use v2 when available",
	}); err != nil {
		t.Fatalf("Deprecate: %v", err)
	}

	g.mu.Lock()
	s := g.slots[poolKey{namespace: "greeter", version: "v1"}]
	if s == nil || s.deprecationReason != "use v2 when available" {
		t.Errorf("slot deprecationReason = %q; want %q", s.deprecationReason, "use v2 when available")
	}
	for _, svc := range s.ir {
		if svc.Deprecated != "use v2 when available" {
			t.Errorf("ir Service.Deprecated = %q; want propagated", svc.Deprecated)
		}
	}
	g.mu.Unlock()
}

// Empty reason rejects — cleaning is the Undeprecate API's job, not
// Deprecate's.
func TestDeprecate_RejectsEmptyReason(t *testing.T) {
	g, cp := newDeprecateGateway(t)
	registerGreeterVersion(t, g, "v1")

	_, err := cp.Deprecate(context.Background(), &cpv1.DeprecateRequest{
		Namespace: "greeter",
		Version:   "v1",
	})
	if err == nil {
		t.Fatal("expected reject for empty reason; got nil")
	}
	if !strings.Contains(err.Error(), "reason") {
		t.Errorf("error %q should name the missing field", err.Error())
	}
}

// Unknown (ns, ver) rejects — operator gets a clear error rather than
// a silent no-op.
func TestDeprecate_RejectsUnregistered(t *testing.T) {
	_, cp := newDeprecateGateway(t)
	_, err := cp.Deprecate(context.Background(), &cpv1.DeprecateRequest{
		Namespace: "greeter",
		Version:   "v1",
		Reason:    "anything",
	})
	if err == nil {
		t.Fatal("expected reject for unregistered slot; got nil")
	}
	if !strings.Contains(err.Error(), "not currently registered") {
		t.Errorf("error %q should name the unregistered state", err.Error())
	}
}

// Undeprecate clears + returns the prior reason.
func TestUndeprecate_ClearsAndReturnsPrior(t *testing.T) {
	g, cp := newDeprecateGateway(t)
	registerGreeterVersion(t, g, "v1")

	if _, err := cp.Deprecate(context.Background(), &cpv1.DeprecateRequest{
		Namespace: "greeter",
		Version:   "v1",
		Reason:    "old",
	}); err != nil {
		t.Fatalf("Deprecate: %v", err)
	}
	resp, err := cp.Undeprecate(context.Background(), &cpv1.UndeprecateRequest{
		Namespace: "greeter",
		Version:   "v1",
	})
	if err != nil {
		t.Fatalf("Undeprecate: %v", err)
	}
	if resp.GetPriorReason() != "old" {
		t.Errorf("priorReason = %q; want old", resp.GetPriorReason())
	}
	g.mu.Lock()
	s := g.slots[poolKey{namespace: "greeter", version: "v1"}]
	if s == nil || s.deprecationReason != "" {
		t.Errorf("slot deprecationReason = %q; want empty", s.deprecationReason)
	}
	g.mu.Unlock()
}

// combineDepReason: manual wins, auto fallback, both empty → empty.
func TestCombineDepReason_Precedence(t *testing.T) {
	cases := []struct{ manual, auto, want string }{
		{"", "", ""},
		{"manual", "", "manual"},
		{"", "auto", "auto"},
		{"manual", "auto", "manual"}, // manual wins
	}
	for _, c := range cases {
		if got := ir.CombineDepReason(c.manual, c.auto); got != c.want {
			t.Errorf("combineDepReason(%q, %q) = %q; want %q", c.manual, c.auto, got, c.want)
		}
	}
}

// parseDeprecatedKey reverses deprecatedKey; handles the standard
// "ns_v3" / "ns_unstable" shapes plus the empty-component edge cases.
func TestParseDeprecatedKey(t *testing.T) {
	cases := []struct {
		key    string
		ns     string
		ver    string
		wantOK bool
	}{
		{"greeter_v1", "greeter", "v1", true},
		{"users_unstable", "users", "unstable", true},
		{"_v1", "", "v1", false},  // empty namespace
		{"greeter_", "greeter", "", false},  // empty version
		{"noseparator", "", "", false},
	}
	for _, c := range cases {
		gotNS, gotVer, gotOK := parseDeprecatedKey(c.key)
		if gotOK != c.wantOK {
			t.Errorf("parseDeprecatedKey(%q): ok=%v, want %v", c.key, gotOK, c.wantOK)
		}
		if gotOK && (gotNS != c.ns || gotVer != c.ver) {
			t.Errorf("parseDeprecatedKey(%q) = (%q, %q); want (%q, %q)", c.key, gotNS, gotVer, c.ns, c.ver)
		}
	}
}

// Manual deprecation persists across slot re-registration via the
// side-state mirror — a slot that joins after the deprecate event
// inherits the prior reason.
func TestDeprecate_StateSurvivesReregistration(t *testing.T) {
	g, cp := newDeprecateGateway(t)
	registerGreeterVersion(t, g, "unstable")

	if _, err := cp.Deprecate(context.Background(), &cpv1.DeprecateRequest{
		Namespace: "greeter",
		Version:   "unstable",
		Reason:    "moving to v1",
	}); err != nil {
		t.Fatalf("Deprecate: %v", err)
	}

	// Force a slot eviction by re-registering unstable with a different
	// concurrency cap (an unstable swap drops the old slot, allocates
	// a fresh one). Hash equality is via descriptor, so we use the same
	// descriptor — only the slot identity changes.
	g.mu.Lock()
	prior := g.slots[poolKey{namespace: "greeter", version: "unstable"}]
	g.evictSlotLocked(prior)
	delete(g.slots, prior.key)
	g.mu.Unlock()
	registerGreeterVersion(t, g, "unstable")

	g.mu.Lock()
	s := g.slots[poolKey{namespace: "greeter", version: "unstable"}]
	got := ""
	if s != nil {
		got = s.deprecationReason
	}
	g.mu.Unlock()
	if got != "moving to v1" {
		t.Errorf("post-rereg slot deprecationReason = %q; want \"moving to v1\" (side-state should restore)", got)
	}
}
