package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/IodeSystems/graphql-go/v2"
	cpv1 "github.com/iodesystems/gwag/gw/proto/controlplane/v1"
)

// TestRetractStable_StandaloneHappyPath: register v1 + v2; retract
// stable to v1; schema's pets.stable now resolves through v1's
// dispatcher (the fold drops v2 from `stable`'s alias). Confirms the
// in-process retract path: control plane RPC → retractStableLocked →
// assembleLocked → renderer picks up new StableVN.
func TestRetractStable_StandaloneHappyPath(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)
	_ = gw.Handler() // force initial assemble

	for _, ver := range []string{"v1", "v2"} {
		if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
			To(nopGRPCConn{}),
			As("greeter"),
			Version(ver),
		); err != nil {
			t.Fatalf("%s register: %v", ver, err)
		}
	}
	gw.mu.Lock()
	if got := gw.stableVN["greeter"]; got != 2 {
		t.Errorf("pre-retract stable = %d, want 2", got)
	}
	gw.mu.Unlock()

	cp := gw.ControlPlane().(*controlPlane)
	resp, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "greeter",
		TargetVN:  1,
	})
	if err != nil {
		t.Fatalf("RetractStable: %v", err)
	}
	if resp.GetPriorVN() != 2 || resp.GetNewVN() != 1 {
		t.Errorf("response: prior=%d new=%d, want 2/1", resp.GetPriorVN(), resp.GetNewVN())
	}

	gw.mu.Lock()
	got := gw.stableVN["greeter"]
	gw.mu.Unlock()
	if got != 1 {
		t.Errorf("post-retract stable = %d, want 1", got)
	}

	// Schema rebuild ran — `stable` sub-field still present, now
	// aliasing v1.
	schema := gw.schema.Load()
	if schema == nil {
		t.Fatal("schema nil")
	}
	greeter := schema.QueryType().Fields()["greeter"]
	if greeter == nil {
		t.Fatal("Query.greeter missing")
	}
	greeterContainer := greeter.Type.(*graphql.NonNull).OfType.(*graphql.Object)
	stable := greeterContainer.Fields()["stable"]
	if stable == nil {
		t.Fatalf("stable missing post-retract; fields: %v", fieldNames(greeterContainer.Fields()))
	}
	// Stable now lags latest (v1 < v2), so it carries @deprecated.
	if stable.DeprecationReason == "" {
		t.Errorf("post-retract stable lacks @deprecated; should be set since v1 < v2")
	}
}

// TestRetractStable_RefuseUnregisteredVN: stable=v3, slot v3 was the
// only registration. Retracting to v2 fails because v2 was never
// registered — stable would point at a missing build.
func TestRetractStable_RefuseUnregisteredVN(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)
	_ = gw.Handler()

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version("v3"),
	); err != nil {
		t.Fatalf("v3 register: %v", err)
	}

	cp := gw.ControlPlane().(*controlPlane)
	_, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "greeter",
		TargetVN:  2,
	})
	if err == nil {
		t.Fatal("expected error for unregistered v2 target")
	}
	if !strings.Contains(err.Error(), "v2") || !strings.Contains(err.Error(), "not currently registered") {
		t.Errorf("error = %q, want it to mention v2 + not currently registered", err.Error())
	}

	// Local stable unchanged.
	gw.mu.Lock()
	got := gw.stableVN["greeter"]
	gw.mu.Unlock()
	if got != 3 {
		t.Errorf("stable mutated despite refusal: %d, want 3", got)
	}
}

// TestRetractStable_RefuseSameOrHigher: target_vN >= current stable
// is nonsensical (raise via registration, not retract).
func TestRetractStable_RefuseSameOrHigher(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)
	_ = gw.Handler()

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"), To(nopGRPCConn{}), As("greeter"), Version("v2")); err != nil {
		t.Fatalf("v2 register: %v", err)
	}

	cp := gw.ControlPlane().(*controlPlane)

	// Same vN.
	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "greeter", TargetVN: 2,
	}); err == nil {
		t.Error("expected error for target_vN equal to current stable")
	}
	// Higher vN.
	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "greeter", TargetVN: 5,
	}); err == nil {
		t.Error("expected error for target_vN higher than current stable")
	}
}

// TestRetractStable_RefuseEmptyOrZero: empty namespace, target_vN=0.
func TestRetractStable_RefuseEmptyOrZero(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)
	_ = gw.Handler()

	cp := gw.ControlPlane().(*controlPlane)

	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "", TargetVN: 1,
	}); err == nil {
		t.Error("expected error for empty namespace")
	}
	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "x", TargetVN: 0,
	}); err == nil {
		t.Error("expected error for target_vN=0")
	}
	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "never_set", TargetVN: 1,
	}); err == nil {
		t.Error("expected error retracting a namespace with no stable")
	}
}

// TestObserveStableFromKV_AcceptsRetract: the watch-loop observer
// must follow KV truth, including decreases — that's how cluster
// peers learn about a retract on another node. Earlier code was
// monotonic and would have ignored a lower KV value.
func TestObserveStableFromKV_AcceptsRetract(t *testing.T) {
	g := &Gateway{}
	g.observeStableFromKVLocked("ns", 5)
	if g.stableVN["ns"] != 5 {
		t.Fatalf("setup: stableVN[ns] = %d, want 5", g.stableVN["ns"])
	}
	if !g.observeStableFromKVLocked("ns", 3) {
		t.Error("observe v3 (retract) returned false; want true (changed)")
	}
	if g.stableVN["ns"] != 3 {
		t.Errorf("after retract observe: stableVN[ns] = %d, want 3", g.stableVN["ns"])
	}
	if g.observeStableFromKVLocked("ns", 3) {
		t.Error("re-observe same value returned true; want false (unchanged)")
	}
	// vN=0 means bucket cleared / retract-to-none.
	if !g.observeStableFromKVLocked("ns", 0) {
		t.Error("observe v0 (clear) returned false; want true")
	}
	if _, has := g.stableVN["ns"]; has {
		t.Error("zero observation should drop the entry, not leave it at 0")
	}
}

// TestClusterE2E_RetractStablePropagates: retract on node A reaches
// node B via the stable KV bucket and B's local stable map drops too.
// Verifies the cross-cluster wire of RetractStable.
func TestClusterE2E_RetractStablePropagates(t *testing.T) {
	resetSharedCluster(t)
	_, _ = getSharedCluster(t)
	a := newSharedClusterNode(t, testCluster.a)
	b := newSharedClusterNode(t, testCluster.b)
	waitForPeers(t, a.gw, b.gw)

	// Seed stable=v3 on both via direct advance + KV propagation. We
	// also need a slot for v3 on A so the retract guard (target_vN
	// must currently be registered) clears for the v1 retract step
	// below — register v1 + v3 directly on A.
	for _, ver := range []string{"v1", "v3"} {
		if err := a.gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
			To(nopGRPCConn{}),
			As("svc"),
			Version(ver),
		); err != nil {
			t.Fatalf("A register %s: %v", ver, err)
		}
	}

	// Wait for both nodes to converge on stable=v3.
	waitForStableConvergence(t, 15*time.Second, []*Gateway{a.gw, b.gw}, func(snap map[string]int) bool {
		return snap["svc"] == 3
	})

	// Retract on A → expect B to follow.
	cp := a.gw.ControlPlane().(*controlPlane)
	if _, err := cp.RetractStable(context.Background(), &cpv1.RetractStableRequest{
		Namespace: "svc",
		TargetVN:  1,
	}); err != nil {
		t.Fatalf("RetractStable on A: %v", err)
	}

	waitForStableConvergence(t, 15*time.Second, []*Gateway{b.gw}, func(snap map[string]int) bool {
		return snap["svc"] == 1
	})
}
