package gateway

import (
	"context"
	"strings"
	"testing"

	authv1 "github.com/iodesystems/gwag/v2/examples/auth/gen/auth/v1"
)

// inventoryTestGateway returns a fresh Gateway with the user/auth
// example protos registered (no pool, just descriptors) so the
// inventory has IR to walk against.
func inventoryTestGateway(t *testing.T) *Gateway {
	t.Helper()
	g := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(g.Close)

	// Register against a sentinel address — the inventory only walks
	// IR, never dispatches, so the address is unused.
	if err := g.AddProtoBytes("user.proto", testProtoBytes(t, "user.proto"), ProtoImports(map[string][]byte{"auth.proto": testProtoBytes(t, "auth.proto")}), To("127.0.0.1:1"), As("user")); err != nil {
		t.Fatalf("AddProtoDescriptor user: %v", err)
	}
	return g
}

func TestInjectorInventory_TypeKeyedActiveLandings(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return &authv1.Context{}, nil
	}))

	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("len(entries)=%d, want %d", got, want)
	}
	e := entries[0]
	if e.Kind != injectorKindType {
		t.Fatalf("Kind=%q want %q", e.Kind, injectorKindType)
	}
	if e.TypeName != "auth.v1.Context" {
		t.Fatalf("TypeName=%q want auth.v1.Context", e.TypeName)
	}
	if !e.Hide {
		t.Fatal("Hide=false; want default true")
	}
	if e.State != injectorStateActive {
		t.Fatalf("State=%q want %q", e.State, injectorStateActive)
	}
	if len(e.Landings) == 0 {
		t.Fatal("no landings; want at least the user.GetMe.auth arg")
	}
	// We expect at least one arg landing (the proto field is distilled
	// into args by IngestProto) and one field landing (the
	// GetMeRequest object type retains the field).
	var sawArg, sawField bool
	for _, l := range e.Landings {
		if l.Kind == "arg" && l.Namespace == "user" && l.Op == "GetMe" && l.ArgName == "auth" {
			sawArg = true
		}
		if l.Kind == "field" && l.FieldName == "auth" {
			sawField = true
		}
	}
	if !sawArg {
		t.Errorf("missing arg landing user.GetMe.auth; got %+v", e.Landings)
	}
	if !sawField {
		t.Errorf("missing field landing on GetMeRequest.auth; got %+v", e.Landings)
	}
	if e.RegisteredAt.File == "" || e.RegisteredAt.Line == 0 {
		t.Errorf("RegisteredAt not captured: %+v", e.RegisteredAt)
	}
	if !strings.HasSuffix(e.RegisteredAt.File, "inject_inventory_test.go") {
		t.Errorf("RegisteredAt.File=%q want suffix inject_inventory_test.go", e.RegisteredAt.File)
	}
}

func TestInjectorInventory_PathKeyedActive(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}))

	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("len(entries)=%d, want %d", got, want)
	}
	e := entries[0]
	if e.Kind != injectorKindPath || e.Path != "user.GetMe.auth" {
		t.Fatalf("Kind=%q Path=%q want path/user.GetMe.auth", e.Kind, e.Path)
	}
	if e.State != injectorStateActive {
		t.Fatalf("State=%q want active; landings=%+v", e.State, e.Landings)
	}
	if len(e.Landings) != 1 {
		t.Fatalf("len(Landings)=%d want 1: %+v", len(e.Landings), e.Landings)
	}
	l := e.Landings[0]
	if l.Kind != "arg" || l.Namespace != "user" || l.Op != "GetMe" || l.ArgName != "auth" {
		t.Errorf("landing=%+v want arg user.GetMe.auth", l)
	}
}

func TestInjectorInventory_PathKeyedDormant(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectPath("user.NoSuchOp.foo", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}))

	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("len(entries)=%d, want %d", got, want)
	}
	e := entries[0]
	if e.State != injectorStateDormant {
		t.Fatalf("State=%q want dormant", e.State)
	}
	if len(e.Landings) != 0 {
		t.Errorf("dormant entry has landings: %+v", e.Landings)
	}
}

func TestInjectorInventory_HeaderActive(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectHeader("X-Tenant-ID", func(_ context.Context, _ *string) (string, error) {
		return "t1", nil
	}))

	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("len(entries)=%d, want %d", got, want)
	}
	e := entries[0]
	if e.Kind != injectorKindHeader || e.HeaderName != "X-Tenant-ID" {
		t.Fatalf("Kind=%q HeaderName=%q want header/X-Tenant-ID", e.Kind, e.HeaderName)
	}
	if e.State != injectorStateActive {
		t.Fatalf("State=%q want active", e.State)
	}
	if len(e.Landings) != 1 || e.Landings[0].Kind != "header" || e.Landings[0].HeaderName != "X-Tenant-ID" {
		t.Errorf("landings=%+v want one header landing", e.Landings)
	}
}

func TestInjectorInventory_HideFalseSurfacesNullable(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}, Hide(false), Nullable(true)))

	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries)=%d want 1", len(entries))
	}
	e := entries[0]
	if e.Hide {
		t.Error("Hide=true; want false")
	}
	if !e.Nullable {
		t.Error("Nullable=false; want true under Nullable(true)")
	}
}

func TestInjectorInventory_Empty(t *testing.T) {
	g := inventoryTestGateway(t)
	entries, err := g.injectorInventory()
	if err != nil {
		t.Fatalf("InjectorInventory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries=%+v; want empty when no Use() calls", entries)
	}
}

// TestEvalInjectPathStates_RegisterDormant covers the registration
// pass: an InjectPath whose path doesn't resolve in the live IR
// surfaces an Initial+Dormant transition exactly once. A subsequent
// re-eval (no schema change) emits no further transitions.
func TestEvalInjectPathStates_RegisterDormant(t *testing.T) {
	g := inventoryTestGateway(t)
	g.Use(InjectPath("user.NoSuchOp.foo", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}))

	g.mu.Lock()
	state := g.injectPathStates["user.NoSuchOp.foo"]
	transitions := g.evalInjectPathStatesLocked()
	g.mu.Unlock()

	if state != injectorStateDormant {
		t.Fatalf("post-Use state=%q want dormant", state)
	}
	if len(transitions) != 0 {
		t.Fatalf("re-eval surfaced unexpected transitions: %+v", transitions)
	}
}

// TestEvalInjectPathStates_DormantToActive covers the activation
// transition: an InjectPath registered before its target schema
// shows up flips dormant→active when a slot brings the path into
// existence on the next eval.
func TestEvalInjectPathStates_DormantToActive(t *testing.T) {
	g := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(g.Close)
	g.Use(InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}))

	g.mu.Lock()
	pre := g.injectPathStates["user.GetMe.auth"]
	g.mu.Unlock()
	if pre != injectorStateDormant {
		t.Fatalf("pre-register state=%q want dormant (no slots yet)", pre)
	}

	if err := g.AddProtoBytes("user.proto", testProtoBytes(t, "user.proto"), ProtoImports(map[string][]byte{"auth.proto": testProtoBytes(t, "auth.proto")}), To("127.0.0.1:1"), As("user")); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}

	g.mu.Lock()
	transitions := g.evalInjectPathStatesLocked()
	post := g.injectPathStates["user.GetMe.auth"]
	g.mu.Unlock()

	if post != injectorStateActive {
		t.Fatalf("post-register state=%q want active", post)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%+v want exactly 1 dormant→active", transitions)
	}
	tr := transitions[0]
	if tr.Path != "user.GetMe.auth" || tr.Previous != injectorStateDormant || tr.Current != injectorStateActive || tr.Initial {
		t.Fatalf("transition=%+v want {path:user.GetMe.auth dormant→active}", tr)
	}

	// Re-eval is silent — already-active rule shouldn't re-fire.
	g.mu.Lock()
	tr2 := g.evalInjectPathStatesLocked()
	g.mu.Unlock()
	if len(tr2) != 0 {
		t.Errorf("idempotent re-eval surfaced transitions: %+v", tr2)
	}
}

// TestEvalInjectPathStates_RegisterActiveSilent confirms the
// initial-eval shape only emits when the rule is dormant: registering
// an InjectPath against an already-present schema is a no-op as far
// as the lifecycle log is concerned.
func TestEvalInjectPathStates_RegisterActiveSilent(t *testing.T) {
	g := inventoryTestGateway(t)

	// Use() runs evalInjectPathStatesLocked under g.mu. Capture the
	// final state map directly — Initial+Active produces no transition.
	g.Use(InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}))

	g.mu.Lock()
	state := g.injectPathStates["user.GetMe.auth"]
	transitions := g.evalInjectPathStatesLocked()
	g.mu.Unlock()

	if state != injectorStateActive {
		t.Fatalf("state=%q want active", state)
	}
	if len(transitions) != 0 {
		t.Fatalf("re-eval transitions=%+v want none", transitions)
	}
}
