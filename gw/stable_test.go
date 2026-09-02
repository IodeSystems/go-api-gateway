package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/IodeSystems/graphql-go/v2"
	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestRuntime_StableAliasMatchesLatest covers the common case:
// stable_vN advances to track the latest cut. The `stable` sub-field
// resolves through the same dispatcher as the bare-namespace flat
// path and the `vN` sub-field (three addressing paths, one operation).
// No deprecation reason — stable is the latest.
func TestRuntime_StableAliasMatchesLatest(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "pets",
		Version:    "v1",
		OriginKind: ir.KindOpenAPI,
		Types: map[string]*ir.Type{
			"Pet": {
				Name:     "Pet",
				TypeKind: ir.TypeObject,
				Fields:   []*ir.Field{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}}},
			},
		},
		Operations: []*ir.Operation{{
			Name:   "getPet",
			Kind:   ir.OpQuery,
			Args:   []*ir.Arg{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true}},
			Output: &ir.TypeRef{Named: "Pet"},
		}},
	}
	ir.PopulateSchemaIDs(svc)

	calls := 0
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		calls++
		return map[string]any{"id": args["id"]}, nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{
		StableVN: map[string]int{"pets": 1},
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	if schema.Type("petsStableQueryNamespace") == nil && schema.Type("PetsStableQueryNamespace") == nil {
		t.Errorf("expected PetsStableQueryNamespace; got %v", schemaTypeNames(schema))
	}

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { stable { getPet(id: "x") { id } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("execute: %v", res.Errors)
	}
	pet := res.Data.(map[string]any)["pets"].(map[string]any)["stable"].(map[string]any)["getPet"].(map[string]any)
	if pet["id"] != "x" {
		t.Errorf("stable.getPet.id = %v, want x", pet["id"])
	}
	if calls != 1 {
		t.Errorf("dispatcher calls = %d, want 1", calls)
	}

	// stable === latest, so no deprecation.
	pets := schema.Type("PetsQueryNamespace")
	if pets == nil {
		t.Fatalf("PetsQueryNamespace missing; types: %v", schemaTypeNames(schema))
	}
	stableField := pets.(*graphql.Object).Fields()["stable"]
	if stableField == nil {
		t.Fatalf("stable field missing on PetsQueryNamespace")
	}
	if stableField.DeprecationReason != "" {
		t.Errorf("stable.DeprecationReason = %q; want empty (stable === latest)", stableField.DeprecationReason)
	}
}

// TestRuntime_StableAliasOlderVersion covers stable lagging latest:
// stable=v1 with v1+v2 in the build means `pets.stable` aliases v1's
// content (older shape, fewer fields) and carries @deprecated.
// Selecting v2-only fields under stable must fail (proves the alias
// uses v1's types, not latest's).
func TestRuntime_StableAliasOlderVersion(t *testing.T) {
	mkSvc := func(ver string, fields []*ir.Field) *ir.Service {
		s := &ir.Service{
			Namespace:  "pets",
			Version:    ver,
			OriginKind: ir.KindOpenAPI,
			Types:      map[string]*ir.Type{"Pet": {Name: "Pet", TypeKind: ir.TypeObject, Fields: fields}},
			Operations: []*ir.Operation{{
				Name:   "getPet",
				Kind:   ir.OpQuery,
				Args:   []*ir.Arg{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true}},
				Output: &ir.TypeRef{Named: "Pet"},
			}},
		}
		ir.PopulateSchemaIDs(s)
		return s
	}
	v1 := mkSvc("v1", []*ir.Field{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}}})
	v2 := mkSvc("v2", []*ir.Field{
		{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}},
		{Name: "tag", Type: ir.TypeRef{Builtin: ir.ScalarString}},
	})

	registry := ir.NewDispatchRegistry()
	registry.Set(v1.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": "v1-" + args["id"].(string)}, nil
	}))
	registry.Set(v2.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": "v2-" + args["id"].(string), "tag": "current"}, nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{v1, v2}, registry, ir.RuntimeOptions{
		StableVN: map[string]int{"pets": 1},
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	// pets.stable.getPet returns v1's shape.
	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { stable { getPet(id: "x") { id } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("stable: %v", res.Errors)
	}
	pet := res.Data.(map[string]any)["pets"].(map[string]any)["stable"].(map[string]any)["getPet"].(map[string]any)
	if pet["id"] != "v1-x" {
		t.Errorf("stable.getPet.id = %v, want v1-x", pet["id"])
	}

	// Selecting v2-only `tag` under stable must fail — v1's Pet has no tag.
	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { stable { getPet(id: "x") { tag } } } }`})
	if len(res.Errors) == 0 {
		t.Errorf("expected stable surface to reject v2-only `tag`")
	}

	// stable lags latest, so the sub-field carries @deprecated.
	pets := schema.Type("PetsQueryNamespace").(*graphql.Object)
	stable := pets.Fields()["stable"]
	if stable == nil {
		t.Fatalf("stable missing")
	}
	if stable.DeprecationReason == "" {
		t.Errorf("stable lagging latest; DeprecationReason empty, want non-empty")
	}
}

// TestRuntime_StableAliasMissingTarget: when stable_vN points to a
// version not currently in the build (its last replica deregistered),
// the renderer omits the `stable` sub-field entirely. The gateway's
// monotonic side-state still holds the higher value so the alias
// snaps back when the cut returns; today the schema can't reference
// types we haven't built.
func TestRuntime_StableAliasMissingTarget(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "pets",
		Version:    "v1",
		OriginKind: ir.KindOpenAPI,
		Types:      map[string]*ir.Type{},
		Operations: []*ir.Operation{{Name: "list", Kind: ir.OpQuery, Output: &ir.TypeRef{Builtin: ir.ScalarString}}},
	}
	ir.PopulateSchemaIDs(svc)
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "ok", nil
	}))

	// stable_vN = 5 but no v5 in build (only v1 lives).
	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{
		StableVN: map[string]int{"pets": 5},
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	pets := schema.Type("PetsQueryNamespace").(*graphql.Object)
	if _, has := pets.Fields()["stable"]; has {
		t.Errorf("stable sub-field present despite missing target vN; fields: %v", fieldNames(pets.Fields()))
	}

	// Selecting `stable` should fail validation.
	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { stable { list } } }`})
	if len(res.Errors) == 0 {
		t.Errorf("expected validation error selecting absent stable sub-field")
	}
}

// TestRuntime_StableSubscriptionFlat: subscriptions emit under
// `<ns>_stable_<op>` (graphql-go forbids nested objects under
// Subscription, so stable flattens like vN does).
func TestRuntime_StableSubscriptionFlat(t *testing.T) {
	mk := func(ver string) *ir.Service {
		s := &ir.Service{
			Namespace:  "events",
			Version:    ver,
			OriginKind: ir.KindGraphQL,
			Types:      map[string]*ir.Type{},
			Operations: []*ir.Operation{{
				Name: "tick", Kind: ir.OpSubscription,
				Output: &ir.TypeRef{Builtin: ir.ScalarString},
			}},
		}
		ir.PopulateSchemaIDs(s)
		return s
	}
	v1 := mk("v1")
	v2 := mk("v2")
	registry := ir.NewDispatchRegistry()
	registry.Set(v1.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) { return "v1tick", nil }))
	registry.Set(v2.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) { return "v2tick", nil }))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{v1, v2}, registry, ir.RuntimeOptions{
		StableVN: map[string]int{"events": 1},
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}
	subRoot := schema.SubscriptionType()
	if subRoot == nil {
		t.Fatal("Subscription root missing")
	}
	if _, ok := subRoot.Fields()["events_stable_tick"]; !ok {
		t.Errorf("Subscription missing events_stable_tick; have %v", fieldNames(subRoot.Fields()))
	}
	// stable=v1 lags latest=v2, so the field carries @deprecated.
	if dep := subRoot.Fields()["events_stable_tick"].DeprecationReason; dep == "" {
		t.Errorf("events_stable_tick missing @deprecated reason; want non-empty (v1 lags v2)")
	}
}

// TestAdvanceStableLocked: the in-memory tracker advances on higher
// vN, holds on equal, and never decrements.
func TestAdvanceStableLocked(t *testing.T) {
	g := &Gateway{}
	g.advanceStableLocked("pets", 0) // unstable: no-op
	if g.stableVN["pets"] != 0 {
		t.Errorf("after unstable: stableVN[pets] = %d, want 0", g.stableVN["pets"])
	}
	g.advanceStableLocked("pets", 3)
	if g.stableVN["pets"] != 3 {
		t.Errorf("after v3: stableVN[pets] = %d, want 3", g.stableVN["pets"])
	}
	g.advanceStableLocked("pets", 1) // monotonic: no-op
	if g.stableVN["pets"] != 3 {
		t.Errorf("after v1 (monotonic): stableVN[pets] = %d, want 3", g.stableVN["pets"])
	}
	g.advanceStableLocked("pets", 5)
	if g.stableVN["pets"] != 5 {
		t.Errorf("after v5: stableVN[pets] = %d, want 5", g.stableVN["pets"])
	}
}

// TestStableMonotonicAcrossRegistration: the gateway-level integration —
// register v2, then v1; stable_vN holds at 2 (the higher cut), not the
// last-registered.
func TestStableMonotonicAcrossRegistration(t *testing.T) {
	g := &Gateway{}
	g.advanceStableLocked("svc", 2)
	g.advanceStableLocked("svc", 1)
	snap := g.stableSnapshotLocked()
	if snap["svc"] != 2 {
		t.Errorf("stable[svc] = %d, want 2 (monotonic)", snap["svc"])
	}
	// Snapshot is a copy: mutating it doesn't bleed back.
	snap["svc"] = 9
	if g.stableVN["svc"] != 2 {
		t.Errorf("snapshot mutation bled into source: stableVN[svc] = %d", g.stableVN["svc"])
	}
}

// TestStableNilSnapshotWhenEmpty: zero state returns nil snapshot so
// the renderer's `nil` fast-path triggers.
func TestStableNilSnapshotWhenEmpty(t *testing.T) {
	g := &Gateway{}
	if snap := g.stableSnapshotLocked(); snap != nil {
		t.Errorf("empty gateway: snapshot = %v, want nil", snap)
	}
}

// TestStableViaGatewayRegistration drives the full path: a real
// Gateway boot with two vN cuts of the same proto under the same
// namespace produces a `stable` sub-field tracking the highest cut.
// Verifies the joinPoolLocked → advanceStableLocked → schema rebuild
// chain wires up end-to-end (the unit tests above stub the registry).
func TestStableViaGatewayRegistration(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)
	_ = gw.Handler() // force initial assemble

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version("v1"),
	); err != nil {
		t.Fatalf("v1 register: %v", err)
	}
	gw.mu.Lock()
	if got := gw.stableVN["greeter"]; got != 1 {
		t.Errorf("after v1: stableVN[greeter] = %d, want 1", got)
	}
	gw.mu.Unlock()

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version("v2"),
	); err != nil {
		t.Fatalf("v2 register: %v", err)
	}
	gw.mu.Lock()
	if got := gw.stableVN["greeter"]; got != 2 {
		t.Errorf("after v2: stableVN[greeter] = %d, want 2", got)
	}
	gw.mu.Unlock()

	schema := gw.schema.Load()
	if schema == nil {
		t.Fatal("schema not assembled")
	}
	greeter, ok := schema.QueryType().Fields()["greeter"]
	if !ok {
		t.Fatal("Query.greeter missing")
	}
	greeterContainer := greeter.Type.(*graphql.NonNull).OfType.(*graphql.Object)
	if _, has := greeterContainer.Fields()["stable"]; !has {
		t.Errorf("Query.greeter.stable missing; fields: %v", fieldNames(greeterContainer.Fields()))
	}

	// Unstable register MUST NOT advance stable (vN==0).
	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version("unstable"),
	); err != nil {
		t.Fatalf("unstable register: %v", err)
	}
	gw.mu.Lock()
	if got := gw.stableVN["greeter"]; got != 2 {
		t.Errorf("unstable register bumped stableVN; got %d, want 2 (unchanged)", got)
	}
	gw.mu.Unlock()
}

// TestStableSDLContainsAlias confirms stable surfaces in the printed
// SDL — schema diff and codegen both consume the SDL, so the field
// must reach there. Light-touch fixture: no full Gateway boot, just a
// runtime-rendered schema piped through ir.PrintSchemaSDL.
func TestStableSDLContainsAlias(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "pets",
		Version:    "v1",
		OriginKind: ir.KindOpenAPI,
		Types:      map[string]*ir.Type{},
		Operations: []*ir.Operation{{Name: "list", Kind: ir.OpQuery, Output: &ir.TypeRef{Builtin: ir.ScalarString}}},
	}
	ir.PopulateSchemaIDs(svc)
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "ok", nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{
		StableVN: map[string]int{"pets": 1},
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}
	sdl := ir.PrintSchemaSDL(schema)
	if !strings.Contains(sdl, "stable: PetsStableQueryNamespace!") {
		t.Errorf("SDL missing `stable: PetsStableQueryNamespace!`:\n%s", sdl)
	}
	if !strings.Contains(sdl, "type PetsStableQueryNamespace") {
		t.Errorf("SDL missing `type PetsStableQueryNamespace`:\n%s", sdl)
	}
}

// --allow-tier without "stable" suppresses the schema alias even when
// numbered cuts are registered. Plan §4: production deployments that
// only want pinned vN can drop the alias entirely.
func TestStableAliasSuppressedByAllowTier(t *testing.T) {
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("ignored")),
		WithAllowTier("vN"),
	)
	t.Cleanup(gw.Close)
	_ = gw.Handler()

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("greeter"),
		Version("v1"),
	); err != nil {
		t.Fatalf("v1 register: %v", err)
	}

	schema := gw.schema.Load()
	if schema == nil {
		t.Fatal("schema not assembled")
	}
	greeter, ok := schema.QueryType().Fields()["greeter"]
	if !ok {
		t.Fatal("Query.greeter missing")
	}
	greeterContainer := greeter.Type.(*graphql.NonNull).OfType.(*graphql.Object)
	if _, has := greeterContainer.Fields()["stable"]; has {
		t.Errorf("Query.greeter.stable surfaced under --allow-tier=vN; fields: %v", fieldNames(greeterContainer.Fields()))
	}
}
