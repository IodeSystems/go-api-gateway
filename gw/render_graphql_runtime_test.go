package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestRuntime_ProtoFlatAndVersioned exercises the smallest fold:
// one proto-origin service `greeter/v1`. The renderer must surface
// the op at both `Query.greeter.hello` (latest flat) AND
// `Query.greeter.v1.hello` (version-addressable). Both paths
// resolve through the same SchemaID — the renderer's lookup
// indirection lets one dispatcher serve both surfaces.
func TestRuntime_ProtoFlatAndVersioned(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "greeter",
		Version:    "v1",
		OriginKind: ir.KindProto,
		Types: map[string]*ir.Type{
			"greeter.HelloReply": {
				Name:     "greeter.HelloReply",
				TypeKind: ir.TypeObject,
				Fields: []*ir.Field{
					{Name: "message", Type: ir.TypeRef{Builtin: ir.ScalarString}},
				},
			},
		},
		Operations: []*ir.Operation{{
			Name:   "hello",
			Kind:   ir.OpQuery,
			Args:   []*ir.Arg{{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}}},
			Output: &ir.TypeRef{Named: "greeter.HelloReply"},
		}},
	}
	ir.PopulateSchemaIDs(svc)

	calls := 0
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		calls++
		return map[string]any{"message": "hello, " + args["name"].(string)}, nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	// Container types follow the SDL naming convention.
	for _, n := range []string{"GreeterQueryNamespace", "GreeterV1QueryNamespace"} {
		if schema.Type(n) == nil {
			t.Errorf("expected type %s; got %v", n, schemaTypeNames(schema))
		}
	}

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ greeter { hello(name: "flat") { message } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("flat: %v", res.Errors)
	}
	flat := res.Data.(map[string]any)["greeter"].(map[string]any)["hello"].(map[string]any)
	if flat["message"] != "hello, flat" {
		t.Errorf("flat.hello.message = %v", flat["message"])
	}

	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ greeter { v1 { hello(name: "ver") { message } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("ver: %v", res.Errors)
	}
	ver := res.Data.(map[string]any)["greeter"].(map[string]any)["v1"].(map[string]any)["hello"].(map[string]any)
	if ver["message"] != "hello, ver" {
		t.Errorf("ver.hello.message = %v", ver["message"])
	}
	if calls != 2 {
		t.Errorf("dispatcher calls = %d, want 2 (one per query)", calls)
	}
}

// TestRuntime_OpenAPIQueryAndMutation covers the OpenAPI-origin
// naming policy (latest → bare `<ns>_` prefix) and the OpQuery /
// OpMutation split into separate roots, both wrapped under the
// namespace container.
func TestRuntime_OpenAPIQueryAndMutation(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "pets",
		Version:    "v1",
		OriginKind: ir.KindOpenAPI,
		Types: map[string]*ir.Type{
			"Pet": {
				Name:     "Pet",
				TypeKind: ir.TypeObject,
				Fields: []*ir.Field{
					{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true},
					{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}},
				},
			},
		},
		Operations: []*ir.Operation{
			{
				Name:   "getPet",
				Kind:   ir.OpQuery,
				Args:   []*ir.Arg{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true}},
				Output: &ir.TypeRef{Named: "Pet"},
			},
			{
				Name:   "createPet",
				Kind:   ir.OpMutation,
				Args:   []*ir.Arg{{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true}},
				Output: &ir.TypeRef{Named: "Pet"},
			},
		},
	}
	ir.PopulateSchemaIDs(svc)

	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": args["id"], "name": "fido"}, nil
	}))
	registry.Set(svc.Operations[1].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": "new", "name": args["name"]}, nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	if schema.Type("pets_Pet") == nil {
		t.Errorf("expected type pets_Pet; got %v", schemaTypeNames(schema))
	}

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { getPet(id: "abc") { id name } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("query: %v", res.Errors)
	}
	pet := res.Data.(map[string]any)["pets"].(map[string]any)["getPet"].(map[string]any)
	if pet["id"] != "abc" || pet["name"] != "fido" {
		t.Errorf("getPet = %v", pet)
	}

	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `mutation { pets { createPet(name: "rex") { id name } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("mutation: %v", res.Errors)
	}
	pet = res.Data.(map[string]any)["pets"].(map[string]any)["createPet"].(map[string]any)
	if pet["id"] != "new" || pet["name"] != "rex" {
		t.Errorf("createPet = %v", pet)
	}
}

// TestRuntime_MultiVersionFold drives the headline behavior of
// step 2: two versions of the same OpenAPI namespace produce
// version-prefixed type names for the older one, latest's content
// flat at top, and per-version sub-groups carrying @deprecated.
func TestRuntime_MultiVersionFold(t *testing.T) {
	mkSvc := func(ver string, petFields []*ir.Field) *ir.Service {
		s := &ir.Service{
			Namespace:  "pets",
			Version:    ver,
			OriginKind: ir.KindOpenAPI,
			Types: map[string]*ir.Type{
				"Pet": {Name: "Pet", TypeKind: ir.TypeObject, Fields: petFields},
			},
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
		{Name: "tag", Type: ir.TypeRef{Builtin: ir.ScalarString}}, // new field in v2
	})

	registry := ir.NewDispatchRegistry()
	registry.Set(v1.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": "old-" + args["id"].(string)}, nil
	}))
	registry.Set(v2.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return map[string]any{"id": "new-" + args["id"].(string), "tag": "current"}, nil
	}))

	// Order intentionally reversed to confirm version sort, not
	// input order, picks latest.
	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{v2, v1}, registry, ir.RuntimeOptions{
		LongType: nil,
		JSONType: nil,
	})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	// Latest's type uses bare prefix; older version uses
	// version-qualified prefix (graphql-go would error on duplicate
	// type names if both were `pets_Pet`).
	for _, n := range []string{"pets_Pet", "pets_v1_Pet"} {
		if schema.Type(n) == nil {
			t.Errorf("expected type %s; got %v", n, schemaTypeNames(schema))
		}
	}

	// Latest flat: v2's getPet.
	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { getPet(id: "x") { id tag } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("flat: %v", res.Errors)
	}
	flat := res.Data.(map[string]any)["pets"].(map[string]any)["getPet"].(map[string]any)
	if flat["id"] != "new-x" {
		t.Errorf("flat.id = %v, want new-x", flat["id"])
	}
	if flat["tag"] != "current" {
		t.Errorf("flat.tag = %v, want current", flat["tag"])
	}

	// v2 sub-group: also v2's getPet (latest is addressable both ways).
	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { v2 { getPet(id: "x") { id } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("v2: %v", res.Errors)
	}
	got := res.Data.(map[string]any)["pets"].(map[string]any)["v2"].(map[string]any)["getPet"].(map[string]any)
	if got["id"] != "new-x" {
		t.Errorf("v2.id = %v, want new-x", got["id"])
	}

	// v1 sub-group: returns v1's Pet (no `tag` field — would error
	// if the v1 sub-group surface used the v2-shaped type).
	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { v1 { getPet(id: "x") { id } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("v1: %v", res.Errors)
	}
	got = res.Data.(map[string]any)["pets"].(map[string]any)["v1"].(map[string]any)["getPet"].(map[string]any)
	if got["id"] != "old-x" {
		t.Errorf("v1.id = %v, want old-x", got["id"])
	}

	// Selecting `tag` on the v1 surface must fail — v1 doesn't
	// expose that field.
	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ pets { v1 { getPet(id: "x") { tag } } } }`})
	if len(res.Errors) == 0 {
		t.Errorf("expected v1 surface to reject `tag` selection (v1's Pet has no tag)")
	}

	// v1 sub-group field carries deprecation reason; v2 does not.
	pets := schema.Type("petsQueryNamespace")
	if pets == nil {
		// graphql-go preserves the case from ObjectConfig.Name.
		// Our convention upper-cases the leading rune of namespace,
		// so the type is "PetsQueryNamespace".
		pets = schema.Type("PetsQueryNamespace")
	}
	if pets == nil {
		t.Fatalf("PetsQueryNamespace missing; types: %v", schemaTypeNames(schema))
	}
	obj, ok := pets.(*graphql.Object)
	if !ok {
		t.Fatalf("PetsQueryNamespace is %T, want *graphql.Object", pets)
	}
	v1Field := obj.Fields()["v1"]
	if v1Field == nil {
		t.Fatalf("v1 sub-field missing on PetsQueryNamespace; fields: %v", fieldNames(obj.Fields()))
	}
	if v1Field.DeprecationReason == "" {
		t.Errorf("v1 sub-field DeprecationReason empty; want non-empty (latest is current)")
	}
	if obj.Fields()["v2"].DeprecationReason != "" {
		t.Errorf("v2 sub-field DeprecationReason = %q; want empty (v2 is latest)", obj.Fields()["v2"].DeprecationReason)
	}
}

// TestRuntime_GraphQLNestedGroup exercises the graphql-origin path
// where the upstream Service has a Group inside latest. The fold
// preserves the group structure under the synthesized namespace
// container — `Query.<ns>.<group>.<op>` — and the recursive
// container naming follows the SDL convention.
func TestRuntime_GraphQLNestedGroup(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "core",
		Version:    "v1",
		OriginKind: ir.KindGraphQL,
		Types: map[string]*ir.Type{
			"Peer": {
				Name:     "Peer",
				TypeKind: ir.TypeObject,
				Fields:   []*ir.Field{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true}},
			},
		},
		Operations: []*ir.Operation{{
			Name:   "ping",
			Kind:   ir.OpQuery,
			Output: &ir.TypeRef{Builtin: ir.ScalarString},
		}},
		Groups: []*ir.OperationGroup{{
			Name: "admin",
			Kind: ir.OpQuery,
			Operations: []*ir.Operation{{
				Name:   "listPeers",
				Kind:   ir.OpQuery,
				Output: &ir.TypeRef{Named: "Peer"}, OutputRepeated: true,
			}},
		}},
	}
	ir.PopulateSchemaIDs(svc)

	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "pong", nil
	}))
	registry.Set(svc.Groups[0].Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return []any{map[string]any{"id": "p1"}}, nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	for _, n := range []string{"CoreQueryNamespace", "CoreAdminQueryNamespace", "CoreV1QueryNamespace", "CoreV1AdminQueryNamespace"} {
		if schema.Type(n) == nil {
			t.Errorf("expected synthesized container %s; got %v", n, schemaTypeNames(schema))
		}
	}

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ core { ping admin { listPeers { id } } } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("execute: %v", res.Errors)
	}
	core := res.Data.(map[string]any)["core"].(map[string]any)
	if core["ping"] != "pong" {
		t.Errorf("ping = %v", core["ping"])
	}
	peers := core["admin"].(map[string]any)["listPeers"].([]any)
	if len(peers) != 1 || peers[0].(map[string]any)["id"] != "p1" {
		t.Errorf("listPeers = %v", peers)
	}
}

// TestRuntime_QueryAndMutationSiblingGroups covers the admin
// dogfood pattern: same namespace hosts both Query and Mutation
// Groups under sibling root fields. Container types disambiguate
// via the kind suffix.
func TestRuntime_QueryAndMutationSiblingGroups(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "admin",
		Version:    "v1",
		OriginKind: ir.KindOpenAPI,
		Types:      map[string]*ir.Type{},
		Operations: []*ir.Operation{
			{Name: "listPeers", Kind: ir.OpQuery, Output: &ir.TypeRef{Builtin: ir.ScalarString}},
			{Name: "forgetPeer", Kind: ir.OpMutation, Args: []*ir.Arg{{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarString}}}, Output: &ir.TypeRef{Builtin: ir.ScalarString}},
		},
	}
	ir.PopulateSchemaIDs(svc)

	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "peers", nil
	}))
	registry.Set(svc.Operations[1].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "forgot-" + args["id"].(string), nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	for _, n := range []string{"AdminQueryNamespace", "AdminMutationNamespace"} {
		if schema.Type(n) == nil {
			t.Errorf("expected %s; got %v", n, schemaTypeNames(schema))
		}
	}

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ admin { listPeers } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("query: %v", res.Errors)
	}
	if got := res.Data.(map[string]any)["admin"].(map[string]any)["listPeers"]; got != "peers" {
		t.Errorf("listPeers = %v", got)
	}

	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `mutation { admin { forgetPeer(id: "p9") } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("mutation: %v", res.Errors)
	}
	if got := res.Data.(map[string]any)["admin"].(map[string]any)["forgetPeer"]; got != "forgot-p9" {
		t.Errorf("forgetPeer = %v", got)
	}
}

// TestRuntime_SubscriptionFlattenAcrossVersions exercises the
// graphql-go "no nested objects under Subscription" workaround
// across multi-version: latest emits `<ns>_<op>`, older emits
// `<ns>_<vN>_<op>`. Subscription Groups recursively flatten with
// `_`-joined names.
func TestRuntime_SubscriptionFlattenAcrossVersions(t *testing.T) {
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

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{v1, v2}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}
	subRoot := schema.SubscriptionType()
	if subRoot == nil {
		t.Fatal("Subscription root missing")
	}
	for _, want := range []string{"events_tick", "events_v1_tick"} {
		if _, ok := subRoot.Fields()[want]; !ok {
			t.Errorf("Subscription missing %s; have %v", want, fieldNames(subRoot.Fields()))
		}
	}
	if dep := subRoot.Fields()["events_v1_tick"].DeprecationReason; dep == "" {
		t.Errorf("events_v1_tick missing @deprecated reason")
	}
	if dep := subRoot.Fields()["events_tick"].DeprecationReason; dep != "" {
		t.Errorf("events_tick has @deprecated %q; want none (latest)", dep)
	}
}

// TestRuntime_DispatchAtCallTime verifies the resolver looks up
// Dispatchers by SchemaID at call time, not at schema-build time —
// swapping or deleting between build and execute is observable.
func TestRuntime_DispatchAtCallTime(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "x",
		Version:    "v1",
		OriginKind: ir.KindProto,
		Types:      map[string]*ir.Type{},
		Operations: []*ir.Operation{{
			Name:   "ping",
			Kind:   ir.OpQuery,
			Output: &ir.TypeRef{Builtin: ir.ScalarString},
		}},
	}
	ir.PopulateSchemaIDs(svc)
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "v1", nil
	}))

	schema, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}

	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "v2", nil
	}))

	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ x { ping } }`})
	if len(res.Errors) > 0 {
		t.Fatalf("execute: %v", res.Errors)
	}
	if v := res.Data.(map[string]any)["x"].(map[string]any)["ping"]; v != "v2" {
		t.Errorf("ping = %v, want v2 (post-swap)", v)
	}

	registry.Delete(svc.Operations[0].SchemaID)
	res = graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ x { ping } }`})
	if len(res.Errors) == 0 {
		t.Fatalf("expected error after dispatcher delete; got %v", res.Data)
	}
	if !strings.Contains(res.Errors[0].Error(), "no dispatcher") {
		t.Errorf("error %q lacks 'no dispatcher'", res.Errors[0].Error())
	}
}

// TestRuntime_OpVersionCollision: an op named the same as a
// version label collides with the synthesized version sub-group
// field. The renderer surfaces this as a build error — better
// loud than silent, and the operator can rename the op.
func TestRuntime_OpVersionCollision(t *testing.T) {
	svc := &ir.Service{
		Namespace:  "weird",
		Version:    "v1",
		OriginKind: ir.KindProto,
		Types:      map[string]*ir.Type{},
		Operations: []*ir.Operation{{
			Name:   "v1", // collides with the v1 sub-group field
			Kind:   ir.OpQuery,
			Output: &ir.TypeRef{Builtin: ir.ScalarString},
		}},
	}
	ir.PopulateSchemaIDs(svc)
	registry := ir.NewDispatchRegistry()
	registry.Set(svc.Operations[0].SchemaID, ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
		return "ok", nil
	}))
	_, err := ir.RenderGraphQLRuntime([]*ir.Service{svc}, registry, ir.RuntimeOptions{})
	if err == nil {
		t.Fatal("expected collision error, got nil")
	}
	if !strings.Contains(err.Error(), "collide") && !strings.Contains(err.Error(), "collision") {
		t.Errorf("error %q does not mention collision", err.Error())
	}
}

// TestRuntime_EmptyServices yields a stub schema with `_status` —
// matches the buildSchemaLocked posture when no services register.
func TestRuntime_EmptyServices(t *testing.T) {
	registry := ir.NewDispatchRegistry()
	schema, err := ir.RenderGraphQLRuntime(nil, registry, ir.RuntimeOptions{})
	if err != nil {
		t.Fatalf("RenderGraphQLRuntime: %v", err)
	}
	res := graphql.Do(graphql.Params{Schema: *schema, RequestString: `{ _status }`})
	if len(res.Errors) > 0 {
		t.Fatalf("execute: %v", res.Errors)
	}
	if v := res.Data.(map[string]any)["_status"]; v != "no services registered" {
		t.Errorf("_status = %v", v)
	}
}

func schemaTypeNames(s *graphql.Schema) []string {
	out := []string{}
	for n := range s.TypeMap() {
		out = append(out, n)
	}
	return out
}

func fieldNames(fields graphql.FieldDefinitionMap) []string {
	out := []string{}
	for n := range fields {
		out = append(out, n)
	}
	return out
}
