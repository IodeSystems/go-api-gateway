package gateway

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/gw/ir"
)

// TestIRTypeBuilder_BuiltinScalars verifies every ir.ScalarKind maps
// to a graphql.Output, and that wrap-flags (Repeated/Required/
// ItemRequired) compose in the canonical T / T! / [T] / [T]! / [T!]! /
// [T!]! order.
func TestIRTypeBuilder_BuiltinScalars(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})

	cases := []struct {
		name     string
		ref      ir.TypeRef
		repeated bool
		required bool
		itemReq  bool
		want     string
	}{
		{"string", ir.TypeRef{Builtin: ir.ScalarString}, false, false, false, "String"},
		{"bool!", ir.TypeRef{Builtin: ir.ScalarBool}, false, true, false, "Boolean!"},
		{"int", ir.TypeRef{Builtin: ir.ScalarInt32}, false, false, false, "Int"},
		{"int64-as-string", ir.TypeRef{Builtin: ir.ScalarInt64}, false, false, false, "String"},
		{"float", ir.TypeRef{Builtin: ir.ScalarFloat}, false, false, false, "Float"},
		{"id!", ir.TypeRef{Builtin: ir.ScalarID}, false, true, false, "ID!"},
		{"[String]", ir.TypeRef{Builtin: ir.ScalarString}, true, false, false, "[String]"},
		{"[String!]!", ir.TypeRef{Builtin: ir.ScalarString}, true, true, true, "[String!]!"},
		{"[String!]", ir.TypeRef{Builtin: ir.ScalarString}, true, false, true, "[String!]"},
		{"[String]!", ir.TypeRef{Builtin: ir.ScalarString}, true, true, false, "[String]!"},
	}
	for _, tc := range cases {
		got, err := b.Output(tc.ref, tc.repeated, tc.required, tc.itemReq)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.String() != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got.String(), tc.want)
		}
	}
}

// TestIRTypeBuilder_Object covers Object construction with mixed
// scalar and named refs, plus the graphql-go forbids-empty-Object
// fallback (_void placeholder).
func TestIRTypeBuilder_Object(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"User": {
			Name:     "User",
			TypeKind: ir.TypeObject,
			Fields: []*ir.Field{
				{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarID}, Required: true},
				{Name: "displayName", Type: ir.TypeRef{Builtin: ir.ScalarString}},
				{Name: "tags", Type: ir.TypeRef{Builtin: ir.ScalarString}, Repeated: true, ItemRequired: true},
			},
		},
		"Empty": {Name: "Empty", TypeKind: ir.TypeObject},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})

	out, err := b.Output(ir.TypeRef{Named: "User"}, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := unwrapNonNull(out).(*graphql.Object)
	if !ok {
		t.Fatalf("expected *graphql.Object, got %T", out)
	}
	fields := obj.Fields()
	if got := fieldTypeStr(fields["id"]); got != "ID!" {
		t.Errorf("id: got %s", got)
	}
	if got := fieldTypeStr(fields["displayName"]); got != "String" {
		t.Errorf("displayName: got %s", got)
	}
	if got := fieldTypeStr(fields["tags"]); got != "[String!]" {
		t.Errorf("tags: got %s", got)
	}

	// Empty object → must surface a _void placeholder field.
	emptyOut, err := b.Output(ir.TypeRef{Named: "Empty"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	emptyObj := emptyOut.(*graphql.Object)
	if _, ok := emptyObj.Fields()["_void"]; !ok {
		t.Errorf("empty object missing _void placeholder")
	}
}

// TestIRTypeBuilder_RecursiveRef verifies that an Object referencing
// itself resolves through the thunk (no infinite recursion at build
// time, the same *graphql.Object instance on both sides of the cycle).
func TestIRTypeBuilder_RecursiveRef(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Node": {
			Name:     "Node",
			TypeKind: ir.TypeObject,
			Fields: []*ir.Field{
				{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarID}},
				{Name: "children", Type: ir.TypeRef{Named: "Node"}, Repeated: true},
			},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	out, err := b.Output(ir.TypeRef{Named: "Node"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	obj := out.(*graphql.Object)
	// Force fields-thunk evaluation: this is what graphql.NewSchema
	// would do at validation time. If the thunk re-entered the
	// builder, the loop guard would trip — same *graphql.Object
	// returned for the inner reference.
	children := obj.Fields()["children"]
	listType, ok := children.Type.(*graphql.List)
	if !ok {
		t.Fatalf("children.Type is %T, expected *graphql.List", children.Type)
	}
	if listType.OfType != obj {
		t.Errorf("recursive ref returned different *graphql.Object; thunk dedup broken")
	}
}

// TestIRTypeBuilder_Enum and Input round-trip an enum and an input
// object through the builder, including custom naming.
func TestIRTypeBuilder_Enum(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Color": {
			Name:     "Color",
			TypeKind: ir.TypeEnum,
			Enum: []ir.EnumValue{
				{Name: "RED", Number: 0},
				{Name: "GREEN", Number: 1},
				{Name: "BLUE", Number: 2, Deprecated: "use COLOR_BLUE"},
			},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{
		EnumName: func(s string) string { return "Test_" + s },
	}, ir.IRTypeBuilderOptions{})
	out, err := b.Output(ir.TypeRef{Named: "Color"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	enum := out.(*graphql.Enum)
	if enum.Name() != "Test_Color" {
		t.Errorf("enum name: got %s, want Test_Color", enum.Name())
	}
	values := enum.Values()
	gotNames := []string{}
	for _, v := range values {
		gotNames = append(gotNames, v.Name)
	}
	for _, want := range []string{"RED", "GREEN", "BLUE"} {
		found := false
		for _, n := range gotNames {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Errorf("enum missing %s", want)
		}
	}
}

func TestIRTypeBuilder_Input(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"NewUser": {
			Name:     "NewUser",
			TypeKind: ir.TypeInput,
			Fields: []*ir.Field{
				{Name: "email", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true},
				{Name: "age", Type: ir.TypeRef{Builtin: ir.ScalarInt32}},
			},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	in, err := b.Input(ir.TypeRef{Named: "NewUser"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	io, ok := in.(*graphql.InputObject)
	if !ok {
		t.Fatalf("expected *graphql.InputObject, got %T", in)
	}
	if io.Name() != "NewUser_Input" {
		t.Errorf("input name: got %s, want NewUser_Input", io.Name())
	}
	fields := io.Fields()
	if fields["email"].Type.String() != "String!" {
		t.Errorf("email: got %s, want String!", fields["email"].Type.String())
	}
}

// TestIRTypeBuilder_OpenAPIOneOf exercises the cross-format chain:
// OpenAPI oneOf → IR TypeUnion → graphql.Union via IRTypeBuilder.
// Pins the contract that a real OpenAPI service's oneOf variants
// surface as a usable GraphQL schema (no JSON-scalar fallback).
func TestIRTypeBuilder_OpenAPIOneOf(t *testing.T) {
	const spec = `{
  "openapi": "3.0.0",
  "info": {"title": "zoo", "version": "1.0.0"},
  "paths": {},
  "components": {
    "schemas": {
      "Cat": {"type": "object", "properties": {"meow": {"type": "boolean"}}},
      "Dog": {"type": "object", "properties": {"bark": {"type": "boolean"}}},
      "Animal": {
        "oneOf": [
          {"$ref": "#/components/schemas/Cat"},
          {"$ref": "#/components/schemas/Dog"}
        ]
      }
    }
  }
}`
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData([]byte(spec))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := doc.Validate(loader.Context); err != nil {
		t.Fatalf("validate: %v", err)
	}
	svc := ir.IngestOpenAPI(doc)

	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	out, err := b.Output(ir.TypeRef{Named: "Animal"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	u, ok := out.(*graphql.Union)
	if !ok {
		t.Fatalf("expected *graphql.Union, got %T", out)
	}
	if len(u.Types()) != 2 {
		t.Errorf("union has %d types, want 2", len(u.Types()))
	}
	gotNames := map[string]bool{}
	for _, ot := range u.Types() {
		gotNames[ot.Name()] = true
	}
	if !gotNames["Cat"] || !gotNames["Dog"] {
		t.Errorf("missing variant: got %v", gotNames)
	}
}

// TestIRTypeBuilder_Union verifies a union projects to graphql.Union
// over its variant Object types and that ResolveType picks via
// __typename.
func TestIRTypeBuilder_Union(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Cat": {
			Name:     "Cat",
			TypeKind: ir.TypeObject,
			Fields:   []*ir.Field{{Name: "meow", Type: ir.TypeRef{Builtin: ir.ScalarBool}}},
		},
		"Dog": {
			Name:     "Dog",
			TypeKind: ir.TypeObject,
			Fields:   []*ir.Field{{Name: "bark", Type: ir.TypeRef{Builtin: ir.ScalarBool}}},
		},
		"Pet": {
			Name:     "Pet",
			TypeKind: ir.TypeUnion,
			Variants: []string{"Cat", "Dog"},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	out, err := b.Output(ir.TypeRef{Named: "Pet"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	u, ok := out.(*graphql.Union)
	if !ok {
		t.Fatalf("expected *graphql.Union, got %T", out)
	}
	if len(u.Types()) != 2 {
		t.Errorf("union types: got %d, want 2", len(u.Types()))
	}
	picked := u.ResolveType(graphql.ResolveTypeParams{Value: map[string]any{"__typename": "Cat"}})
	if picked == nil || picked.Name() != "Cat" {
		t.Errorf("ResolveType picked %v, want Cat", picked)
	}
}

// TestIRTypeBuilder_UnionDiscriminator exercises the OpenAPI-style
// resolver: discriminator property + mapping pick the variant
// without `__typename` on the value.
func TestIRTypeBuilder_UnionDiscriminator(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Cat": {
			Name:     "Cat",
			TypeKind: ir.TypeObject,
			Fields:   []*ir.Field{{Name: "meow", Type: ir.TypeRef{Builtin: ir.ScalarBool}}},
		},
		"Dog": {
			Name:     "Dog",
			TypeKind: ir.TypeObject,
			Fields:   []*ir.Field{{Name: "bark", Type: ir.TypeRef{Builtin: ir.ScalarBool}}},
		},
		"Animal": {
			Name:                  "Animal",
			TypeKind:              ir.TypeUnion,
			Variants:              []string{"Cat", "Dog"},
			DiscriminatorProperty: "kind",
			DiscriminatorMapping:  map[string]string{"feline": "Cat", "canine": "Dog"},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	out, err := b.Output(ir.TypeRef{Named: "Animal"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	u := out.(*graphql.Union)

	// Mapping value → variant name lookup.
	picked := u.ResolveType(graphql.ResolveTypeParams{Value: map[string]any{"kind": "feline"}})
	if picked == nil || picked.Name() != "Cat" {
		t.Errorf("mapping lookup picked %v, want Cat", picked)
	}
	// Identity fallback: "Dog" is not in mapping but matches a variant name directly.
	picked = u.ResolveType(graphql.ResolveTypeParams{Value: map[string]any{"kind": "Dog"}})
	if picked == nil || picked.Name() != "Dog" {
		t.Errorf("identity fallback picked %v, want Dog", picked)
	}
	// __typename still works as a fallback when discriminator value is unknown.
	picked = u.ResolveType(graphql.ResolveTypeParams{Value: map[string]any{
		"kind":       "unknown",
		"__typename": "Cat",
	}})
	if picked == nil || picked.Name() != "Cat" {
		t.Errorf("__typename fallback picked %v, want Cat", picked)
	}
}

// TestIRTypeBuilder_NamingPolicy verifies that an OpenAPI-style
// per-source prefix policy produces distinct graphql type names for
// two services sharing a schema-name like "Pet".
func TestIRTypeBuilder_NamingPolicy(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Pet": {
			Name:     "Pet",
			TypeKind: ir.TypeObject,
			Fields:   []*ir.Field{{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}}},
		},
	}}
	b1 := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{
		ObjectName: func(s string) string { return "petstore_" + s },
	}, ir.IRTypeBuilderOptions{})
	b2 := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{
		ObjectName: func(s string) string { return "petstore_v1_" + s },
	}, ir.IRTypeBuilderOptions{})

	o1, _ := b1.Output(ir.TypeRef{Named: "Pet"}, false, false, false)
	o2, _ := b2.Output(ir.TypeRef{Named: "Pet"}, false, false, false)
	if o1.(*graphql.Object).Name() != "petstore_Pet" {
		t.Errorf("b1: got %s, want petstore_Pet", o1.(*graphql.Object).Name())
	}
	if o2.(*graphql.Object).Name() != "petstore_v1_Pet" {
		t.Errorf("b2: got %s, want petstore_v1_Pet", o2.(*graphql.Object).Name())
	}
}

// TestIRTypeBuilder_LongScalar verifies the OpenAPI-style int64
// override path: when the caller passes a Long scalar via
// IRTypeBuilderOptions.Int64Type, ScalarInt64 refs render through it.
func TestIRTypeBuilder_LongScalar(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	long := b.LongScalar()
	b2 := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{
		Int64Type:  long,
		UInt64Type: long,
	})
	out, err := b2.Output(ir.TypeRef{Builtin: ir.ScalarInt64}, false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "Long!" {
		t.Errorf("got %s, want Long!", out.String())
	}
}

// TestIRTypeBuilder_ValidatesAsSchema asserts the produced types pass
// through graphql.NewSchema validation — the surest test that we
// haven't constructed something graphql-go rejects (e.g. invalid
// names, broken thunks, dangling refs).
func TestIRTypeBuilder_ValidatesAsSchema(t *testing.T) {
	svc := &ir.Service{Types: map[string]*ir.Type{
		"Pet": {
			Name:     "Pet",
			TypeKind: ir.TypeObject,
			Fields: []*ir.Field{
				{Name: "id", Type: ir.TypeRef{Builtin: ir.ScalarID}, Required: true},
				{Name: "owner", Type: ir.TypeRef{Named: "Owner"}},
			},
		},
		"Owner": {
			Name:     "Owner",
			TypeKind: ir.TypeObject,
			Fields: []*ir.Field{
				{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}},
				{Name: "pets", Type: ir.TypeRef{Named: "Pet"}, Repeated: true, ItemRequired: true},
			},
		},
	}}
	b := ir.NewIRTypeBuilder(svc, ir.IRTypeNaming{}, ir.IRTypeBuilderOptions{})
	pet, err := b.Output(ir.TypeRef{Named: "Pet"}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"pet": &graphql.Field{Type: pet},
			},
		}),
	})
	if err != nil {
		t.Fatalf("schema validate: %v", err)
	}
	// Sanity: the schema's PrintSchema-equivalent contains both types.
	sdl := ir.PrintSchemaSDL(&schema)
	if !strings.Contains(sdl, "type Pet") || !strings.Contains(sdl, "type Owner") {
		t.Errorf("schema missing expected types:\n%s", sdl)
	}
}

func unwrapNonNull(t graphql.Output) graphql.Output {
	if nn, ok := t.(*graphql.NonNull); ok {
		return nn.OfType
	}
	return t
}

func fieldTypeStr(f *graphql.FieldDefinition) string {
	if f == nil {
		return "<nil>"
	}
	return f.Type.String()
}
