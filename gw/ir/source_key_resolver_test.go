package ir

// Coverage for sourceKeyResolver — the field resolver that handles
// the snake_case-source / camelCase-schema mismatch introduced when
// gat renames fields for GraphQL idiom. The OpenAPI dispatcher path
// hits this most often; without the resolver, every snake_case JSON
// key (e.g. `created_at` from a typical Go REST handler) resolves to
// `null` and trips non-null schema fields.

import (
	"testing"

	"github.com/IodeSystems/graphql-go/v2"
)

// rp builds a ResolveParams with the given Source for tests; we don't
// need a real Info struct because sourceKeyResolver only reads Source.
func rp(src any) graphql.ResolveParams {
	return graphql.ResolveParams{Source: src}
}

// When original == schemaKey, we return nil so graphql-go uses its
// own DefaultResolveFn unchanged — keeps the proto-no-rename path
// zero-overhead.
func TestSourceKeyResolver_NoRenameReturnsNil(t *testing.T) {
	if got := sourceKeyResolver("foo", "foo"); got != nil {
		t.Fatalf("want nil resolver when names match, got non-nil")
	}
}

// The headline case: REST returns snake_case JSON, schema is
// camelCase, lookup must succeed.
func TestSourceKeyResolver_MapSnakeCase(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	src := map[string]any{"created_at": int64(123456)}
	got, err := fn(rp(src))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != int64(123456) {
		t.Errorf("got %v; want 123456", got)
	}
}

// Proto-origin maps already have camelCase keys (via
// gw/gat/proto_convert.go::protoMessageToMap → lowerCamel). Falling
// back to the schema key keeps that path working.
func TestSourceKeyResolver_MapCamelCaseFallback(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	src := map[string]any{"createdAt": int64(789)}
	got, err := fn(rp(src))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != int64(789) {
		t.Errorf("got %v; want 789", got)
	}
}

// Original wins when both keys are present — the snake_case source
// is the canonical answer for OpenAPI-origin maps.
func TestSourceKeyResolver_MapPrefersOriginal(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	src := map[string]any{
		"created_at": "snake",
		"createdAt":  "camel",
	}
	got, _ := fn(rp(src))
	if got != "snake" {
		t.Errorf("got %v; want snake (original-name wins)", got)
	}
}

// Missing key returns nil cleanly — non-null schema fields will still
// trip downstream, but that's the schema's contract talking, not a
// resolver bug.
func TestSourceKeyResolver_MapMissingReturnsNil(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	src := map[string]any{"other": 1}
	got, err := fn(rp(src))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != nil {
		t.Errorf("got %v; want nil for missing key", got)
	}
}

// A typed string-map source (e.g. body bound to a Go
// map[string]string) goes through the reflect path.
func TestSourceKeyResolver_TypedStringMap(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	src := map[string]string{"created_at": "yes"}
	got, _ := fn(rp(src))
	if got != "yes" {
		t.Errorf("got %v; want yes", got)
	}
}

// Struct sources fall through to DefaultResolveFn, which handles
// case-insensitive Go-field-name matching + json tag lookup — we
// don't want to intercept that path.
func TestSourceKeyResolver_StructDelegatesToDefault(t *testing.T) {
	type Row struct {
		CreatedAt int64 `json:"created_at"`
	}
	fn := sourceKeyResolver("created_at", "createdAt")
	// Mimic graphql-go's call shape: Info.FieldName is the schema key
	// (camelCase). DefaultResolveFn does case-insensitive match against
	// Go's `CreatedAt`, which succeeds.
	p := graphql.ResolveParams{Source: Row{CreatedAt: 42}}
	p.Info.FieldName = "createdAt"
	got, err := fn(p)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != int64(42) {
		t.Errorf("got %v; want 42 from struct delegation", got)
	}
}

// nil source returns nil, not a panic.
func TestSourceKeyResolver_NilSource(t *testing.T) {
	fn := sourceKeyResolver("created_at", "createdAt")
	got, err := fn(rp(nil))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != nil {
		t.Errorf("got %v; want nil", got)
	}
}
