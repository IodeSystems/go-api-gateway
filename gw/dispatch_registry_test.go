package gateway

import (
	"context"
	"testing"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// After a schema rebuild, every dispatchable operation must be
// addressable by its IR SchemaID. That's the contract the runtime
// renderer relies on: resolvers are built via registry lookup, not
// by capturing dispatcher pointers — so a missing entry is a silent
// dispatch failure.
//
// SchemaIDs use the wire-native op name (proto rpc names stay
// PascalCase). The GraphQL surface lower-camels at field-key
// emission, so `greeter.hello` and `greeter.v1.hello` are both
// looked up under the same id `greeter/v1/Hello` — one dispatcher
// serves both surfaces.
func TestDispatchRegistry_PopulatedAfterSchemaBuild(t *testing.T) {
	f := newGRPCE2EFixture(t)

	if got := f.gw.dispatchers.Len(); got == 0 {
		t.Fatal("registry empty after schema build")
	}

	// One entry per (pool, RPC) keyed by IR's op.SchemaID. The flat
	// and versioned graphql surfaces resolve through the same id
	// because RenderGraphQLRuntime walks IR Operations once and
	// emits the same SchemaID under both aliases.
	sid := ir.MakeSchemaID("greeter", "v1", "Hello")
	if d := f.gw.dispatchers.Get(sid); d == nil {
		t.Fatalf("dispatcher missing for %s", sid)
	}

	// The dispatcher fetched from the registry must actually run.
	d := f.gw.dispatchers.Get(sid)
	out, err := d.Dispatch(context.Background(), map[string]any{"name": "registry"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("Dispatch result type: %T", out)
	}
	if got := m["greeting"]; got != "hello registry" {
		t.Fatalf("greeting=%v want %q", got, "hello registry")
	}
	if got := f.greeter.helloCalls.Load(); got != 1 {
		t.Fatalf("backend Hello called %d times, want 1", got)
	}
}

// Schema rebuild must replace the registry — stale entries from a
// prior pool / source layout cannot leak into the new schema, or
// dispatch will route to dead state.
func TestDispatchRegistry_RebuildClearsStaleEntries(t *testing.T) {
	f := newGRPCE2EFixture(t)

	// Stuff a phantom entry under a SchemaID the gateway will never
	// produce. Schema rebuild should drop it.
	stale := ir.SchemaID("phantom/v1/op")
	f.gw.dispatchers.Set(stale, ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		t.Fatal("stale dispatcher must not be invoked")
		return nil, nil
	}))

	f.gw.mu.Lock()
	if err := f.gw.assembleLocked(); err != nil {
		f.gw.mu.Unlock()
		t.Fatalf("rebuild: %v", err)
	}
	f.gw.mu.Unlock()

	if d := f.gw.dispatchers.Get(stale); d != nil {
		t.Fatal("stale registry entry survived rebuild")
	}
	// Real dispatcher must still be there post-rebuild.
	if d := f.gw.dispatchers.Get(ir.MakeSchemaID("greeter", "v1", "Hello")); d == nil {
		t.Fatal("greeter dispatcher missing after rebuild")
	}
}
