package gateway

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
	psv1 "github.com/iodesystems/gwag/v2/gw/proto/ps/v1"
)

// TestInternalProto_RegisterAndDispatch exercises the internal-proto
// slot kind end-to-end: register the bundled gwag.ps.v1.PubSub proto
// under a side-namespace ("intpb") with a stub Pub handler (the real
// "ps" slot is auto-installed in New() and tested separately), then
// call Dispatch through the registry and verify the response shape +
// that the handler observed the args.
func TestInternalProto_RegisterAndDispatch(t *testing.T) {
	g := New()

	var (
		gotChannel string
		gotPayload string
	)
	handlers := map[string]internalProtoHandler{
		"Pub": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			m := req.ProtoReflect()
			gotChannel = m.Get(m.Descriptor().Fields().ByName("channel")).String()
			gotPayload = m.Get(m.Descriptor().Fields().ByName("payload")).String()
			return &psv1.PubResponse{}, nil
		},
	}

	// Pass nil subscriptionHandlers — this test pins the no-handler path
	// for streaming methods; registerProtoDispatchersLocked skips Sub.
	g.mu.Lock()
	err := g.addInternalProtoSlotLocked("intpb", "v1", psv1.File_gw_proto_ps_v1_ps_proto, nil, handlers, nil)
	g.mu.Unlock()
	if err != nil {
		t.Fatalf("addInternalProtoSlotLocked: %v", err)
	}

	g.mu.Lock()
	slot, ok := g.slots[poolKey{namespace: "intpb", version: "v1"}]
	g.mu.Unlock()
	if !ok {
		t.Fatal("expected slot at intpb/v1 after registration")
	}
	if slot.kind != slotKindInternalProto {
		t.Fatalf("slot.kind = %v, want slotKindInternalProto", slot.kind)
	}
	if slot.internalProto == nil {
		t.Fatal("slot.internalProto is nil after registration")
	}
	if len(slot.ir) == 0 {
		t.Fatal("slot.ir is empty after registration; bakeSlotIRLocked did not run")
	}

	// Schema assemble would normally run during the first Handler()
	// call, but the dispatcher table is rebuilt via assembleLocked.
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	sid := ir.MakeSchemaID("intpb", "v1", "Pub")
	d := g.dispatchers.Get(sid)
	if d == nil {
		t.Fatalf("no dispatcher registered under %s", sid)
	}

	resp, err := d.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": "{\"order\":42}",
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if gotChannel != "events.orders.42.update" {
		t.Errorf("handler observed channel %q, want %q", gotChannel, "events.orders.42.update")
	}
	if gotPayload != "{\"order\":42}" {
		t.Errorf("handler observed payload %q, want %q", gotPayload, "{\"order\":42}")
	}
	m, ok := resp.(map[string]any)
	if !ok {
		t.Fatalf("Dispatch returned %T, want map[string]any", resp)
	}
	if len(m) != 0 {
		t.Errorf("Dispatch returned non-empty map %v, want empty (PubResponse has no fields)", m)
	}

	// Sub's server-streaming method must not have a unary dispatcher
	// registered — registerProtoDispatchersLocked skips it.
	subSid := ir.MakeSchemaID("intpb", "v1", "Sub")
	if g.dispatchers.Get(subSid) != nil {
		t.Errorf("unexpected dispatcher registered for Sub (streaming)")
	}
}

// TestInternalProto_AppearsInPublicSDL confirms the internal-proto
// slot's services show up in the rendered SDL the same way upstream
// proto services do — the proto exists for IR / SDL / MCP / admin-
// listing parity.
func TestInternalProto_AppearsInPublicSDL(t *testing.T) {
	g := New()
	handlers := map[string]internalProtoHandler{
		"Pub": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			return &psv1.PubResponse{}, nil
		},
	}
	g.mu.Lock()
	err := g.addInternalProtoSlotLocked("intpb", "v1", psv1.File_gw_proto_ps_v1_ps_proto, nil, handlers, nil)
	g.mu.Unlock()
	if err != nil {
		t.Fatalf("addInternalProtoSlotLocked: %v", err)
	}
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	schema := g.schema.Load()
	if schema == nil {
		t.Fatal("g.schema is nil after assembleLocked")
	}
	sdl := ir.PrintSchemaSDL(schema)
	// The IR ingest pulls method names through, so the rendered SDL
	// contains both pub and sub fields under the ps namespace's
	// GraphQL object.
	if !strings.Contains(sdl, "pub") {
		t.Errorf("SDL missing pub field; got:\n%s", sdl)
	}
	if !strings.Contains(sdl, "sub") {
		t.Errorf("SDL missing sub field; got:\n%s", sdl)
	}
}
