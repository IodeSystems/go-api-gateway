package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestExtractChannelBindings_FromProtoOption pins that
// `(gwag.ps.v1.binding)` on a message round-trips through
// protocompile and surfaces on the extracted ChannelBindings list as
// (pattern, message FQN). Top-level and nested messages both count;
// messages without the option are ignored.
func TestExtractChannelBindings_FromProtoOption(t *testing.T) {
	optionsBytes, err := os.ReadFile("proto/ps/v1/options.proto")
	if err != nil {
		t.Fatalf("read options.proto: %v", err)
	}
	src := []byte(`syntax = "proto3";

package example.events.v1;

import "gw/proto/ps/v1/options.proto";

message OrderUpdate {
  option (gwag.ps.v1.binding) = { pattern: "events.orders.*.update" };
  string order_id = 1;
}

message ShipmentCreated {
  option (gwag.ps.v1.binding) = { pattern: "events.shipments.>" };
  string shipment_id = 1;

  // Nested-with-binding to confirm walkMessagesForBindings recurses.
  message LegEvent {
    option (gwag.ps.v1.binding) = { pattern: "events.shipments.legs.*.update" };
    string leg_id = 1;
  }
}

message NoBinding {
  string thing = 1;
}
`)
	fd, err := compileProtoBytes("entry.proto", src, map[string][]byte{
		"gw/proto/ps/v1/options.proto": optionsBytes,
	})
	if err != nil {
		t.Fatalf("compileProtoBytes: %v", err)
	}

	got := extractChannelBindings(fd)
	sort.Slice(got, func(i, j int) bool { return got[i].Pattern < got[j].Pattern })
	want := []ir.ChannelBinding{
		{Pattern: "events.orders.*.update", MessageFQN: "example.events.v1.OrderUpdate"},
		{Pattern: "events.shipments.>", MessageFQN: "example.events.v1.ShipmentCreated"},
		{Pattern: "events.shipments.legs.*.update", MessageFQN: "example.events.v1.ShipmentCreated.LegEvent"},
	}
	sort.Slice(want, func(i, j int) bool { return want[i].Pattern < want[j].Pattern })
	if len(got) != len(want) {
		t.Fatalf("got %d bindings, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("binding[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestExtractChannelBindings_NoOptionsFileImport pins the no-op case:
// a proto file that never imports options.proto yields no bindings,
// even when other messages in the same compile have bindings. Empty
// vs nil is irrelevant — callers iterate without checking.
func TestExtractChannelBindings_NoOptionsFileImport(t *testing.T) {
	src := []byte(`syntax = "proto3";
package example.plain.v1;

message Thing { string id = 1; }
`)
	fd, err := compileProtoBytes("entry.proto", src, nil)
	if err != nil {
		t.Fatalf("compileProtoBytes: %v", err)
	}
	if got := extractChannelBindings(fd); len(got) != 0 {
		t.Errorf("expected no bindings, got %#v", got)
	}
}

// TestBakeSlotIR_StampsChannelBindings pins the end-to-end:
// a proto file registered via the internal-proto path lands with its
// ChannelBindings populated on every Service in s.ir. This is the
// path schema-rebuild aggregation (todo 8) will walk.
func TestBakeSlotIR_StampsChannelBindings(t *testing.T) {
	optionsBytes, err := os.ReadFile("proto/ps/v1/options.proto")
	if err != nil {
		t.Fatalf("read options.proto: %v", err)
	}
	src := []byte(`syntax = "proto3";
package example.bake.v1;

import "gw/proto/ps/v1/options.proto";

message Tick {
  option (gwag.ps.v1.binding) = { pattern: "ticks.>" };
  string id = 1;
}

service Heartbeat {
  rpc Beat(Tick) returns (Tick);
}
`)
	fd, err := compileProtoBytes("entry.proto", src, map[string][]byte{
		"gw/proto/ps/v1/options.proto": optionsBytes,
	})
	if err != nil {
		t.Fatalf("compileProtoBytes: %v", err)
	}

	g := &Gateway{}
	handlers := map[string]internalProtoHandler{
		"Beat": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			return req, nil
		},
	}
	if err := g.addInternalProtoSlotLocked("example_bake", "v1", fd, src, handlers, nil); err != nil {
		t.Fatalf("addInternalProtoSlotLocked: %v", err)
	}
	s := g.slots[poolKey{namespace: "example_bake", version: "v1"}]
	if s == nil {
		t.Fatal("slot not created")
	}
	if len(s.ir) == 0 {
		t.Fatal("slot.ir empty after bake")
	}
	for i, svc := range s.ir {
		if len(svc.ChannelBindings) != 1 {
			t.Fatalf("svc[%d].ChannelBindings = %#v, want 1 entry", i, svc.ChannelBindings)
		}
		got := svc.ChannelBindings[0]
		want := ir.ChannelBinding{Pattern: "ticks.>", MessageFQN: "example.bake.v1.Tick"}
		if got != want {
			t.Errorf("svc[%d].ChannelBindings[0] = %+v, want %+v", i, got, want)
		}
	}
}

// installInternalProtoWithBinding registers a single-message,
// single-method internal-proto slot at (ns, ver) whose only message
// declares `(gwag.ps.v1.binding) = { pattern }`. Helper used by the
// cross-slot uniqueness tests below — keeps each test self-contained
// without re-spelling the proto source.
func installInternalProtoWithBinding(t *testing.T, g *Gateway, ns, ver, pattern string) {
	t.Helper()
	optionsBytes, err := os.ReadFile("proto/ps/v1/options.proto")
	if err != nil {
		t.Fatalf("read options.proto: %v", err)
	}
	pkg := strings.ReplaceAll(ns, "_", ".") + "." + ver
	entry := fmt.Sprintf("entry_%s_%s.proto", ns, ver)
	src := []byte(fmt.Sprintf(`syntax = "proto3";
package %s;

import "gw/proto/ps/v1/options.proto";

message Event {
  option (gwag.ps.v1.binding) = { pattern: %q };
  string id = 1;
}

service S {
  rpc Echo(Event) returns (Event);
}
`, pkg, pattern))
	fd, err := compileProtoBytes(entry, src, map[string][]byte{
		"gw/proto/ps/v1/options.proto": optionsBytes,
	})
	if err != nil {
		t.Fatalf("compile %s/%s: %v", ns, ver, err)
	}
	handlers := map[string]internalProtoHandler{
		"Echo": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			return req, nil
		},
	}
	g.mu.Lock()
	if err := g.addInternalProtoSlotLocked(ns, ver, fd, src, handlers, nil); err != nil {
		g.mu.Unlock()
		t.Fatalf("addInternalProtoSlotLocked(%s/%s): %v", ns, ver, err)
	}
	g.mu.Unlock()
}

// TestRegisterSlot_CrossSlotBindingConflict_Rejected pins the pre-1.0
// rule that two different `(namespace, version)` slots cannot both
// declare the same channel binding pattern. The conflict is hard-
// rejected at slot registration; the prior occupant stays intact.
func TestRegisterSlot_CrossSlotBindingConflict_Rejected(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.*.update")

	optionsBytes, err := os.ReadFile("proto/ps/v1/options.proto")
	if err != nil {
		t.Fatalf("read options.proto: %v", err)
	}
	// Different namespace/version, same pattern → must reject.
	conflictSrc := []byte(`syntax = "proto3";
package shipments.v1;

import "gw/proto/ps/v1/options.proto";

message Update {
  option (gwag.ps.v1.binding) = { pattern: "events.orders.*.update" };
  string id = 1;
}

service S { rpc Echo(Update) returns (Update); }
`)
	fd, err := compileProtoBytes("conflict.proto", conflictSrc, map[string][]byte{
		"gw/proto/ps/v1/options.proto": optionsBytes,
	})
	if err != nil {
		t.Fatalf("compile conflict: %v", err)
	}
	handlers := map[string]internalProtoHandler{
		"Echo": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			return req, nil
		},
	}
	g.mu.Lock()
	err = g.addInternalProtoSlotLocked("shipments", "v1", fd, conflictSrc, handlers, nil)
	g.mu.Unlock()
	if err == nil {
		t.Fatal("expected cross-slot binding conflict reject; got nil")
	}
	if !strings.Contains(err.Error(), "events.orders.*.update") {
		t.Errorf("error %q missing conflicting pattern", err.Error())
	}
	if !strings.Contains(err.Error(), "already claimed by orders/v1") {
		t.Errorf("error %q missing prior-claimant attribution", err.Error())
	}
	if _, ok := g.slots[poolKey{namespace: "shipments", version: "v1"}]; ok {
		t.Error("rejected slot was inserted anyway")
	}
	// Prior slot should still own its binding.
	s := g.slots[poolKey{namespace: "orders", version: "v1"}]
	if s == nil || len(s.channelBindings) != 1 || s.channelBindings[0].Pattern != "events.orders.*.update" {
		t.Errorf("prior slot lost bindings after rejection: %+v", s)
	}
}

// TestRegisterSlot_CrossSlotBinding_NoConflictWhenPatternsDiffer pins
// that two slots with distinct patterns coexist — the check fires
// only on overlap.
func TestRegisterSlot_CrossSlotBinding_NoConflictWhenPatternsDiffer(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.>")
	installInternalProtoWithBinding(t, g, "shipments", "v1", "events.shipments.>")
	if g.slots[poolKey{namespace: "shipments", version: "v1"}] == nil {
		t.Fatal("second slot not installed despite distinct pattern")
	}
}

// TestRegisterSlot_CrossSlotBindingReleased_ClearsClaim pins that
// when a slot's last replica drops (`releaseSlotLocked`), its bindings
// release with it, so a fresh slot can claim the freed pattern.
func TestRegisterSlot_CrossSlotBindingReleased_ClearsClaim(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.>")
	g.mu.Lock()
	g.releaseSlotLocked(poolKey{namespace: "orders", version: "v1"})
	g.mu.Unlock()
	// With the slot released, a different namespace can adopt the pattern.
	installInternalProtoWithBinding(t, g, "shipments", "v1", "events.orders.>")
	s := g.slots[poolKey{namespace: "shipments", version: "v1"}]
	if s == nil || len(s.channelBindings) != 1 {
		t.Fatalf("shipments/v1 did not install after orders/v1 released: %+v", s)
	}
}

// TestRegisterSlot_CrossSlotBinding_UnstableSelfSwap pins that an
// unstable slot re-registering with the SAME pattern is not a self-
// conflict — the registration is a no-op (same hash) or a swap whose
// own bindings are excluded from the cross-slot check.
func TestRegisterSlot_CrossSlotBinding_UnstableSelfSwap(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "unstable", "events.orders.>")
	// Re-register the same slot with the same proto → hash match, idempotent.
	installInternalProtoWithBinding(t, g, "orders", "unstable", "events.orders.>")
	s := g.slots[poolKey{namespace: "orders", version: "unstable"}]
	if s == nil || len(s.channelBindings) != 1 || s.channelBindings[0].Pattern != "events.orders.>" {
		t.Fatalf("self-swap clobbered slot: %+v", s)
	}
}

// TestRebuildChannelBindingIndex_AggregatesAcrossSlots pins that
// rebuildChannelBindingIndexLocked collects bindings from every
// registered slot into the gateway-wide index.
func TestRebuildChannelBindingIndex_AggregatesAcrossSlots(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.*.update")
	installInternalProtoWithBinding(t, g, "shipments", "v1", "events.shipments.>")

	g.mu.Lock()
	g.rebuildChannelBindingIndexLocked()
	g.mu.Unlock()

	entries := g.channelBindingIndexSnapshot()
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %#v", len(entries), entries)
	}

	// Check both entries are present.
	found := map[string]channelBindingEntry{}
	for _, e := range entries {
		found[e.pattern] = e
	}
	if e, ok := found["events.orders.*.update"]; !ok {
		t.Error("missing orders binding")
	} else {
		if e.namespace != "orders" || e.version != "v1" {
			t.Errorf("orders binding ns/ver = %s/%s, want orders/v1", e.namespace, e.version)
		}
		if e.messageFQN != "orders.v1.Event" {
			t.Errorf("orders binding messageFQN = %s, want orders.v1.Event", e.messageFQN)
		}
	}
	if e, ok := found["events.shipments.>"]; !ok {
		t.Error("missing shipments binding")
	} else {
		if e.namespace != "shipments" || e.version != "v1" {
			t.Errorf("shipments binding ns/ver = %s/%s, want shipments/v1", e.namespace, e.version)
		}
	}
}

// TestRebuildChannelBindingIndex_SlotReleaseRemovesBinding pins that
// when a slot is released, its bindings disappear from the index
// after the next rebuild.
func TestRebuildChannelBindingIndex_SlotReleaseRemovesBinding(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.>")
	installInternalProtoWithBinding(t, g, "shipments", "v1", "events.shipments.>")

	g.mu.Lock()
	g.rebuildChannelBindingIndexLocked()
	g.mu.Unlock()

	if got := len(g.channelBindingIndexSnapshot()); got != 2 {
		t.Fatalf("initial index has %d entries, want 2", got)
	}

	// Release orders slot.
	g.mu.Lock()
	g.releaseSlotLocked(poolKey{namespace: "orders", version: "v1"})
	g.rebuildChannelBindingIndexLocked()
	g.mu.Unlock()

	entries := g.channelBindingIndexSnapshot()
	if len(entries) != 1 {
		t.Fatalf("after release got %d entries, want 1: %#v", len(entries), entries)
	}
	if entries[0].pattern != "events.shipments.>" {
		t.Errorf("remaining binding = %q, want events.shipments.>", entries[0].pattern)
	}
}

// TestChannelBindingIndex_LookupPayloadType pins the pattern matching
// logic used by psPub to stamp Event.payload_type.
func TestChannelBindingIndex_LookupPayloadType(t *testing.T) {
	idx := &channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "example.OrderUpdate", namespace: "orders", version: "v1"},
			{pattern: "events.shipments.>", messageFQN: "example.ShipmentEvent", namespace: "shipments", version: "v1"},
			{pattern: "ticks", messageFQN: "example.Tick", namespace: "clock", version: "v1"},
		},
	}

	tests := []struct {
		channel string
		want    string
	}{
		{"events.orders.42.update", "example.OrderUpdate"},
		{"events.orders.999.update", "example.OrderUpdate"},
		{"events.shipments.created", "example.ShipmentEvent"},
		{"events.shipments.legs.1.arrive", "example.ShipmentEvent"},
		{"ticks", "example.Tick"},
		{"events.unknown.foo", ""},
	}
	for _, tc := range tests {
		got := idx.lookupPayloadType(tc.channel)
		if got != tc.want {
			t.Errorf("lookupPayloadType(%q) = %q, want %q", tc.channel, got, tc.want)
		}
	}
}

// TestChannelBindingIndex_LookupPayloadType_NilIndex pins the nil-safe
// path — a gateway that has never assembled returns "" for any channel.
func TestChannelBindingIndex_LookupPayloadType_NilIndex(t *testing.T) {
	var idx *channelBindingIndex
	if got := idx.lookupPayloadType("anything"); got != "" {
		t.Errorf("nil index lookup returned %q, want empty", got)
	}
}

// TestAdminBindings_Endpoint pins the GET /admin/bindings huma
// endpoint: it returns the aggregated binding entries with namespace,
// version, pattern, and messageFQN.
func TestAdminBindings_Endpoint(t *testing.T) {
	g := newSlotGateway(t)
	installInternalProtoWithBinding(t, g, "orders", "v1", "events.orders.*.update")
	installInternalProtoWithBinding(t, g, "shipments", "v1", "events.shipments.>")

	// Rebuild the index so the admin endpoint has data.
	g.mu.Lock()
	g.rebuildChannelBindingIndexLocked()
	g.mu.Unlock()

	mux, _, err := g.AdminHumaRouter()
	if err != nil {
		t.Fatalf("AdminHumaRouter: %v", err)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := httpGet(srv.URL + "/admin/bindings")
	if err != nil {
		t.Fatalf("GET /admin/bindings: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// Huma wraps response with $schema at top level; use flat struct
	// to match the actual JSON shape (same pattern as
	// TestAdminHuma_Channels_EmptyWhenNoBroker).
	var out struct {
		Bindings []bindingInfo `json:"bindings"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if len(out.Bindings) != 2 {
		t.Fatalf("got %d bindings, want 2: %s", len(out.Bindings), body)
	}

	patterns := map[string]bindingInfo{}
	for _, b := range out.Bindings {
		patterns[b.Pattern] = b
	}
	if b, ok := patterns["events.orders.*.update"]; !ok {
		t.Error("missing orders binding in admin response")
	} else {
		if b.Namespace != "orders" || b.Version != "v1" {
			t.Errorf("orders binding ns/ver = %s/%s, want orders/v1", b.Namespace, b.Version)
		}
	}
	if b, ok := patterns["events.shipments.>"]; !ok {
		t.Error("missing shipments binding in admin response")
	} else {
		if b.Namespace != "shipments" || b.Version != "v1" {
			t.Errorf("shipments binding ns/ver = %s/%s, want shipments/v1", b.Namespace, b.Version)
		}
	}
}

// TestAdminBindings_Empty pins the empty case: no slots with bindings
// returns an empty array (not null).
func TestAdminBindings_Empty(t *testing.T) {
	g := newSlotGateway(t)

	mux, _, err := g.AdminHumaRouter()
	if err != nil {
		t.Fatalf("AdminHumaRouter: %v", err)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := httpGet(srv.URL + "/admin/bindings")
	if err != nil {
		t.Fatalf("GET /admin/bindings: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var out struct {
		Bindings []bindingInfo `json:"bindings"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if out.Bindings == nil {
		t.Error("bindings is nil, want empty slice")
	}
	if len(out.Bindings) != 0 {
		t.Errorf("got %d bindings, want 0", len(out.Bindings))
	}
}

func httpGet(url string) (*http.Response, error) {
	return http.Get(url)
}

// TestWithChannelBinding_Basic pins that WithChannelBinding populates
// the ps slot with runtime-declared bindings during New(). The
// bindings appear in the slot, the IR, and the aggregated index.
func TestWithChannelBinding_Basic(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "chbind-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	defer cluster.Close()

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("test")),
		WithChannelBinding("events.orders.*.update", "example.events.v1.OrderUpdate"),
		WithChannelBinding("events.shipments.>", "example.events.v1.ShipmentEvent"),
	)
	defer g.Close()

	// Check ps slot has the bindings.
	psSlot := g.slots[poolKey{namespace: "ps", version: "v1"}]
	if psSlot == nil {
		t.Fatal("ps slot not installed")
	}
	if len(psSlot.channelBindings) != 2 {
		t.Fatalf("ps slot has %d bindings, want 2: %#v", len(psSlot.channelBindings), psSlot.channelBindings)
	}
	if psSlot.channelBindings[0].Pattern != "events.orders.*.update" {
		t.Errorf("binding[0].pattern = %q, want events.orders.*.update", psSlot.channelBindings[0].Pattern)
	}
	if psSlot.channelBindings[0].MessageFQN != "example.events.v1.OrderUpdate" {
		t.Errorf("binding[0].messageFQN = %q, want example.events.v1.OrderUpdate", psSlot.channelBindings[0].MessageFQN)
	}
	if psSlot.channelBindings[1].Pattern != "events.shipments.>" {
		t.Errorf("binding[1].pattern = %q, want events.shipments.>", psSlot.channelBindings[1].Pattern)
	}

	// Check aggregated index.
	entries := g.channelBindingIndexSnapshot()
	if len(entries) != 2 {
		t.Fatalf("index has %d entries, want 2: %#v", len(entries), entries)
	}

	// Check lookup works.
	idx := g.channelBindingIndex.Load()
	if got := idx.lookupPayloadType("events.orders.42.update"); got != "example.events.v1.OrderUpdate" {
		t.Errorf("lookupPayloadType(events.orders.42.update) = %q, want example.events.v1.OrderUpdate", got)
	}
	if got := idx.lookupPayloadType("events.shipments.created"); got != "example.events.v1.ShipmentEvent" {
		t.Errorf("lookupPayloadType(events.shipments.created) = %q, want example.events.v1.ShipmentEvent", got)
	}
}

// TestWithChannelBinding_CrossSlotConflict pins that a runtime
// binding conflicting with an existing slot is rejected at New() time.
func TestWithChannelBinding_CrossSlotConflict(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "chbind-conflict",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	defer cluster.Close()

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("test")),
	)
	defer g.Close()

	// Register a proto slot with a binding.
	optionsBytes, err := os.ReadFile("proto/ps/v1/options.proto")
	if err != nil {
		t.Fatalf("read options.proto: %v", err)
	}
	src := []byte(`syntax = "proto3";
package example.orders.v1;

import "gw/proto/ps/v1/options.proto";

message OrderUpdate {
  option (gwag.ps.v1.binding) = { pattern: "events.orders.*.update" };
  string id = 1;
}

service S { rpc Echo(OrderUpdate) returns (OrderUpdate); }
`)
	fd, err := compileProtoBytes("orders.proto", src, map[string][]byte{
		"gw/proto/ps/v1/options.proto": optionsBytes,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	handlers := map[string]internalProtoHandler{
		"Echo": func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
			return req, nil
		},
	}
	g.mu.Lock()
	err = g.addInternalProtoSlotLocked("orders", "v1", fd, src, handlers, nil)
	g.mu.Unlock()
	if err != nil {
		t.Fatalf("addInternalProtoSlotLocked: %v", err)
	}

	// Now try to apply a runtime binding that conflicts.
	err = g.applyRuntimeBindingsLocked([]ir.ChannelBinding{
		{Pattern: "events.orders.*.update", MessageFQN: "other.v1.Different"},
	})
	if err == nil {
		t.Fatal("expected cross-slot conflict; got nil")
	}
	if !strings.Contains(err.Error(), "events.orders.*.update") {
		t.Errorf("error %q missing conflicting pattern", err.Error())
	}
}

// TestWithChannelBinding_NoPSSlot pins that applying runtime bindings
// when the ps slot doesn't exist (no cluster) returns a clear error.
func TestWithChannelBinding_NoPSSlot(t *testing.T) {
	g := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("test")),
	)
	defer g.Close()

	err := g.applyRuntimeBindingsLocked([]ir.ChannelBinding{
		{Pattern: "events.foo", MessageFQN: "example.Foo"},
	})
	if err == nil {
		t.Fatal("expected error when ps slot missing; got nil")
	}
	if !strings.Contains(err.Error(), "not installed") {
		t.Errorf("error %q doesn't mention missing ps slot", err.Error())
	}
}
