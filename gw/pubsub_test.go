package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
	psv1 "github.com/iodesystems/gwag/v2/gw/proto/ps/v1"
)

// TestPubSub_RoundTrip exercises the gateway-bundled gwag.ps.v1.PubSub
// dispatchers end-to-end against an embedded NATS cluster: subscribe
// via ps.sub, publish via ps.pub, and verify the subscriber receives
// the Event payload. Pre-empts the auth-tier work — this iteration
// ships the broker primitive only; tier policy lands in commit 2 of
// the workstream.
func TestPubSub_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	// Open tier on the test channel space; the round-trip test focuses
	// on broker behaviour, not auth — the hmac tier has dedicated
	// coverage in auth_channel_test.go.
	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
	)
	t.Cleanup(g.Close)

	// Trigger a schema assemble so dispatchers are registered.
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	pubSID := ir.MakeSchemaID(psNamespace, psVersion, "Pub")
	subSID := ir.MakeSchemaID(psNamespace, psVersion, "Sub")
	pub := g.dispatchers.Get(pubSID)
	if pub == nil {
		t.Fatalf("no Pub dispatcher registered under %s", pubSID)
	}
	sub := g.dispatchers.Get(subSID)
	if sub == nil {
		t.Fatalf("no Sub dispatcher registered under %s", subSID)
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)

	channel := "events.test.42"
	src, err := sub.Dispatch(subCtx, map[string]any{"channel": channel})
	if err != nil {
		t.Fatalf("Sub dispatch: %v", err)
	}
	events, ok := src.(chan any)
	if !ok {
		t.Fatalf("Sub returned %T, want chan any", src)
	}

	// nats.Subscribe is asynchronous — give the broker a beat to bind
	// the per-subject NATS subscription before the publish lands.
	// Without this the Pub fires into a void and the test hangs.
	deadline := time.Now().Add(2 * time.Second)
	for cluster.Server.NumSubscriptions() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if cluster.Server.NumSubscriptions() == 0 {
		t.Fatal("broker did not bind a NATS subscription within 2s")
	}

	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": "hello world",
	}); err != nil {
		t.Fatalf("Pub dispatch: %v", err)
	}

	select {
	case raw, ok := <-events:
		if !ok {
			t.Fatal("subscription channel closed before any event arrived")
		}
		ev, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("event %T, want map[string]any", raw)
		}
		if got := ev["channel"]; got != channel {
			t.Errorf("event.channel = %v, want %q", got, channel)
		}
		if got := ev["payload"]; got != "hello world" {
			t.Errorf("event.payload = %v, want %q", got, "hello world")
		}
		// Event.ts is wall-clock at Pub entry — int64 stringified by
		// messageToMap to preserve precision over the JSON wire.
		ts, ok := ev["ts"].(string)
		if !ok {
			t.Fatalf("event.ts %T = %v, want string", ev["ts"], ev["ts"])
		}
		if ts == "" || ts == "0" {
			t.Errorf("event.ts = %q, want > 0", ts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// TestPubSub_RejectsWildcardOnPublish pins the publish-side validation
// — wildcards belong to subscribe.
func TestPubSub_RejectsWildcardOnPublish(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	for _, ch := range []string{"events.*", "events.>", "events.foo.>.bar"} {
		if _, err := pub.Dispatch(context.Background(), map[string]any{
			"channel": ch,
			"payload": "x",
		}); err == nil {
			t.Errorf("Pub on wildcard channel %q: want error, got nil", ch)
		}
	}
}

// TestPubSub_NoClusterSkipsInstall pins the boot-time gate: a
// standalone gateway (no WithCluster) does not register the ps/v1
// slot, so ps.pub / ps.sub do not appear in the schema. This matches
// the proto-streaming Subscription path which also requires a cluster
// to function — keeping the public surface tied to deployment
// capability instead of advertising fields that always error.
func TestPubSub_NoClusterSkipsInstall(t *testing.T) {
	g := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	_, ok := g.slots[poolKey{namespace: psNamespace, version: psVersion}]
	g.mu.Unlock()
	if ok {
		t.Fatal("ps/v1 slot was installed without cluster; expected to be skipped")
	}
	if d := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub")); d != nil {
		t.Error("Pub dispatcher registered without cluster")
	}
	if d := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Sub")); d != nil {
		t.Error("Sub dispatcher registered without cluster")
	}
}

// TestPubSub_PayloadTypeStamped pins that ps.pub stamps
// Event.payload_type with the matching binding's MessageFQN when a
// channel binding registry entry covers the publish channel.
func TestPubSub_PayloadTypeStamped(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
	)
	t.Cleanup(g.Close)

	// Manually install a binding index entry so ps.pub has something
	// to look up. In production this comes from a slot's channelBindings
	// aggregated by rebuildChannelBindingIndexLocked.
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	// Store binding index after assembleLocked (which rebuilds from
	// slots; no slot here has bindings, so assemble would clear it).
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "example.events.v1.OrderUpdate", namespace: "orders", version: "v1"},
		},
	})
	g.mu.Unlock()

	pubSID := ir.MakeSchemaID(psNamespace, psVersion, "Pub")
	pub := g.dispatchers.Get(pubSID)
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	subSID := ir.MakeSchemaID(psNamespace, psVersion, "Sub")
	sub := g.dispatchers.Get(subSID)
	if sub == nil {
		t.Fatal("no Sub dispatcher")
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)

	channel := "events.orders.42.update"
	src, err := sub.Dispatch(subCtx, map[string]any{"channel": channel})
	if err != nil {
		t.Fatalf("Sub dispatch: %v", err)
	}
	events, ok := src.(chan any)
	if !ok {
		t.Fatalf("Sub returned %T, want chan any", src)
	}

	deadline := time.Now().Add(2 * time.Second)
	for cluster.Server.NumSubscriptions() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": `{"order_id":"42"}`,
	}); err != nil {
		t.Fatalf("Pub dispatch: %v", err)
	}

	select {
	case raw := <-events:
		ev, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("event %T, want map[string]any", raw)
		}
		if got := ev["payloadType"]; got != "example.events.v1.OrderUpdate" {
			t.Errorf("event.payloadType = %v, want example.events.v1.OrderUpdate", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// TestPubSub_PayloadTypeEmpty_NoBinding pins that when no binding
// matches the channel, payloadType is delivered empty (proto3 default
// string "" is not serialized by messageToMap, so the key is absent
// from the resulting map).
func TestPubSub_PayloadTypeEmpty_NoBinding(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	pubSID := ir.MakeSchemaID(psNamespace, psVersion, "Pub")
	pub := g.dispatchers.Get(pubSID)
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	subSID := ir.MakeSchemaID(psNamespace, psVersion, "Sub")
	sub := g.dispatchers.Get(subSID)
	if sub == nil {
		t.Fatal("no Sub dispatcher")
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)

	channel := "events.unknown.foo"
	src, err := sub.Dispatch(subCtx, map[string]any{"channel": channel})
	if err != nil {
		t.Fatalf("Sub dispatch: %v", err)
	}
	events, ok := src.(chan any)
	if !ok {
		t.Fatalf("Sub returned %T, want chan any", src)
	}

	deadline := time.Now().Add(2 * time.Second)
	for cluster.Server.NumSubscriptions() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": "test",
	}); err != nil {
		t.Fatalf("Pub dispatch: %v", err)
	}

	select {
	case raw := <-events:
		ev, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("event %T, want map[string]any", raw)
		}
		// proto3 default string "" is not serialized by messageToMap,
		// so the key is absent (nil) when no binding matches.
		if got := ev["payloadType"]; got != nil {
			t.Errorf("event.payloadType = %v, want nil (no matching binding)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// TestPubSub_StrictPayloadTypes_RejectsUnboundChannel pins that
// WithStrictPayloadTypes rejects publishes to channels with no
// matching channel binding.
func TestPubSub_StrictPayloadTypes_RejectsUnboundChannel(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
		WithStrictPayloadTypes(),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "example.events.v1.OrderUpdate", namespace: "orders", version: "v1"},
		},
	})
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	// Channel with no binding should be rejected.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.unknown.foo",
		"payload": "test",
	})
	if err == nil {
		t.Fatal("expected error for unbound channel with strict payload types; got nil")
	}
	if !containsStr(err.Error(), "no channel binding") {
		t.Errorf("error %q doesn't mention missing binding", err.Error())
	}

	// Channel WITH a binding should still succeed (no shape enforcement
	// since WithChannelBindingEnforce is not set).
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": "anything goes",
	})
	if err != nil {
		t.Fatalf("Pub on bound channel should succeed without shape enforcement: %v", err)
	}
}

// TestPubSub_ChannelBindingEnforce_ValidPayload pins that with
// WithChannelBindingEnforce, a valid JSON payload matching the bound
// proto message type is accepted.
func TestPubSub_ChannelBindingEnforce_ValidPayload(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
		WithChannelBindingEnforce(),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	// Use the ps.v1.Event message descriptor as the bound type — it has
	// fields: channel(string), payload(string), payload_type(string), ts(int64).
	eventDesc := psv1.File_gw_proto_ps_v1_ps_proto.Messages().ByName("Event")
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "gwag.ps.v1.Event", namespace: "ps", version: "v1", messageDesc: eventDesc},
		},
	})
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	// Valid JSON matching the Event shape should succeed.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": `{"channel":"events.orders.42.update","payload":"order data"}`,
	})
	if err != nil {
		t.Fatalf("Pub with valid payload: %v", err)
	}
}

// TestPubSub_ChannelBindingEnforce_InvalidPayload pins that with
// WithChannelBindingEnforce, a payload that doesn't match the bound
// proto message type is rejected with InvalidArgument.
func TestPubSub_ChannelBindingEnforce_InvalidPayload(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
		WithChannelBindingEnforce(),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	eventDesc := psv1.File_gw_proto_ps_v1_ps_proto.Messages().ByName("Event")
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "gwag.ps.v1.Event", namespace: "ps", version: "v1", messageDesc: eventDesc},
		},
	})
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	// Invalid JSON should be rejected.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": "not json at all",
	})
	if err == nil {
		t.Fatal("expected error for invalid payload; got nil")
	}
	if !containsStr(err.Error(), "does not match bound type") {
		t.Errorf("error %q doesn't mention type mismatch", err.Error())
	}

	// JSON with wrong field types (ts expects int64, not string) should also reject.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": `{"channel":"x","ts":"notanumber"}`,
	})
	if err == nil {
		t.Fatal("expected error for wrong field types; got nil")
	}
}

// TestPubSub_ChannelBindingEnforce_NoDescriptor pins that enforcement
// is a no-op when the binding has no resolved message descriptor
// (e.g. runtime binding with FQN that doesn't match any registered
// proto slot). The publish should succeed without validation.
func TestPubSub_ChannelBindingEnforce_NoDescriptor(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
		WithChannelBindingEnforce(),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	// Binding with no messageDesc — simulates a runtime binding whose
	// FQN doesn't resolve to any registered proto slot.
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "unresolved.v1.Foo", namespace: "orders", version: "v1"},
		},
	})
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	// Should succeed — no descriptor to validate against.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": "anything",
	})
	if err != nil {
		t.Fatalf("Pub should succeed when binding has no descriptor: %v", err)
	}
}

// TestPubSub_BothStrictnessKnobs pins that both knobs can be enabled
// together and interact correctly: strict payload types rejects
// unbound channels, and binding enforce validates shape on bound ones.
func TestPubSub_BothStrictnessKnobs(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "pubsub-test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.>", ChannelAuthOpen),
		WithChannelBindingEnforce(),
		WithStrictPayloadTypes(),
	)
	t.Cleanup(g.Close)

	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	eventDesc := psv1.File_gw_proto_ps_v1_ps_proto.Messages().ByName("Event")
	g.channelBindingIndex.Store(&channelBindingIndex{
		entries: []channelBindingEntry{
			{pattern: "events.orders.*.update", messageFQN: "gwag.ps.v1.Event", namespace: "ps", version: "v1", messageDesc: eventDesc},
		},
	})
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	if pub == nil {
		t.Fatal("no Pub dispatcher")
	}

	// Unbound channel rejected by strict payload types.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.unknown.foo",
		"payload": "test",
	})
	if err == nil {
		t.Fatal("expected error for unbound channel; got nil")
	}

	// Bound channel with invalid payload rejected by binding enforce.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": "not json",
	})
	if err == nil {
		t.Fatal("expected error for invalid payload; got nil")
	}

	// Bound channel with valid payload succeeds.
	_, err = pub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.42.update",
		"payload": `{"channel":"x","payload":"y"}`,
	})
	if err != nil {
		t.Fatalf("Pub with valid payload on bound channel: %v", err)
	}
}

func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}
