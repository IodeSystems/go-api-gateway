package gateway

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	aev1 "github.com/iodesystems/gwag/v2/gw/proto/adminevents/v1"
)

// TestAdminEvents_PublishOnRegister verifies the gateway publishes a
// ServiceChange to NATS when a service registers (and again on
// deregister), and that a directly-attached NATS subscriber decodes
// the event correctly. Exercises the publisher path without needing
// the WebSocket transport or graphql-ws.
func TestAdminEvents_PublishOnRegister(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "test",
		ClientListen:  "127.0.0.1:0",
		ClusterListen: "127.0.0.1:0",
		DataDir:       dir,
		StartTimeout:  10 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	gw := New(WithCluster(cluster), WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)

	if err := gw.AddAdminEvents(); err != nil {
		t.Fatalf("AddAdminEvents: %v", err)
	}

	events := make(chan *aev1.ServiceChange, 8)
	sub, err := cluster.Conn.Subscribe("events.admin_events.WatchServices.*", func(m *nats.Msg) {
		var sc aev1.ServiceChange
		if err := proto.Unmarshal(m.Data, &sc); err != nil {
			t.Errorf("unmarshal: %v", err)
			return
		}
		events <- &sc
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Trigger a register: greeter under "watched/v1".
	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("watched"),
	); err != nil {
		t.Fatalf("AddProtoDescriptor watched: %v", err)
	}

	select {
	case ev := <-events:
		if ev.GetAction() != aev1.ServiceChange_ACTION_REGISTERED {
			t.Errorf("action = %v, want REGISTERED", ev.GetAction())
		}
		if ev.GetNamespace() != "watched" {
			t.Errorf("namespace = %q, want watched", ev.GetNamespace())
		}
		if ev.GetVersion() != "v1" {
			t.Errorf("version = %q, want v1", ev.GetVersion())
		}
		if ev.GetReplicaCount() != 1 {
			t.Errorf("replicaCount = %d, want 1", ev.GetReplicaCount())
		}
		if ev.GetTimestampUnixMs() == 0 {
			t.Errorf("timestamp not set")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no register event received within 5s")
	}

	// Drain replicas → both pools (admin_events and watched) carry
	// boot-time owner="" entries, so removeReplicasByOwnerLocked
	// fires deregister events for both. Map iteration is unordered;
	// scan the channel until we find the "watched" one.
	gw.mu.Lock()
	_, _ = gw.removeReplicasByOwnerLocked("")
	gw.mu.Unlock()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.GetAction() != aev1.ServiceChange_ACTION_DEREGISTERED {
				continue
			}
			if ev.GetNamespace() != "watched" {
				continue
			}
			if ev.GetReplicaCount() != 0 {
				t.Errorf("replicaCount = %d, want 0", ev.GetReplicaCount())
			}
			return
		case <-deadline:
			t.Fatal("no deregister for 'watched' received within 5s")
		}
	}
}

// TestAdminEvents_StandaloneSchemaOnly ensures AddAdminEvents
// succeeds on a cluster-less gateway so SDL-driven codegen consumers
// still see the admin_events_watchServices subscription field. The
// subscription handler itself returns a clean "subscription broker
// not available" error at subscribe time, but that's a runtime
// concern; the schema registration is what makes the SDL useful in
// development / codegen contexts.
func TestAdminEvents_StandaloneSchemaOnly(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)
	if err := gw.AddAdminEvents(); err != nil {
		t.Fatalf("AddAdminEvents on standalone gateway: %v", err)
	}
	if _, err := gw.adminEventsWatchServicesHandler(t.Context(), map[string]any{}); err == nil {
		t.Fatal("expected subscription broker error on standalone gateway")
	}
}
