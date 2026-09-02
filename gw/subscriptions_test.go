package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// nopGRPCConn is a stub grpc.ClientConnInterface attached to the
// greeter pool. The subscription path never invokes it (NATS pub/sub
// is the streaming transport); unary calls would fail, but tests
// don't exercise unary on the subscription fixture.
type nopGRPCConn struct{}

func (nopGRPCConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return fmt.Errorf("nopGRPCConn: unary not supported in subscription tests")
}

func (nopGRPCConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("nopGRPCConn: streams not supported")
}

type subFixture struct {
	gw      *Gateway
	cluster *Cluster
	server  *httptest.Server
	wsURL   string
	greeter *fakeGreeterServer
}

func (f *subFixture) close() {
	f.server.Close()
	f.gw.Close()
	f.cluster.Close()
}

// newSubFixture spins an embedded NATS cluster, a real upstream gRPC
// greeter server, registers greeter pointing at it, and exposes
// gw.Handler() over httptest. The honest server-streaming path opens
// a direct gRPC stream to the upstream per subscriber.
//
// opts apply to gateway.New. WithSubscriptionAuth /
// WithoutSubscriptionAuth affect ps.sub auth only; the honest
// proto-streaming path delegates auth to the upstream.
func newSubFixture(t *testing.T, opts ...Option) *subFixture {
	t.Helper()
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

	// Real upstream gRPC server.
	beLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{}
	beSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(beSrv, greeter)
	go func() { _ = beSrv.Serve(beLis) }()
	t.Cleanup(beSrv.Stop)

	allOpts := append([]Option{
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
	}, opts...)
	gw := New(allOpts...)
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(beLis.Addr().String()),
		As("greeter"),
	); err != nil {
		t.Fatalf("AddProtoDescriptor greeter: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/graphql"
	return &subFixture{gw: gw, cluster: cluster, server: srv, wsURL: wsURL, greeter: greeter}
}

// dialWS opens a graphql-transport-ws connection and returns it after
// completing the connection_init / connection_ack handshake.
func dialWS(t *testing.T, ctx context.Context, url string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"graphql-transport-ws"},
	})
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	if err := writeWS(conn, ctx, wsMessage{Type: "connection_init"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	ack, err := readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ack.Type != "connection_ack" {
		t.Fatalf("expected connection_ack, got %s", ack.Type)
	}
	return conn
}

func writeWS(conn *websocket.Conn, ctx context.Context, m wsMessage) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}

func readWS(conn *websocket.Conn, ctx context.Context) (wsMessage, error) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, raw, err := conn.Read(rctx)
	if err != nil {
		return wsMessage{}, err
	}
	var m wsMessage
	return m, json.Unmarshal(raw, &m)
}

// publishGreeting marshals a Greeting and publishes it to subject.
func publishGreeting(t *testing.T, c *Cluster, subject, greeting, forName string) {
	t.Helper()
	g := &greeterv1.Greeting{Greeting: greeting, ForName: forName}
	b, err := proto.Marshal(g)
	if err != nil {
		t.Fatalf("marshal greeting: %v", err)
	}
	if err := c.Conn.Publish(subject, b); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Flush so the gateway's subscriber actually sees it before we read.
	if err := c.Conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestSubscriptionE2E_HappyPath(t *testing.T) {
	f := newSubFixture(t)
	defer f.close()

	// Configure the upstream to stream a specific greeting.
	var sent atomic.Int32
	f.greeter.greetingsFn = func(ctx context.Context, req *greeterv1.GreetingsFilter, stream grpc.ServerStreamingServer[greeterv1.Greeting]) error {
		if err := stream.Send(&greeterv1.Greeting{
			Greeting: "hi " + req.GetName(),
			ForName:  req.GetName(),
		}); err != nil {
			return err
		}
		sent.Add(1)
		// Keep the stream open briefly so the subscriber can receive.
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialWS(t, ctx, f.wsURL)
	defer conn.Close(websocket.StatusNormalClosure, "done")

	subPayload, _ := json.Marshal(subscribePayload{
		Query: `subscription { greeter_greetings(name:"alice") { greeting forName } }`,
	})
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "subscribe", Payload: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	msg, err := readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if msg.Type != "next" || msg.ID != "1" {
		t.Fatalf("unexpected frame: %s id=%s payload=%s", msg.Type, msg.ID, msg.Payload)
	}
	var result struct {
		Data struct {
			GreeterGreetings map[string]any `json:"greeter_greetings"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(msg.Payload, &result); err != nil {
		t.Fatalf("decode payload: %v: %s", err, msg.Payload)
	}
	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors: %v", result.Errors)
	}
	if got := result.Data.GreeterGreetings["greeting"]; got != "hi alice" {
		t.Fatalf("greeting=%v want %q", got, "hi alice")
	}
	if got := result.Data.GreeterGreetings["forName"]; got != "alice" {
		t.Fatalf("forName=%v want %q", got, "alice")
	}
	if got := sent.Load(); got != 1 {
		t.Fatalf("upstream sent=%d want=1", got)
	}
}

func TestSubscriptionE2E_MultipleFrames(t *testing.T) {
	f := newSubFixture(t)
	defer f.close()

	// Upstream sends multiple frames.
	f.greeter.greetingsFn = func(ctx context.Context, req *greeterv1.GreetingsFilter, stream grpc.ServerStreamingServer[greeterv1.Greeting]) error {
		for i := 0; i < 3; i++ {
			if err := stream.Send(&greeterv1.Greeting{
				Greeting: fmt.Sprintf("frame %d for %s", i, req.GetName()),
				ForName:  req.GetName(),
			}); err != nil {
				return err
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialWS(t, ctx, f.wsURL)
	defer conn.Close(websocket.StatusNormalClosure, "done")

	subPayload, _ := json.Marshal(subscribePayload{
		Query: `subscription { greeter_greetings(name:"bob") { greeting } }`,
	})
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "subscribe", Payload: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Receive 3 frames + complete.
	for i := 0; i < 3; i++ {
		msg, err := readWS(conn, ctx)
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if msg.Type != "next" {
			t.Fatalf("frame %d: expected next, got %s payload=%s", i, msg.Type, msg.Payload)
		}
		var result struct {
			Data struct {
				GreeterGreetings map[string]any `json:"greeter_greetings"`
			} `json:"data"`
		}
		if err := json.Unmarshal(msg.Payload, &result); err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		if got := result.Data.GreeterGreetings["greeting"]; got != fmt.Sprintf("frame %d for bob", i) {
			t.Fatalf("frame %d: greeting=%v", i, got)
		}
	}

	// Stream should complete.
	msg, err := readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read complete: %v", err)
	}
	if msg.Type != "complete" {
		t.Fatalf("expected complete, got %s payload=%s", msg.Type, msg.Payload)
	}
}

func TestSubscriptionE2E_UpstreamError(t *testing.T) {
	f := newSubFixture(t)
	defer f.close()

	// Upstream returns an error.
	f.greeter.greetingsFn = func(ctx context.Context, req *greeterv1.GreetingsFilter, stream grpc.ServerStreamingServer[greeterv1.Greeting]) error {
		return fmt.Errorf("upstream failure")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialWS(t, ctx, f.wsURL)
	defer conn.Close(websocket.StatusNormalClosure, "done")

subPayload, _ := json.Marshal(subscribePayload{
		Query: `subscription { greeter_greetings(name:"alice") { greeting } }`,
	})
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "subscribe", Payload: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

// The upstream error should surface as either an error frame or
		// the subscription closing with a complete frame.
		msg, err := readWS(conn, ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if msg.Type != "error" && msg.Type != "complete" {
			t.Fatalf("expected error or complete on upstream failure, got %s payload=%s", msg.Type, msg.Payload)
		}
}

func TestSubscriptionE2E_AdminEventsWatchServices(t *testing.T) {
	// End-to-end test of the admin_events_watchServices Subscription
	// field: register a service, observe a ServiceChange frame on the
	// WS, including round-tripping the proto enum through graphql-go's
	// JSON serialiser (action should land as "ACTION_REGISTERED", not
	// the numeric enum value). Admin events use the internal-proto
	// path with NATS broker, not the honest gRPC stream path.
	f := newSubFixture(t)
	defer f.close()
	if err := f.gw.AddAdminEvents(); err != nil {
		t.Fatalf("AddAdminEvents: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialWS(t, ctx, f.wsURL)
	defer conn.Close(websocket.StatusNormalClosure, "done")

	subPayload, _ := json.Marshal(subscribePayload{
		Query: `subscription {
			admin_events_watchServices(namespace: "watched") {
				action namespace version replicaCount
			}
		}`,
	})
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "subscribe", Payload: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Wait for the broker to register the subject before triggering
	// the publish — otherwise the first event slips past.
	deadline := time.Now().Add(2 * time.Second)
	for f.gw.subscriptionBroker().activeSubjectCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("broker never registered admin_events subject")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Trigger a register on the watched namespace → publishes a
	// ServiceChange to events.admin_events.WatchServices.watched.
	if err := f.gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(nopGRPCConn{}),
		As("watched"),
	); err != nil {
		t.Fatalf("AddProtoDescriptor watched: %v", err)
	}

	msg, err := readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if msg.Type != "next" {
		t.Fatalf("unexpected frame type %s payload=%s", msg.Type, msg.Payload)
	}
	var result struct {
		Data struct {
			AdminEvents map[string]any `json:"admin_events_watchServices"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg.Payload, &result); err != nil {
		t.Fatalf("decode payload: %v: %s", err, msg.Payload)
	}
	change := result.Data.AdminEvents
	if change["action"] != "ACTION_REGISTERED" {
		t.Errorf("action = %v (%T), want %q (graphql-go should serialise the enum NAME, not the number)",
			change["action"], change["action"], "ACTION_REGISTERED")
	}
	if change["namespace"] != "watched" {
		t.Errorf("namespace = %v, want watched", change["namespace"])
	}
	if change["version"] != "v1" {
		t.Errorf("version = %v, want v1", change["version"])
	}
	if n, ok := change["replicaCount"].(float64); !ok || int(n) != 1 {
		t.Errorf("replicaCount = %v (%T), want 1", change["replicaCount"], change["replicaCount"])
	}
}

func TestSubscriptionE2E_ClientCompleteCleansUp(t *testing.T) {
	f := newSubFixture(t)
	defer f.close()

	// Upstream keeps the stream open until context cancels.
	f.greeter.greetingsFn = func(ctx context.Context, req *greeterv1.GreetingsFilter, stream grpc.ServerStreamingServer[greeterv1.Greeting]) error {
		// Send one frame, then wait for context cancellation.
		_ = stream.Send(&greeterv1.Greeting{
			Greeting: "hello " + req.GetName(),
			ForName:  req.GetName(),
		})
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialWS(t, ctx, f.wsURL)
	defer conn.Close(websocket.StatusNormalClosure, "done")

	subPayload, _ := json.Marshal(subscribePayload{
		Query: `subscription { greeter_greetings(name:"alice") { greeting } }`,
	})
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "subscribe", Payload: subPayload}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Receive the first frame to confirm the subscription is active.
	msg, err := readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if msg.Type != "next" {
		t.Fatalf("expected next, got %s payload=%s", msg.Type, msg.Payload)
	}

	// Client says complete; gateway must cancel the upstream stream.
	if err := writeWS(conn, ctx, wsMessage{ID: "1", Type: "complete"}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The subscription should complete cleanly.
	msg, err = readWS(conn, ctx)
	if err != nil {
		t.Fatalf("read complete: %v", err)
	}
	if msg.Type != "complete" {
		t.Fatalf("expected complete, got %s payload=%s", msg.Type, msg.Payload)
	}
}
