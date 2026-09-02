package gateway

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestSubjectMatchesPattern pins NATS-style wildcard matching: `.`
// segments, `*` for one segment, `>` for the rest.
func TestSubjectMatchesPattern(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"events.orders.42.update", "events.orders.42.update", true},
		{"events.orders.42.update", "events.orders.42.create", false},
		{"events.*.update", "events.orders.update", true},
		{"events.*.update", "events.orders.42.update", false},
		{"events.>", "events.orders.42.update", true},
		{"events.>", "events.orders", true},
		{"events.>", "events", false},
		{"events.orders.>", "events.orders.42.update", true},
		{"events.orders.>", "events.orders", false},
		{"events.orders.>", "events.public.foo", false},
		{">", "anything.at.all", true},
		{">", "", false},
	}
	for _, tc := range cases {
		got := subjectMatchesPattern(tc.pattern, tc.subject)
		if got != tc.want {
			t.Errorf("subjectMatchesPattern(%q, %q) = %v, want %v", tc.pattern, tc.subject, got, tc.want)
		}
	}
}

// TestPatternsIntersect pins the wildcard ∩ wildcard relation used
// to decide which WithChannelAuth rules a wildcard Sub reaches.
func TestPatternsIntersect(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"events.>", "events.orders.>", true},
		{"events.>", "events", false},
		{"events.*.update", "events.orders.>", true},
		{"events.public.>", "events.orders.>", false},
		{"events.public.>", "events.>", true},
		{"events.*", "events.orders.42", false},
		{"events.*", "events.orders", true},
		{">", "anything.at.all", true},
		{">", ">", true},
	}
	for _, tc := range cases {
		got := patternsIntersect(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("patternsIntersect(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestPatternCovers pins the superset relation used to decide
// whether a wildcard Sub is fully governed by a single rule (no
// implicit default-HMAC fold-in).
func TestPatternCovers(t *testing.T) {
	cases := []struct {
		auth, sub string
		want      bool
	}{
		{"events.>", "events.orders.>", true},
		{"events.>", "events.orders.42.update", true},
		{"events.orders.>", "events.>", false},
		{"events.public.>", "events.>", false},
		{">", "events.>", true},
		{"events.*", "events.orders", true},
		{"events.*", "events.>", false},
		{"events.*.update", "events.orders.update", true},
		{"events.*.update", "events.orders.>", false},
	}
	for _, tc := range cases {
		got := patternCovers(tc.auth, tc.sub)
		if got != tc.want {
			t.Errorf("patternCovers(%q, %q) = %v, want %v", tc.auth, tc.sub, got, tc.want)
		}
	}
}

// TestResolveChannelTier_Pub covers first-hit-wins on literal Pub
// channels and the default-hmac when no rule matches.
func TestResolveChannelTier_Pub(t *testing.T) {
	g := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.public.>", ChannelAuthOpen),
		WithChannelAuth("events.>", ChannelAuthHMAC),
		WithChannelAuth("audit.>", ChannelAuthDelegate),
	)
	t.Cleanup(g.Close)

	cases := []struct {
		channel string
		want    ChannelAuthTier
	}{
		{"events.public.foo", ChannelAuthOpen},     // first rule
		{"events.orders.42", ChannelAuthHMAC},      // second rule (first didn't match)
		{"audit.write.123", ChannelAuthDelegate},   // third rule
		{"unrelated.thing", ChannelAuthHMAC},       // no match → default
	}
	for _, tc := range cases {
		got := g.resolveChannelTier(tc.channel, false)
		if got != tc.want {
			t.Errorf("resolveChannelTier(%q, false) = %s, want %s", tc.channel, got, tc.want)
		}
	}
}

// TestResolveChannelTier_WildcardSub covers strictest-wins for
// wildcard Subs, including the implicit default-hmac fold-in when
// the requested pattern isn't fully covered by any single rule.
func TestResolveChannelTier_WildcardSub(t *testing.T) {
	g := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithChannelAuth("events.public.>", ChannelAuthOpen),
		WithChannelAuth("events.orders.>", ChannelAuthHMAC),
		WithChannelAuth("audit.>", ChannelAuthDelegate),
	)
	t.Cleanup(g.Close)

	cases := []struct {
		channel string
		want    ChannelAuthTier
		why     string
	}{
		// Fully covered by a single Open rule → Open.
		{"events.public.foo.>", ChannelAuthOpen, "subset of events.public.>"},
		// Fully covered by a single Open rule equal to it → Open.
		{"events.public.>", ChannelAuthOpen, "equals events.public.>"},
		// Single matching rule (hmac) fully covers → HMAC.
		{"events.orders.42.>", ChannelAuthHMAC, "subset of events.orders.>"},
		// Spans two rules at different tiers → strictest (HMAC).
		{"events.>", ChannelAuthHMAC, "spans events.public.> and events.orders.>"},
		// Crosses delegate → Delegate.
		{">", ChannelAuthDelegate, "spans all rules incl audit"},
		// No matching rule → default (HMAC).
		{"foo.>", ChannelAuthHMAC, "no rule intersects"},
		// Intersects with only an Open rule but doesn't fully cover → default-hmac folds in.
		{"events.*", ChannelAuthHMAC, "events.public.> doesn't cover events.* (events.orders is uncovered)"},
	}
	for _, tc := range cases {
		got := g.resolveChannelTier(tc.channel, true)
		if got != tc.want {
			t.Errorf("resolveChannelTier(%q, true) = %s, want %s [%s]", tc.channel, got, tc.want, tc.why)
		}
	}
}

// TestChannelAuth_EndToEnd_HMAC drives ps.pub and ps.sub through the
// hmac tier: requests without a valid token are rejected, requests
// with one round-trip an event. This is the only test that wires the
// full broker + WithSubscriptionAuth secret + sign primitive together.
func TestChannelAuth_EndToEnd_HMAC(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "chauth-test",
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

	secret := []byte("channel-secret-32-bytes-of-padding!!")
	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
		// Default tier is hmac; no explicit rule needed. Adding one
		// to pin behaviour against future default changes.
		WithChannelAuth("events.>", ChannelAuthHMAC),
	)
	t.Cleanup(g.Close)
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	pub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Pub"))
	sub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Sub"))
	if pub == nil || sub == nil {
		t.Fatal("ps dispatchers not registered")
	}

	channel := "events.orders.42"

	// 1. Pub without hmac → reject.
	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": "x",
	}); err == nil {
		t.Fatal("Pub without hmac: want error, got nil")
	} else if !strings.Contains(err.Error(), "hmac") {
		t.Errorf("Pub without hmac: error = %v, want one mentioning hmac", err)
	}

	// 2. Pub with bad hmac → reject.
	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": "x",
		"hmac":    base64.StdEncoding.EncodeToString([]byte("not-the-right-mac")),
		"ts":      int64(time.Now().Unix()),
	}); err == nil {
		t.Fatal("Pub with bad hmac: want error, got nil")
	}

	// 3. Sub with valid hmac → succeeds; Pub with valid hmac → event delivered.
	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)

	subHMAC, subTs := SignSubscribeToken(secret, channel, 60)
	src, err := sub.Dispatch(subCtx, map[string]any{
		"channel": channel,
		"hmac":    subHMAC,
		"ts":      subTs,
	})
	if err != nil {
		t.Fatalf("Sub with valid hmac: %v", err)
	}
	events := src.(chan any)

	deadline := time.Now().Add(2 * time.Second)
	for cluster.Server.NumSubscriptions() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if cluster.Server.NumSubscriptions() == 0 {
		t.Fatal("broker did not bind a NATS subscription within 2s")
	}

	pubHMAC, pubTs := SignSubscribeToken(secret, channel, 60)
	if _, err := pub.Dispatch(context.Background(), map[string]any{
		"channel": channel,
		"payload": "hello",
		"hmac":    pubHMAC,
		"ts":      pubTs,
	}); err != nil {
		t.Fatalf("Pub with valid hmac: %v", err)
	}

	select {
	case raw, ok := <-events:
		if !ok {
			t.Fatal("subscription channel closed before any event arrived")
		}
		ev := raw.(map[string]any)
		if ev["payload"] != "hello" {
			t.Errorf("event.payload = %v, want %q", ev["payload"], "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// TestChannelAuth_EndToEnd_WildcardSubBindsToPattern pins the
// security-critical property: an HMAC minted for a concrete subject
// does NOT satisfy a wildcard Sub on a covering pattern. Subscribers
// sign over the literal string they request.
func TestChannelAuth_EndToEnd_WildcardSubBindsToPattern(t *testing.T) {
	dir := t.TempDir()
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      "chauth-wildcard-test",
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

	secret := []byte("channel-secret-32-bytes-of-padding!!")
	g := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
	)
	t.Cleanup(g.Close)
	g.mu.Lock()
	if err := g.assembleLocked(); err != nil {
		g.mu.Unlock()
		t.Fatalf("assembleLocked: %v", err)
	}
	g.mu.Unlock()

	sub := g.dispatchers.Get(ir.MakeSchemaID(psNamespace, psVersion, "Sub"))

	// Mint a token for "events.orders.42" (a concrete subject) and
	// try to use it on a wildcard sub of "events.orders.>". Should
	// fail because the HMAC payload disagrees.
	concreteHMAC, ts := SignSubscribeToken(secret, "events.orders.42", 60)
	if _, err := sub.Dispatch(context.Background(), map[string]any{
		"channel": "events.orders.>",
		"hmac":    concreteHMAC,
		"ts":      ts,
	}); err == nil {
		t.Fatal("Sub on wildcard with concrete-channel HMAC: want error, got nil")
	}

	// Mint against the wildcard pattern itself — should succeed.
	wildHMAC, ts := SignSubscribeToken(secret, "events.orders.>", 60)
	subCtx, subCancel := context.WithCancel(context.Background())
	t.Cleanup(subCancel)
	if _, err := sub.Dispatch(subCtx, map[string]any{
		"channel": "events.orders.>",
		"hmac":    wildHMAC,
		"ts":      ts,
	}); err != nil {
		t.Errorf("Sub on wildcard with pattern-bound HMAC: %v", err)
	}
}
