package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	psv1 "github.com/iodesystems/gwag/v2/gw/proto/ps/v1"
)

// gwag.ps.v1 is the gateway-bundled pub/sub primitive — installed at
// (psNamespace, psVersion) on every gateway via installPubSubSlot.
// Renders as Mutation.ps.pub / Subscription.ps_sub through the existing
// nested-namespace convention. Auth tiers and channel-binding registry
// land in the follow-up commits per docs/plan.md Tier 1 Pub/Sub.
const (
	psNamespace = "ps"
	psVersion   = "v1"
)

// installPubSubSlot registers gwag.ps.v1.PubSub as an internal-proto
// slot wired to the gateway's NATS broker. Idempotent: a same-hash
// re-install (e.g. when New is followed by an explicit AddProto of the
// same proto under (ps, v1)) returns existed=true via registerSlotLocked
// and is a no-op. The slot is installed regardless of whether a cluster
// is configured — the schema is consistent across deployment shapes;
// individual handler calls error with a clear "requires cluster"
// message when no NATS connection is available.
//
// Caller does NOT hold g.mu.
func (g *Gateway) installPubSubSlot() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	handlers := map[string]internalProtoHandler{
		"Pub": g.psPub,
	}
	subHandlers := map[string]internalProtoSubscriptionHandler{
		"Sub": g.psSub,
	}
	return g.addInternalProtoSlotLocked(
		psNamespace, psVersion,
		psv1.File_gw_proto_ps_v1_ps_proto,
		nil,
		handlers,
		subHandlers,
	)
}

// psPub publishes a single Event onto the NATS subject named by the
// request's `channel` field. Wildcards are rejected — they're only
// meaningful at subscribe time. The Event is proto-encoded so
// subscribers' broker fan-out (which decodes against the Event
// descriptor) sees a typed payload.
//
// Auth: the channel's tier is resolved via WithChannelAuth (first-
// hit-wins for literal Pub channels; default is hmac). Open tier
// ignores hmac/ts; hmac/delegate tiers verify the (channel, ts) HMAC
// against WithSubscriptionAuth. The delegate fall-through lands in
// the follow-up commit; for now `delegate+hmac` runs the HMAC path.
func (g *Gateway) psPub(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	if g.cfg.cluster == nil {
		return nil, fmt.Errorf("gateway: ps.pub requires a configured cluster (NATS)")
	}
	m := req.ProtoReflect()
	fields := m.Descriptor().Fields()
	channel := m.Get(fields.ByName("channel")).String()
	payload := m.Get(fields.ByName("payload")).String()
	hmacB64 := m.Get(fields.ByName("hmac")).String()
	ts := m.Get(fields.ByName("ts")).Int()
	if channel == "" {
		return nil, fmt.Errorf("ps.pub: channel required")
	}
	if strings.ContainsAny(channel, "*>") {
		return nil, fmt.Errorf("ps.pub: wildcards not valid on publish (channel=%q)", channel)
	}
	if err := g.checkChannelAuth(ctx, channel, false, hmacB64, ts); err != nil {
		return nil, err
	}
	idx := g.channelBindingIndex.Load()
	payloadType := idx.lookupPayloadType(channel)
	if payloadType == "" {
		g.cfg.metrics.RecordPubNoBinding()
		if g.cfg.psStrictPayloadTypes {
			return nil, Reject(CodeInvalidArgument, fmt.Sprintf("ps.pub: channel %q has no channel binding (strict payload types enabled)", channel))
		}
	}
	if g.cfg.psBindingEnforce {
		if err := idx.validatePayload(channel, payload); err != nil {
			return nil, err
		}
	}
	ev := &psv1.Event{
		Channel:     channel,
		Payload:     payload,
		PayloadType: payloadType,
		Ts:          time.Now().Unix(),
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("ps.pub: marshal event: %w", err)
	}
	if err := g.cfg.cluster.Conn.Publish(channel, b); err != nil {
		return nil, fmt.Errorf("ps.pub: publish: %w", err)
	}
	return &psv1.PubResponse{}, nil
}

// psSub joins the subscription broker for the requested channel (NATS
// wildcards permitted at this layer; the auth tier will eventually
// constrain which channels a token can reach). Returns the broker's
// fan-out chan; the release goroutine fires on ctx cancel so the per-
// subject NATS subscription drops when the last listener leaves.
//
// Stream-cap accounting (per-pool + gateway-wide stream sems used by
// upstream-proto subscribeNATS) does not apply here: there is no
// upstream pool. The gateway-wide cap could be folded in alongside
// auth tiers; deferred until that workstream lands so the broker
// primitive ships in isolation.
func (g *Gateway) psSub(ctx context.Context, args map[string]any) (any, error) {
	channel, _ := args["channel"].(string)
	if channel == "" {
		return nil, fmt.Errorf("ps.sub: channel required")
	}
	hmacB64, _ := args["hmac"].(string)
	ts, _ := asInt64(args["ts"])
	wildcard := strings.ContainsAny(channel, "*>")
	if err := g.checkChannelAuth(ctx, channel, wildcard, hmacB64, ts); err != nil {
		return nil, err
	}
	broker := g.subscriptionBroker()
	if broker == nil {
		return nil, fmt.Errorf("gateway: ps.sub requires a configured cluster (NATS)")
	}
	eventDesc := psv1.File_gw_proto_ps_v1_ps_proto.Messages().ByName("Event")
	source, release, err := broker.acquire(channel, eventDesc)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		release()
	}()
	return source, nil
}
