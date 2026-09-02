package gateway

import (
	"context"
	"time"

	psav1 "github.com/iodesystems/gwag/v2/gw/proto/pubsubauth/v1"
	"google.golang.org/grpc"
)

// pubsubAuthorizerNamespace is the reserved registration namespace for
// the PubSubAuthorizer delegate. A service implementing the Authorize
// RPC registers under "_pubsub_auth/v1"; the gateway consults it when
// a channel's tier is ChannelAuthDelegate (after HMAC has verified).
const pubsubAuthorizerNamespace = "_pubsub_auth"

// channelDelegateOutcome is what consultPubSubDelegate returns to
// checkChannelAuth.  Mirrors the AdminAuthorizer fall-through policy:
// only an explicit DENIED short-circuits; anything else lets the HMAC
// pass we just made stand on its own.
type channelDelegateOutcome int

const (
	channelDelegateFallthrough channelDelegateOutcome = iota
	channelDelegateAccept
	channelDelegateReject
)

// consultPubSubDelegate calls Authorize on the registered
// PubSubAuthorizer if one is present. The HMAC step has already passed
// when this is invoked; the delegate only narrows further.
func (g *Gateway) consultPubSubDelegate(ctx context.Context, channel, hmacB64 string, ts int64, wildcard bool) (channelDelegateOutcome, string) {
	pool, ok := g.lookupPool(pubsubAuthorizerNamespace, "v1")
	if !ok {
		return channelDelegateFallthrough, ""
	}
	rep := pool.pickReplica()
	if rep == nil {
		return channelDelegateFallthrough, "no live _pubsub_auth/v1 replicas"
	}
	conn, ok := rep.conn.(grpc.ClientConnInterface)
	if !ok {
		return channelDelegateFallthrough, "delegate replica conn not usable"
	}
	remoteAddr := ""
	if r := HTTPRequestFromContext(ctx); r != nil {
		remoteAddr = r.RemoteAddr
	}
	client := psav1.NewPubSubAuthorizerClient(conn)
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := client.Authorize(dctx, &psav1.AuthorizeRequest{
		Channel:    channel,
		Hmac:       hmacB64,
		Ts:         ts,
		Wildcard:   wildcard,
		RemoteAddr: remoteAddr,
	})
	if err != nil {
		return channelDelegateFallthrough, err.Error()
	}
	switch resp.GetCode() {
	case psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_OK:
		return channelDelegateAccept, resp.GetReason()
	case psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_DENIED:
		return channelDelegateReject, resp.GetReason()
	default:
		return channelDelegateFallthrough, resp.GetReason()
	}
}
