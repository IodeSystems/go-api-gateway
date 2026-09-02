package gateway

import (
	"context"
	"fmt"
	"strings"
	"testing"

	psav1 "github.com/iodesystems/gwag/v2/gw/proto/pubsubauth/v1"
	"google.golang.org/grpc"
)

// fakePubSubAuthConn is a grpc.ClientConnInterface that intercepts the
// PubSubAuthorizer.Authorize unary call and runs handler in-process.
// Anything else is unsupported.
type fakePubSubAuthConn struct {
	handler   func(*psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error)
	lastReq   *psav1.AuthorizeRequest
	callCount int
}

func (f *fakePubSubAuthConn) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if method != psav1.PubSubAuthorizer_Authorize_FullMethodName {
		return fmt.Errorf("fakePubSubAuthConn: unsupported method %s", method)
	}
	in := args.(*psav1.AuthorizeRequest)
	f.lastReq = in
	f.callCount++
	resp, err := f.handler(in)
	if err != nil {
		return err
	}
	out := reply.(*psav1.AuthorizeResponse)
	out.Code = resp.Code
	out.Reason = resp.Reason
	return nil
}

func (f *fakePubSubAuthConn) NewStream(_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("fakePubSubAuthConn: streams not supported")
}

// withPubSubDelegate registers the fake pubsub authorizer at
// "_pubsub_auth/v1" so subsequent checkChannelAuth calls hit it.
func withPubSubDelegate(t *testing.T, gw *Gateway, fake *fakePubSubAuthConn) {
	t.Helper()
	if err := gw.AddProtoBytes("pubsubauth.proto", testProtoBytes(t, "pubsubauth.proto"),
		To(fake),
		As(pubsubAuthorizerNamespace),
		AsInternal(),
	); err != nil {
		t.Fatalf("register delegate: %v", err)
	}
}

// newDelegateGateway is a checkChannelAuth-only gateway: HMAC secret +
// a single Delegate-tier rule. No cluster — pubsub round-trip isn't
// what we're exercising here.
func newDelegateGateway(t *testing.T, pattern string) (*Gateway, []byte) {
	t.Helper()
	secret := []byte("channel-secret-32-bytes-of-padding!!")
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
		WithChannelAuth(pattern, ChannelAuthDelegate),
	)
	t.Cleanup(gw.Close)
	return gw, secret
}

// signedAuthCall mints a valid HMAC for `channel` and runs
// checkChannelAuth against it. The delegate (if any) sees an already-
// HMAC-verified call — exactly the runtime ordering.
func signedAuthCall(t *testing.T, gw *Gateway, secret []byte, channel string, wildcard bool) error {
	t.Helper()
	hmacB64, ts := SignSubscribeToken(secret, channel, 60)
	return gw.checkChannelAuth(context.Background(), channel, wildcard, hmacB64, ts)
}

func TestPubSubDelegate_NoDelegate_HMACOnlyStillWorks(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	// No delegate registered — HMAC-passed call must succeed because
	// the delegate path is opt-in; "no delegate" never locks operators
	// out of an otherwise-valid HMAC token.
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("no-delegate Pub: %v", err)
	}
}

func TestPubSubDelegate_OK_Accepts(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	fake := &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_OK}, nil
		},
	}
	withPubSubDelegate(t, gw, fake)
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("OK delegate: %v", err)
	}
	if fake.callCount != 1 {
		t.Fatalf("delegate call count = %d, want 1", fake.callCount)
	}
}

func TestPubSubDelegate_DENIED_Rejects(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	withPubSubDelegate(t, gw, &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_DENIED, Reason: "policy"}, nil
		},
	})
	err := signedAuthCall(t, gw, secret, "events.orders.42", false)
	if err == nil {
		t.Fatal("DENIED delegate: want error, got nil")
	}
	if !strings.Contains(err.Error(), "denied by delegate") || !strings.Contains(err.Error(), "policy") {
		t.Errorf("error = %v, want one mentioning denied + reason", err)
	}
}

func TestPubSubDelegate_UNAVAILABLE_FallsThrough(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	withPubSubDelegate(t, gw, &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_UNAVAILABLE}, nil
		},
	})
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("UNAVAILABLE fall-through: %v", err)
	}
}

func TestPubSubDelegate_NOT_CONFIGURED_FallsThrough(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	withPubSubDelegate(t, gw, &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_NOT_CONFIGURED}, nil
		},
	})
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("NOT_CONFIGURED fall-through: %v", err)
	}
}

func TestPubSubDelegate_UNSPECIFIED_FallsThrough(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	withPubSubDelegate(t, gw, &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_UNSPECIFIED}, nil
		},
	})
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("UNSPECIFIED fall-through: %v", err)
	}
}

func TestPubSubDelegate_TransportError_FallsThrough(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	withPubSubDelegate(t, gw, &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return nil, fmt.Errorf("simulated network failure")
		},
	})
	// Transport failure must NOT lock operators out — HMAC already
	// passed, so the call proceeds.
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("transport-err fall-through: %v", err)
	}
}

func TestPubSubDelegate_HMACFailure_DelegateNotCalled(t *testing.T) {
	gw, _ := newDelegateGateway(t, "events.>")
	fake := &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_OK}, nil
		},
	}
	withPubSubDelegate(t, gw, fake)
	// No HMAC at all → fail before reaching the delegate.
	if err := gw.checkChannelAuth(context.Background(), "events.orders.42", false, "", 0); err == nil {
		t.Fatal("no-hmac call: want error, got nil")
	}
	if fake.callCount != 0 {
		t.Fatalf("delegate must not be consulted on HMAC failure; calls=%d", fake.callCount)
	}
}

func TestPubSubDelegate_RequestFieldsForwarded(t *testing.T) {
	gw, secret := newDelegateGateway(t, "events.>")
	fake := &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_OK}, nil
		},
	}
	withPubSubDelegate(t, gw, fake)
	// Wildcard sub: token bound to the wildcard pattern (the security
	// property pinned in auth_channel_test.go).
	channel := "events.orders.>"
	hmacB64, ts := SignSubscribeToken(secret, channel, 60)
	if err := gw.checkChannelAuth(context.Background(), channel, true, hmacB64, ts); err != nil {
		t.Fatalf("wildcard sub: %v", err)
	}
	req := fake.lastReq
	if got, want := req.GetChannel(), channel; got != want {
		t.Errorf("channel = %q, want %q", got, want)
	}
	if !req.GetWildcard() {
		t.Errorf("wildcard = false, want true")
	}
	if got, want := req.GetHmac(), hmacB64; got != want {
		t.Errorf("hmac forwarded = %q, want %q", got, want)
	}
	if got, want := req.GetTs(), ts; got != want {
		t.Errorf("ts forwarded = %d, want %d", got, want)
	}
}

func TestPubSubDelegate_HMACTierUnaffected(t *testing.T) {
	// HMAC-tier channels must not call the delegate even if one is
	// registered — only Delegate-tier triggers it.
	secret := []byte("channel-secret-32-bytes-of-padding!!")
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
		WithChannelAuth("events.>", ChannelAuthHMAC),
	)
	t.Cleanup(gw.Close)
	fake := &fakePubSubAuthConn{
		handler: func(_ *psav1.AuthorizeRequest) (*psav1.AuthorizeResponse, error) {
			return &psav1.AuthorizeResponse{Code: psav1.PubSubAuthCode_PUBSUB_AUTH_CODE_DENIED}, nil
		},
	}
	withPubSubDelegate(t, gw, fake)
	if err := signedAuthCall(t, gw, secret, "events.orders.42", false); err != nil {
		t.Fatalf("HMAC-tier with delegate registered: %v", err)
	}
	if fake.callCount != 0 {
		t.Fatalf("delegate must not be consulted for HMAC-tier; calls=%d", fake.callCount)
	}
}
