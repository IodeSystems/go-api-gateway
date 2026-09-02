package gateway

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// TestVerifySubscribe_LegacyTokenStillVerifies covers the back-compat
// path: only the legacy `Secret` field is set; tokens minted via
// `SignSubscribeToken` (no kid) still verify after the kid plumbing
// landed.
func TestVerifySubscribe_LegacyTokenStillVerifies(t *testing.T) {
	secret := []byte("legacy-key")
	channel := "events.x"
	hmacB64, ts := SignSubscribeToken(secret, channel, 60)

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      hmacB64,
		"timestamp": ts,
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("legacy token: got %s, want OK", got)
	}
}

// TestVerifySubscribe_RotatedKidVerifies covers the rotation happy
// path: a kid is configured in Secrets and the client sends it
// alongside the (kid-bound) token.
func TestVerifySubscribe_RotatedKidVerifies(t *testing.T) {
	secret := []byte("rotated-key")
	kid := "v2"
	channel := "events.x"
	hmacB64, _, ts := SignSubscribeTokenWithKid(secret, kid, channel, 60)

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{kid: secret},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      hmacB64,
		"timestamp": ts,
		"kid":       kid,
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("rotated token: got %s, want OK", got)
	}
}

// TestVerifySubscribe_UnknownKid covers the case where a client sends
// a kid that doesn't appear in Secrets.
func TestVerifySubscribe_UnknownKid(t *testing.T) {
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{"v2": []byte("v2-key")},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	hmacB64, _, ts := SignSubscribeTokenWithKid([]byte("v3-key"), "v3", "events.x", 60)
	got := gw.verifySubscribe("events.x", map[string]any{
		"hmac":      hmacB64,
		"timestamp": ts,
		"kid":       "v3",
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_UNKNOWN_KID {
		t.Errorf("unknown kid: got %s, want UNKNOWN_KID", got)
	}
}

// TestVerifySubscribe_CrossKidReplayFails confirms a token signed
// under one kid can't be replayed by claiming a different kid: the
// HMAC payload includes kid, so changing kid breaks the signature.
func TestVerifySubscribe_CrossKidReplayFails(t *testing.T) {
	keyA := []byte("a-key")
	keyB := []byte("b-key")
	channel := "events.x"

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{"a": keyA, "b": keyB},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	// Sign for kid="a" but try to use kid="b" on the wire.
	hmacB64, _, ts := SignSubscribeTokenWithKid(keyA, "a", channel, 60)
	got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      hmacB64,
		"timestamp": ts,
		"kid":       "b",
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_SIGNATURE_MISMATCH {
		t.Errorf("cross-kid replay: got %s, want SIGNATURE_MISMATCH", got)
	}
}

// TestVerifySubscribe_LegacyAndRotatedCoexist covers the rolling-
// rotation window: the operator has both `Secret` (legacy default
// kid="") and a new `Secrets["v2"]` configured, and tokens signed
// under either form verify.
func TestVerifySubscribe_LegacyAndRotatedCoexist(t *testing.T) {
	legacy := []byte("legacy")
	v2 := []byte("v2-key")
	channel := "events.x"

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secret:  legacy,
			Secrets: map[string][]byte{"v2": v2},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	// Legacy token (no kid).
	hmacOld, tsOld := SignSubscribeToken(legacy, channel, 60)
	if got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      hmacOld,
		"timestamp": tsOld,
	}); got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("legacy token: got %s, want OK", got)
	}

	// Rotated token (kid="v2").
	hmacNew, _, tsNew := SignSubscribeTokenWithKid(v2, "v2", channel, 60)
	if got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      hmacNew,
		"timestamp": tsNew,
		"kid":       "v2",
	}); got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("rotated token: got %s, want OK", got)
	}
}

// TestVerifySubscribe_NoSecretConfigured covers the case where neither
// Secret nor Secrets is set (and Insecure is false).
func TestVerifySubscribe_NoSecretConfigured(t *testing.T) {
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	got := gw.verifySubscribe("events.x", map[string]any{
		"hmac":      "x",
		"timestamp": int64(0),
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_NOT_CONFIGURED {
		t.Errorf("got %s, want NOT_CONFIGURED", got)
	}
}

// TestSignSubscriptionTokenRPC_RotatedKid covers the RPC's new kid
// in/out: caller passes kid in the request, gateway looks it up in
// Secrets, signs the rotated payload, and echoes kid in the response.
// The result must verify through verifySubscribe with the same kid.
func TestSignSubscriptionTokenRPC_RotatedKid(t *testing.T) {
	v2 := []byte("v2-key")
	channel := "events.x"
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{"v2": v2},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	cp := &controlPlane{gw: gw}
	resp, err := cp.SignSubscriptionToken(context.Background(), &cpv1.SignSubscriptionTokenRequest{
		Channel:    channel,
		TtlSeconds: 60,
		Kid:        "v2",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	if resp.GetKid() != "v2" {
		t.Errorf("kid in response = %q, want v2", resp.GetKid())
	}
	// Round-trip through the verifier with the kid the response gave us.
	got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      resp.GetHmac(),
		"timestamp": resp.GetTimestampUnix(),
		"kid":       resp.GetKid(),
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("verify of RPC-signed token: got %s, want OK", got)
	}
}

// TestSignSubscriptionTokenRPC_UnknownKid covers the rejection path:
// the caller asks the RPC to sign with a kid the gateway doesn't have.
func TestSignSubscriptionTokenRPC_UnknownKid(t *testing.T) {
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{"v2": []byte("k")},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	cp := &controlPlane{gw: gw}
	resp, err := cp.SignSubscriptionToken(context.Background(), &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
		Kid:     "v3",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_UNKNOWN_KID {
		t.Errorf("code=%s, want UNKNOWN_KID", resp.GetCode())
	}
	if resp.GetKid() != "v3" {
		t.Errorf("kid echoed = %q, want v3", resp.GetKid())
	}
}

// TestSignSubscriptionTokenRPC_LegacyDefaultKid covers back-compat:
// caller omits kid, gateway uses legacy Secret + legacy payload, and
// the empty-kid token verifies.
func TestSignSubscriptionTokenRPC_LegacyDefaultKid(t *testing.T) {
	secret := []byte("legacy")
	channel := "events.x"
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: secret}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	cp := &controlPlane{gw: gw}
	resp, err := cp.SignSubscriptionToken(context.Background(), &cpv1.SignSubscriptionTokenRequest{
		Channel: channel,
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	if resp.GetKid() != "" {
		t.Errorf("kid = %q, want empty", resp.GetKid())
	}
	got := gw.verifySubscribe(channel, map[string]any{
		"hmac":      resp.GetHmac(),
		"timestamp": resp.GetTimestampUnix(),
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Errorf("verify of legacy-signed token: got %s, want OK", got)
	}
}

// TestVerifySubscribe_KidMalformedType — the kid arg must be a string
// when present. Other types are MALFORMED.
func TestVerifySubscribe_KidMalformedType(t *testing.T) {
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{
			Secrets: map[string][]byte{"v2": []byte("k")},
		}),
		WithAdminToken([]byte("admin")),
	)
	t.Cleanup(gw.Close)

	got := gw.verifySubscribe("events.x", map[string]any{
		"hmac":      base64.StdEncoding.EncodeToString([]byte{1, 2, 3}),
		"timestamp": time.Now().Unix(),
		"kid":       42, // not a string
	})
	if got != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED {
		t.Errorf("got %s, want MALFORMED", got)
	}
}
