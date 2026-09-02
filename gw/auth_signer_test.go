package gateway

import (
	"context"
	"encoding/hex"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// signRecorder is a thin metric mock that captures RecordSignAuth
// outcomes — the rest of the Metrics surface goes through noopMetrics.
type signRecorder struct {
	noopMetrics
	mu     sync.Mutex
	codes  []string
}

func (s *signRecorder) RecordSignAuth(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes = append(s.codes, code)
}

func (s *signRecorder) outcomes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.codes))
	copy(out, s.codes)
	return out
}

// fakePeerCtx returns ctx with a synthetic gRPC peer attached, so
// checkSignAuth treats the call as a wire call (the gate fires).
// Without this, peer.FromContext returns ok=false and the gate is
// bypassed as in-process.
func fakePeerCtx(ctx context.Context) context.Context {
	return peer.NewContext(ctx, &peer.Peer{
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
	})
}

func newSignerGW(t *testing.T, signer, admin []byte, m Metrics) *Gateway {
	t.Helper()
	opts := []Option{
		WithoutBackpressure(),
		WithSubscriptionAuth(SubscriptionAuthOptions{Secret: []byte("k")}),
	}
	if signer != nil {
		opts = append(opts, WithSignerSecret(signer))
	}
	if admin != nil {
		opts = append(opts, WithAdminToken(admin))
	}
	if m != nil {
		opts = append(opts, WithMetrics(m))
	} else {
		opts = append(opts, WithoutMetrics())
	}
	gw := New(opts...)
	t.Cleanup(gw.Close)
	return gw
}

// TestSignAuth_InProcessBypass — no gRPC peer in ctx → gate is
// bypassed regardless of bearer presence. This is the path tests +
// the huma /admin/sign handler take.
func TestSignAuth_InProcessBypass(t *testing.T) {
	rec := &signRecorder{}
	gw := newSignerGW(t, []byte("signer"), []byte("admin"), rec)
	cp := &controlPlane{gw: gw}
	resp, err := cp.SignSubscriptionToken(context.Background(), &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthInProcess {
		t.Fatalf("outcomes=%v, want [in_process]", got)
	}
}

// TestSignAuth_WireRequiresBearer — peer attached, no bearer → DENIED
// + missing_bearer outcome.
func TestSignAuth_WireRequiresBearer(t *testing.T) {
	rec := &signRecorder{}
	gw := newSignerGW(t, []byte("signer"), []byte("admin"), rec)
	cp := &controlPlane{gw: gw}
	ctx := fakePeerCtx(context.Background())
	resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_DENIED {
		t.Fatalf("code=%s, want DENIED", resp.GetCode())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthMissingBearer {
		t.Fatalf("outcomes=%v, want [missing_bearer]", got)
	}
}

// TestSignAuth_SignerSecretAccepts — peer attached + signer-secret
// presented → ok_signer + token issued.
func TestSignAuth_SignerSecretAccepts(t *testing.T) {
	signer := []byte{0x01, 0x02, 0x03, 0x04}
	rec := &signRecorder{}
	gw := newSignerGW(t, signer, []byte("admin"), rec)
	cp := &controlPlane{gw: gw}
	md := metadata.New(map[string]string{
		"authorization": "Bearer " + hex.EncodeToString(signer),
	})
	ctx := metadata.NewIncomingContext(fakePeerCtx(context.Background()), md)
	resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthOKSigner {
		t.Fatalf("outcomes=%v, want [ok_signer]", got)
	}
}

// TestSignAuth_AdminTokenFallback — peer attached + admin token
// presented → ok_bearer (admin is the always-works fallback even when
// signer-secret is also set).
func TestSignAuth_AdminTokenFallback(t *testing.T) {
	admin := []byte{0xaa, 0xbb, 0xcc}
	rec := &signRecorder{}
	gw := newSignerGW(t, []byte("signer"), admin, rec)
	cp := &controlPlane{gw: gw}
	md := metadata.New(map[string]string{
		"authorization": "Bearer " + hex.EncodeToString(admin),
	})
	ctx := metadata.NewIncomingContext(fakePeerCtx(context.Background()), md)
	resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthOKBearer {
		t.Fatalf("outcomes=%v, want [ok_bearer]", got)
	}
}

// TestSignAuth_WrongBearerDenied — peer attached + wrong bearer →
// DENIED + denied_bearer outcome.
func TestSignAuth_WrongBearerDenied(t *testing.T) {
	rec := &signRecorder{}
	gw := newSignerGW(t, []byte("signer"), []byte("admin"), rec)
	cp := &controlPlane{gw: gw}
	md := metadata.New(map[string]string{
		"authorization": "Bearer " + hex.EncodeToString([]byte("wrong")),
	})
	ctx := metadata.NewIncomingContext(fakePeerCtx(context.Background()), md)
	resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_DENIED {
		t.Fatalf("code=%s, want DENIED", resp.GetCode())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthDeniedBearer {
		t.Fatalf("outcomes=%v, want [denied_bearer]", got)
	}
}

// TestSignAuth_NoSignerOnlyAdmin — WithSignerSecret unset; admin
// token still works as the bearer (back-compat for operators who
// don't opt into signer rotation).
func TestSignAuth_NoSignerOnlyAdmin(t *testing.T) {
	admin := []byte{0x42}
	rec := &signRecorder{}
	gw := newSignerGW(t, nil, admin, rec)
	cp := &controlPlane{gw: gw}
	md := metadata.New(map[string]string{
		"authorization": "Bearer " + hex.EncodeToString(admin),
	})
	ctx := metadata.NewIncomingContext(fakePeerCtx(context.Background()), md)
	resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel: "events.x",
	})
	if err != nil {
		t.Fatalf("SignSubscriptionToken: %v", err)
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		t.Fatalf("code=%s reason=%s", resp.GetCode(), resp.GetReason())
	}
	got := rec.outcomes()
	if len(got) != 1 || got[0] != signAuthOKBearer {
		t.Fatalf("outcomes=%v, want [ok_bearer]", got)
	}
}
