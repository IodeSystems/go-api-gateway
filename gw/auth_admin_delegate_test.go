package gateway

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	aav1 "github.com/iodesystems/gwag/v2/gw/proto/adminauth/v1"
	"google.golang.org/grpc"
)

// fakeAdminAuthConn is a grpc.ClientConnInterface that intercepts the
// AdminAuthorizer.Authorize unary call and runs handler in-process.
// Anything else is unsupported.
type fakeAdminAuthConn struct {
	handler  func(*aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error)
	lastReq  *aav1.AuthorizeRequest
	callCount int
}

func (f *fakeAdminAuthConn) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if method != aav1.AdminAuthorizer_Authorize_FullMethodName {
		return fmt.Errorf("fakeAdminAuthConn: unsupported method %s", method)
	}
	in := args.(*aav1.AuthorizeRequest)
	f.lastReq = in
	f.callCount++
	resp, err := f.handler(in)
	if err != nil {
		return err
	}
	out := reply.(*aav1.AuthorizeResponse)
	out.Code = resp.Code
	out.Reason = resp.Reason
	return nil
}

func (f *fakeAdminAuthConn) NewStream(_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("fakeAdminAuthConn: streams not supported")
}

// withDelegate registers the fake admin authorizer at "_admin_auth/v1"
// as an internal service so subsequent middleware calls hit it.
func withDelegate(t *testing.T, gw *Gateway, fake *fakeAdminAuthConn) {
	t.Helper()
	if err := gw.AddProtoBytes("adminauth.proto", testProtoBytes(t, "adminauth.proto"),
		To(fake),
		As(adminAuthorizerNamespace),
		AsInternal(),
	); err != nil {
		t.Fatalf("register delegate: %v", err)
	}
}

func newAdminAuthGateway(t *testing.T, tok []byte) *Gateway {
	t.Helper()
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken(tok))
	t.Cleanup(gw.Close)
	return gw
}

func runAdminWrite(t *testing.T, gw *Gateway, header string) (status int, sawAuth bool) {
	t.Helper()
	h := gw.AdminMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = isAdminAuth(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/peers/x/forget", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code, sawAuth
}

func TestAdminDelegate_NoDelegate_BootTokenStillWorks(t *testing.T) {
	tok := []byte("supersecret")
	gw := newAdminAuthGateway(t, tok)

	// No delegate registered → fall through to boot token.
	status, authed := runAdminWrite(t, gw, "Bearer "+hex.EncodeToString(tok))
	if status != 200 || !authed {
		t.Fatalf("status=%d authed=%v want 200 true", status, authed)
	}

	// Wrong token → 401.
	status, authed = runAdminWrite(t, gw, "Bearer deadbeef")
	if status != 401 || authed {
		t.Fatalf("wrong token: status=%d authed=%v want 401 false", status, authed)
	}
}

func TestAdminDelegate_OK_AcceptsWithoutBootToken(t *testing.T) {
	gw := newAdminAuthGateway(t, []byte("ignored"))
	fake := &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			return &aav1.AuthorizeResponse{Code: aav1.AdminAuthCode_ADMIN_AUTH_CODE_OK}, nil
		},
	}
	withDelegate(t, gw, fake)

	// No bearer at all — delegate accepts.
	status, authed := runAdminWrite(t, gw, "")
	if status != 200 || !authed {
		t.Fatalf("status=%d authed=%v want 200 true", status, authed)
	}
	if fake.callCount != 1 {
		t.Fatalf("delegate call count = %d, want 1", fake.callCount)
	}
}

func TestAdminDelegate_DENIED_RejectsRegardlessOfBootToken(t *testing.T) {
	tok := []byte("supersecret")
	gw := newAdminAuthGateway(t, tok)
	withDelegate(t, gw, &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			return &aav1.AuthorizeResponse{Code: aav1.AdminAuthCode_ADMIN_AUTH_CODE_DENIED, Reason: "policy"}, nil
		},
	})

	// Even with the correct boot token, DENIED short-circuits to 401.
	status, authed := runAdminWrite(t, gw, "Bearer "+hex.EncodeToString(tok))
	if status != 401 || authed {
		t.Fatalf("status=%d authed=%v want 401 false", status, authed)
	}
}

func TestAdminDelegate_UNAVAILABLE_FallsThroughToBootToken(t *testing.T) {
	tok := []byte("supersecret")
	gw := newAdminAuthGateway(t, tok)
	withDelegate(t, gw, &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			return &aav1.AuthorizeResponse{Code: aav1.AdminAuthCode_ADMIN_AUTH_CODE_UNAVAILABLE}, nil
		},
	})

	// Boot token present → fallthrough accepts.
	status, authed := runAdminWrite(t, gw, "Bearer "+hex.EncodeToString(tok))
	if status != 200 || !authed {
		t.Fatalf("status=%d authed=%v want 200 true", status, authed)
	}

	// Boot token absent → fallthrough still rejects.
	status, _ = runAdminWrite(t, gw, "")
	if status != 401 {
		t.Fatalf("no token: status=%d want 401", status)
	}
}

func TestAdminDelegate_TransportError_FallsThrough(t *testing.T) {
	tok := []byte("supersecret")
	gw := newAdminAuthGateway(t, tok)
	withDelegate(t, gw, &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			return nil, fmt.Errorf("simulated network failure")
		},
	})

	// Transport failure must NOT lock operators out.
	status, authed := runAdminWrite(t, gw, "Bearer "+hex.EncodeToString(tok))
	if status != 200 || !authed {
		t.Fatalf("status=%d authed=%v want 200 true (boot token always works)", status, authed)
	}
}

func TestAdminDelegate_ReadsStillPublic(t *testing.T) {
	gw := newAdminAuthGateway(t, []byte("supersecret"))
	called := false
	withDelegate(t, gw, &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			called = true
			return &aav1.AuthorizeResponse{Code: aav1.AdminAuthCode_ADMIN_AUTH_CODE_DENIED}, nil
		},
	})
	h := gw.AdminMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/admin/peers", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200", rr.Code)
	}
	if called {
		t.Fatalf("delegate must not be called for public reads")
	}
}

func TestAdminDelegate_RequestFieldsForwarded(t *testing.T) {
	gw := newAdminAuthGateway(t, []byte("ignored"))
	fake := &fakeAdminAuthConn{
		handler: func(_ *aav1.AuthorizeRequest) (*aav1.AuthorizeResponse, error) {
			return &aav1.AuthorizeResponse{Code: aav1.AdminAuthCode_ADMIN_AUTH_CODE_OK}, nil
		},
	}
	withDelegate(t, gw, fake)
	_, _ = runAdminWrite(t, gw, "Bearer abc123")
	if got, want := fake.lastReq.GetToken(), "abc123"; got != want {
		t.Errorf("token = %q want %q", got, want)
	}
	if got, want := fake.lastReq.GetMethod(), "POST"; got != want {
		t.Errorf("method = %q want %q", got, want)
	}
	if got, want := fake.lastReq.GetPath(), "/admin/peers/x/forget"; got != want {
		t.Errorf("path = %q want %q", got, want)
	}
}

func TestBearerFromRequest(t *testing.T) {
	cases := []struct {
		header string
		want   string
	}{
		{"", ""},
		{"Bearer abc", "abc"},
		{"Bearer  abc  ", "abc"},
		{"Basic abc", ""},
		{"bearer abc", ""}, // case-sensitive prefix
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		if got := bearerFromRequest(req); got != tc.want {
			t.Errorf("bearerFromRequest(%q) = %q want %q", tc.header, got, tc.want)
		}
	}
}
