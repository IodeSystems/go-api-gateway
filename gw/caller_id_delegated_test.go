package gateway

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cav1 "github.com/iodesystems/gwag/v2/gw/proto/callerauth/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Plan §Caller-ID delegated mode — pins (1) the delegated extractor
// resolves tokens via _caller_auth/v1, (2) TTL cache cuts repeat
// lookups to one RPC, (3) singleflight collapses concurrent misses,
// (4) DENIED is cached as anonymous for NegativeTTL, (5) UNAVAILABLE
// / transport-error falls through without caching, and (6) the NATS
// revoke event evicts the matching token across the cluster.

// fakeCallerAuthConn is a grpc.ClientConnInterface that intercepts
// CallerAuthorizer.Authorize and runs handler in-process. Concurrency-
// safe call counter so the singleflight test can assert collapse.
type fakeCallerAuthConn struct {
	handler   func(*cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error)
	callCount atomic.Int32
	lastReq   atomic.Pointer[cav1.AuthorizeRequest]
}

func (f *fakeCallerAuthConn) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if method != cav1.CallerAuthorizer_Authorize_FullMethodName {
		return fmt.Errorf("fakeCallerAuthConn: unsupported method %s", method)
	}
	in := args.(*cav1.AuthorizeRequest)
	f.lastReq.Store(in)
	f.callCount.Add(1)
	resp, err := f.handler(in)
	if err != nil {
		return err
	}
	out := reply.(*cav1.AuthorizeResponse)
	out.Code = resp.GetCode()
	out.CallerId = resp.GetCallerId()
	out.TtlSeconds = resp.GetTtlSeconds()
	out.Reason = resp.GetReason()
	return nil
}

func (f *fakeCallerAuthConn) NewStream(_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("fakeCallerAuthConn: streams not supported")
}

func registerCallerAuthDelegate(t *testing.T, gw *Gateway, fake *fakeCallerAuthConn) {
	t.Helper()
	if err := gw.AddProtoBytes("callerauth.proto", testProtoBytes(t, "callerauth.proto"),
		To(fake),
		As(callerAuthorizerNamespace),
		AsInternal(),
	); err != nil {
		t.Fatalf("register caller_auth delegate: %v", err)
	}
}

func newDelegatedGateway(t *testing.T, opts CallerIDDelegatedOptions) *Gateway {
	t.Helper()
	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithCallerIDDelegated(opts),
	)
	t.Cleanup(gw.Close)
	return gw
}

func ctxWithCallerToken(token string) context.Context {
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set(DelegatedCallerIDTokenHeader, token)
	return withHTTPRequest(context.Background(), r)
}

func TestDelegatedCallerID_OK_ResolvesAndCaches(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "billing",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("opaque-token-1")
	got, err := gw.callerAuth.resolve(ctx)
	if err != nil || got != "billing" {
		t.Fatalf("first resolve: got=%q err=%v want billing nil", got, err)
	}
	// Second call hits the cache — no extra RPC.
	got, err = gw.callerAuth.resolve(ctx)
	if err != nil || got != "billing" {
		t.Fatalf("second resolve: got=%q err=%v want billing nil", got, err)
	}
	if n := fake.callCount.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 (TTL cache should suppress second)", n)
	}
}

func TestDelegatedCallerID_TokenForwardedToDelegate(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "svc",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	_, _ = gw.callerAuth.resolve(ctxWithCallerToken("alpha-token"))
	if req := fake.lastReq.Load(); req == nil || req.GetToken() != "alpha-token" {
		t.Fatalf("delegate didn't see token; req=%+v", req)
	}
}

func TestDelegatedCallerID_AnonymousNoToken(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			t.Fatalf("delegate must not be called for anonymous request")
			return nil, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	got, err := gw.callerAuth.resolve(context.Background())
	if err != nil || got != "" {
		t.Fatalf("anonymous: got=%q err=%v want \"\" nil", got, err)
	}
	if n := fake.callCount.Load(); n != 0 {
		t.Errorf("delegate calls = %d, want 0", n)
	}
}

func TestDelegatedCallerID_DENIED_CachesNegativeAnonymous(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{
		NegativeTTL: time.Minute,
	})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:   cav1.CallerAuthCode_CALLER_AUTH_CODE_DENIED,
				Reason: "revoked",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("revoked-token")
	got, err := gw.callerAuth.resolve(ctx)
	if err != nil || got != "" {
		t.Fatalf("DENIED first call: got=%q err=%v want \"\" nil", got, err)
	}
	// Second call: negative entry still cached; delegate must not be
	// hit again until NegativeTTL expires.
	got, err = gw.callerAuth.resolve(ctx)
	if err != nil || got != "" {
		t.Fatalf("DENIED second call: got=%q err=%v want \"\" nil", got, err)
	}
	if n := fake.callCount.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 (NegativeTTL must suppress second)", n)
	}
}

func TestDelegatedCallerID_UNAVAILABLE_DoesNotCache(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code: cav1.CallerAuthCode_CALLER_AUTH_CODE_UNAVAILABLE,
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("token-x")
	for i := 0; i < 3; i++ {
		got, err := gw.callerAuth.resolve(ctx)
		if err != nil || got != "" {
			t.Fatalf("call %d: got=%q err=%v want \"\" nil", i, got, err)
		}
	}
	if n := fake.callCount.Load(); n != 3 {
		t.Errorf("delegate calls = %d, want 3 (UNAVAILABLE must not cache)", n)
	}
}

func TestDelegatedCallerID_TransportError_DoesNotCache(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return nil, fmt.Errorf("simulated transport failure")
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("token-y")
	for i := 0; i < 3; i++ {
		got, err := gw.callerAuth.resolve(ctx)
		if err != nil || got != "" {
			t.Fatalf("call %d: got=%q err=%v want \"\" nil", i, got, err)
		}
	}
	if n := fake.callCount.Load(); n != 3 {
		t.Errorf("delegate calls = %d, want 3 (transport error must not cache)", n)
	}
}

func TestDelegatedCallerID_NoDelegateRegistered_FallsBackAnonymous(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{})
	// No delegate registered.
	got, err := gw.callerAuth.resolve(ctxWithCallerToken("orphan"))
	if err != nil || got != "" {
		t.Fatalf("got=%q err=%v want \"\" nil", got, err)
	}
}

func TestDelegatedCallerID_DelegateTTLOverride(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{
		TTL:    time.Hour,
		MaxTTL: 2 * time.Second,
	})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:       cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId:   "short-lived",
				TtlSeconds: 9999, // delegate asks for forever; MaxTTL caps it.
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("ttl-token")
	if _, err := gw.callerAuth.resolve(ctx); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// MaxTTL=2s caps the entry; verify by reaching directly into the
	// cache rather than sleeping past 2s.
	entry, ok := gw.callerAuth.lookup("ttl-token")
	if !ok {
		t.Fatalf("cache miss right after resolve")
	}
	now := time.Now()
	if delta := entry.expiresAt.Sub(now); delta <= 0 || delta > 3*time.Second {
		t.Errorf("expiresAt-now = %v, want capped by MaxTTL=2s", delta)
	}
}

func TestDelegatedCallerID_SingleflightCollapsesConcurrentMisses(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	release := make(chan struct{})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			// Block until the test releases — gives every goroutine
			// time to enter sf.Do and hash onto the same key.
			<-release
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "burst",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	const N = 16
	var wg sync.WaitGroup
	results := make([]string, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, _ := gw.callerAuth.resolve(ctxWithCallerToken("burst-token"))
			results[idx] = got
		}(i)
	}
	// Give the goroutines a moment to all queue up under sf.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, got := range results {
		if got != "burst" {
			t.Errorf("results[%d] = %q, want burst", i, got)
		}
	}
	if n := fake.callCount.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 (singleflight should collapse all %d)", n, N)
	}
}

func TestDelegatedCallerID_GRPCMetadataToken(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "grpc-caller",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	md := metadata.Pairs(DelegatedCallerIDTokenMetadata, "grpc-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	got, err := gw.callerAuth.resolve(ctx)
	if err != nil || got != "grpc-caller" {
		t.Fatalf("got=%q err=%v want grpc-caller nil", got, err)
	}
}

func TestDelegatedCallerID_HTTPHeaderWinsOverGRPCMetadata(t *testing.T) {
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	fake := &fakeCallerAuthConn{
		handler: func(req *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: req.GetToken(), // echo so the test sees which token won
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set(DelegatedCallerIDTokenHeader, "from-http")
	ctx := withHTTPRequest(context.Background(), r)
	md := metadata.Pairs(DelegatedCallerIDTokenMetadata, "from-grpc")
	ctx = metadata.NewIncomingContext(ctx, md)

	got, err := gw.callerAuth.resolve(ctx)
	if err != nil || got != "from-http" {
		t.Fatalf("got=%q err=%v want from-http nil", got, err)
	}
}

func TestDelegatedCallerID_RevokeEventEvictsCache(t *testing.T) {
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

	gw := New(
		WithCluster(cluster),
		WithoutMetrics(),
		WithoutBackpressure(),
		WithCallerIDDelegated(CallerIDDelegatedOptions{TTL: time.Hour}),
	)
	t.Cleanup(gw.Close)

	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "rev-caller",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	ctx := ctxWithCallerToken("rev-token")
	if got, err := gw.callerAuth.resolve(ctx); err != nil || got != "rev-caller" {
		t.Fatalf("prime: got=%q err=%v want rev-caller nil", got, err)
	}
	// Cached — second call must not hit the delegate.
	if got, _ := gw.callerAuth.resolve(ctx); got != "rev-caller" {
		t.Fatalf("expected cached hit, got=%q", got)
	}
	if n := fake.callCount.Load(); n != 1 {
		t.Fatalf("delegate calls before revoke = %d, want 1", n)
	}

	// Publish revoke; gateway's subscriber drops the entry.
	if err := PublishCallerRevoked(cluster.Conn, "rev-token"); err != nil {
		t.Fatalf("PublishCallerRevoked: %v", err)
	}

	// Wait for the subscriber to process — bounded poll, not a fixed
	// sleep. Cache should miss again within ~500ms in practice.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := gw.callerAuth.lookup("rev-token"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cache entry not evicted after revoke")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Next resolve refills the cache via the delegate.
	if got, err := gw.callerAuth.resolve(ctx); err != nil || got != "rev-caller" {
		t.Fatalf("post-revoke resolve: got=%q err=%v want rev-caller nil", got, err)
	}
	if n := fake.callCount.Load(); n != 2 {
		t.Errorf("delegate calls after revoke = %d, want 2", n)
	}
}

func TestDelegatedCallerID_OptionInstallsExtractor(t *testing.T) {
	// End-to-end seam check: after WithCallerIDDelegated, the
	// callerIDExtractor on cfg routes through the delegate. Verifies
	// the New() wiring beyond the per-test direct .resolve() calls.
	gw := newDelegatedGateway(t, CallerIDDelegatedOptions{TTL: time.Minute})
	fake := &fakeCallerAuthConn{
		handler: func(_ *cav1.AuthorizeRequest) (*cav1.AuthorizeResponse, error) {
			return &cav1.AuthorizeResponse{
				Code:     cav1.CallerAuthCode_CALLER_AUTH_CODE_OK,
				CallerId: "seam-caller",
			}, nil
		},
	}
	registerCallerAuthDelegate(t, gw, fake)

	if gw.cfg.callerIDExtractor == nil {
		t.Fatal("WithCallerIDDelegated did not install a callerIDExtractor")
	}
	got, err := gw.cfg.callerIDExtractor(ctxWithCallerToken("seam-token"))
	if err != nil || got != "seam-caller" {
		t.Fatalf("extractor: got=%q err=%v want seam-caller nil", got, err)
	}
}
