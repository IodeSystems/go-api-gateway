package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qav1 "github.com/iodesystems/gwag/v2/gw/proto/quotaauth/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Plan §Caller-ID + quota ladder — pins (1) default-allow when no
// WithQuota is set, (2) OK refill installs a block + debits one
// permit per dispatch, (3) DENIED surfaces 429-shaped rejection with
// RetryAfter, (4) UNAVAILABLE / transport error fails open (emergency
// permits), (5) singleflight collapses concurrent misses, (6) per-
// (caller, ns, ver) buckets are isolated, (7) MaxBlockSize caps a
// misbehaving delegate.

// newQuotaTestGateway builds a gateway with WithQuota and replaces
// the delegate's call function with a fake so we don't need a real
// _quota_auth/v1 pool registered. The fake's call counter lets tests
// pin singleflight + caching behaviour.
func newQuotaTestGateway(t *testing.T, opts QuotaOptions, handler func(*qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error)) (*Gateway, *atomic.Int32) {
	t.Helper()
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithQuota(opts))
	t.Cleanup(gw.Close)
	if gw.quotaAuth == nil {
		t.Fatalf("quotaAuth nil with WithQuota set")
	}
	var calls atomic.Int32
	gw.quotaAuth.callDelegateFn = func(_ context.Context, caller, ns, ver string) (*qav1.AcquireBlockResponse, error) {
		calls.Add(1)
		return handler(&qav1.AcquireBlockRequest{
			CallerId:         caller,
			Namespace:        ns,
			Version:          ver,
			RequestedPermits: gw.quotaAuth.blockSize(),
		})
	}
	return gw, &calls
}

func TestQuota_DefaultAllow_NoOptionInstalled(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)
	if gw.quotaAuth != nil {
		t.Fatalf("quotaAuth should be nil without WithQuota")
	}
	mw := gw.quotaMiddleware("billing", "v1")
	if mw == nil {
		t.Fatalf("middleware factory should never be nil (identity expected)")
	}
	// Wrap a sentinel dispatcher; the wrap must be identity.
	called := false
	core := dispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		called = true
		return "ok", nil
	})
	got, err := mw(core).Dispatch(context.Background(), nil)
	if err != nil || got != "ok" || !called {
		t.Fatalf("identity passthrough failed: got=%v err=%v called=%v", got, err, called)
	}
}

func TestQuota_OK_GrantsBlockAndDebits(t *testing.T) {
	gw, calls := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 3},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 3,
			}, nil
		},
	)
	d := gw.quotaAuth
	// First three checks succeed without re-consulting the delegate
	// (one refill installs the full block).
	for i := 0; i < 3; i++ {
		if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 (one refill installs N permits)", n)
	}
	// Fourth check drains, triggers another refill.
	if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
		t.Fatalf("fourth check: %v", err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("delegate calls = %d, want 2 after bucket drain", n)
	}
}

func TestQuota_DENIED_SurfacesRejectWithRetryAfter(t *testing.T) {
	retryUntil := time.Now().Add(7 * time.Second)
	gw, _ := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 5},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:       qav1.QuotaAuthCode_QUOTA_AUTH_CODE_DENIED,
				ValidUntil: timestamppb.New(retryUntil),
				Reason:     "monthly cap exceeded",
			}, nil
		},
	)
	err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1")
	if err == nil {
		t.Fatalf("expected rejection on DENIED")
	}
	var rej *rejection
	if !errors.As(err, &rej) {
		t.Fatalf("err type = %T, want *rejection", err)
	}
	if rej.Code != CodeResourceExhausted {
		t.Errorf("code = %v, want CodeResourceExhausted", rej.Code)
	}
	if rej.RetryAfter < 5*time.Second || rej.RetryAfter > 10*time.Second {
		t.Errorf("RetryAfter = %v, want ~7s window", rej.RetryAfter)
	}
}

func TestQuota_UNAVAILABLE_FailsOpenWithEmergencyPermits(t *testing.T) {
	gw, calls := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 5, EmergencyPermits: 2, EmergencyTTL: 10 * time.Millisecond},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code: qav1.QuotaAuthCode_QUOTA_AUTH_CODE_UNAVAILABLE,
			}, nil
		},
	)
	d := gw.quotaAuth
	// First two succeed on the emergency block.
	for i := 0; i < 2; i++ {
		if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
			t.Fatalf("emergency dispatch %d: %v", i, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 after first emergency block", n)
	}
}

func TestQuota_TransportError_FailsOpen(t *testing.T) {
	gw, _ := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 5, EmergencyPermits: 1, EmergencyTTL: 10 * time.Millisecond},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return nil, errors.New("transport boom")
		},
	)
	if err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1"); err != nil {
		t.Fatalf("transport error should fail-open: %v", err)
	}
}

func TestQuota_Singleflight_CollapsesConcurrentMisses(t *testing.T) {
	gw, calls := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 100},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			// Hold briefly so concurrent callers actually pile up.
			time.Sleep(20 * time.Millisecond)
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 100,
			}, nil
		},
	)
	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			errs <- gw.quotaAuth.check(context.Background(), "alice", "billing", "v1")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent check failed: %v", err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("delegate calls = %d, want 1 (singleflight collapse)", n)
	}
}

func TestQuota_PerCallerNamespaceVersionIsolation(t *testing.T) {
	gw, calls := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 1},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 1,
			}, nil
		},
	)
	d := gw.quotaAuth
	tuples := [][3]string{
		{"alice", "billing", "v1"},
		{"bob", "billing", "v1"},
		{"alice", "library", "v1"},
		{"alice", "billing", "v2"},
	}
	for _, tup := range tuples {
		if err := d.check(context.Background(), tup[0], tup[1], tup[2]); err != nil {
			t.Fatalf("check %v: %v", tup, err)
		}
	}
	if n := calls.Load(); n != int32(len(tuples)) {
		t.Errorf("delegate calls = %d, want %d (each (caller,ns,ver) is its own bucket)", n, len(tuples))
	}
}

func TestQuota_MaxBlockSize_CapsMisbehavingDelegate(t *testing.T) {
	gw, _ := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 10, MaxBlockSize: 5},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 1_000_000,
			}, nil
		},
	)
	d := gw.quotaAuth
	key := quotaBucketKey("alice", "billing", "v1")
	if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
		t.Fatalf("check: %v", err)
	}
	b := d.buckets[key]
	b.mu.Lock()
	got := b.permits
	b.mu.Unlock()
	// Granted 5 (capped), debited 1 → 4 remaining.
	if got != 4 {
		t.Errorf("permits after first debit = %d, want 4 (MaxBlockSize=5 capped 1M grant)", got)
	}
}

func TestQuota_OKZeroPermits_TreatedAsDenied(t *testing.T) {
	gw, _ := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 5},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 0,
				Reason:         "soft-deny",
			}, nil
		},
	)
	err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1")
	var rej *rejection
	if !errors.As(err, &rej) || rej.Code != CodeResourceExhausted {
		t.Fatalf("expected ResourceExhausted rejection for OK+0 permits, got %v", err)
	}
}

func TestQuota_ValidUntilExpired_ForcesRefill(t *testing.T) {
	var n int
	gw, _ := newQuotaTestGateway(t,
		QuotaOptions{BlockSize: 100},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			n++
			if n == 1 {
				// Short-lived block.
				return &qav1.AcquireBlockResponse{
					Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
					GrantedPermits: 100,
					ValidUntil:     timestamppb.New(time.Now().Add(5 * time.Millisecond)),
				}, nil
			}
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 100,
			}, nil
		},
	)
	d := gw.quotaAuth
	if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
		t.Fatalf("first check: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := d.check(context.Background(), "alice", "billing", "v1"); err != nil {
		t.Fatalf("second check after expiry: %v", err)
	}
	if n != 2 {
		t.Errorf("delegate calls = %d, want 2 (expiry forces refill even with permits remaining)", n)
	}
}

// dispatcherFunc is a tiny test helper mirroring ir.DispatcherFunc so
// tests don't need to import gw/ir.
type dispatcherFunc func(ctx context.Context, args map[string]any) (any, error)

func (f dispatcherFunc) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	return f(ctx, args)
}

// Plan §Caller-ID + quota ladder — pins the WithQuotaEnforce switch.
// Default WithQuota fails open on delegate UNAVAILABLE / transport
// error / no delegate registered. Adding WithQuotaEnforce flips those
// to fail-closed with CodeResourceExhausted.

// newQuotaEnforceTestGateway mirrors newQuotaTestGateway but flips on
// WithQuotaEnforce.
func newQuotaEnforceTestGateway(t *testing.T, opts QuotaOptions, handler func(*qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error)) (*Gateway, *atomic.Int32) {
	t.Helper()
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithQuota(opts), WithQuotaEnforce())
	t.Cleanup(gw.Close)
	if gw.quotaAuth == nil {
		t.Fatalf("quotaAuth nil with WithQuota set")
	}
	if !gw.quotaAuth.enforce {
		t.Fatalf("enforce flag not propagated from cfg.quotaEnforce")
	}
	var calls atomic.Int32
	gw.quotaAuth.callDelegateFn = func(_ context.Context, caller, ns, ver string) (*qav1.AcquireBlockResponse, error) {
		calls.Add(1)
		return handler(&qav1.AcquireBlockRequest{
			CallerId:         caller,
			Namespace:        ns,
			Version:          ver,
			RequestedPermits: gw.quotaAuth.blockSize(),
		})
	}
	return gw, &calls
}

func TestQuotaEnforce_UNAVAILABLE_RejectsWithRetryAfter(t *testing.T) {
	gw, _ := newQuotaEnforceTestGateway(t,
		QuotaOptions{BlockSize: 5, EmergencyPermits: 2, EmergencyTTL: 11 * time.Second},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:   qav1.QuotaAuthCode_QUOTA_AUTH_CODE_UNAVAILABLE,
				Reason: "shard outage",
			}, nil
		},
	)
	err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1")
	if err == nil {
		t.Fatalf("expected rejection on UNAVAILABLE under enforce")
	}
	var rej *rejection
	if !errors.As(err, &rej) {
		t.Fatalf("err type = %T, want *rejection", err)
	}
	if rej.Code != CodeResourceExhausted {
		t.Errorf("code = %v, want CodeResourceExhausted", rej.Code)
	}
	if rej.RetryAfter < 5*time.Second || rej.RetryAfter > 15*time.Second {
		t.Errorf("RetryAfter = %v, want ~emergencyTTL", rej.RetryAfter)
	}
	if !strings.Contains(rej.Msg, "shard outage") {
		t.Errorf("msg = %q, want the delegate-supplied reason", rej.Msg)
	}
}

func TestQuotaEnforce_TransportError_RejectsResourceExhausted(t *testing.T) {
	gw, _ := newQuotaEnforceTestGateway(t,
		QuotaOptions{BlockSize: 5, EmergencyPermits: 1, EmergencyTTL: 11 * time.Second},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return nil, errors.New("dial: connection refused")
		},
	)
	err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1")
	if err == nil {
		t.Fatalf("expected rejection on transport error under enforce")
	}
	var rej *rejection
	if !errors.As(err, &rej) {
		t.Fatalf("err type = %T, want *rejection", err)
	}
	if rej.Code != CodeResourceExhausted {
		t.Errorf("code = %v, want CodeResourceExhausted", rej.Code)
	}
	if rej.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %v, want non-zero so client backs off", rej.RetryAfter)
	}
}

// Sanity-check that enforce doesn't break the OK path.
func TestQuotaEnforce_OK_StillGrants(t *testing.T) {
	gw, _ := newQuotaEnforceTestGateway(t,
		QuotaOptions{BlockSize: 2},
		func(_ *qav1.AcquireBlockRequest) (*qav1.AcquireBlockResponse, error) {
			return &qav1.AcquireBlockResponse{
				Code:           qav1.QuotaAuthCode_QUOTA_AUTH_CODE_OK,
				GrantedPermits: 2,
			}, nil
		},
	)
	for i := 0; i < 2; i++ {
		if err := gw.quotaAuth.check(context.Background(), "alice", "billing", "v1"); err != nil {
			t.Fatalf("OK dispatch %d: %v", i, err)
		}
	}
}

// End-to-end: WithQuotaEnforce + UNAVAILABLE → 429-shaped GraphQL
// error envelope with retryAfterSeconds extension. Same surface
// adopters see as on explicit DENIED (TestQuota_E2E_DeniedSurfaces…),
// just driven by the enforce path.
func TestQuotaEnforce_E2E_UnavailableSurfaces429Extension(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"abc","name":"thing"}`))
	}))
	t.Cleanup(be.Close)

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithCallerIDPublic(),
		WithQuota(QuotaOptions{BlockSize: 1, EmergencyTTL: 8 * time.Second}),
		WithQuotaEnforce(),
	)
	t.Cleanup(gw.Close)
	gw.quotaAuth.callDelegateFn = func(_ context.Context, _, _, _ string) (*qav1.AcquireBlockResponse, error) {
		return &qav1.AcquireBlockResponse{
			Code: qav1.QuotaAuthCode_QUOTA_AUTH_CODE_UNAVAILABLE,
		}, nil
	}

	if err := gw.AddOpenAPIBytes([]byte(minimalOpenAPISpec), To(be.URL), As("test")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/graphql",
		strings.NewReader(`{"query":"{ test { getThing(id:\"1\") { id } } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(PublicCallerIDHeader, "alice")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)

	var body struct {
		Errors []struct {
			Message    string `json:"message"`
			Extensions struct {
				Code              string `json:"code"`
				RetryAfterSeconds int    `json:"retryAfterSeconds"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rr.Body.String())
	}
	if len(body.Errors) == 0 {
		t.Fatalf("expected errors[] in response, got %s", rr.Body.String())
	}
	ext := body.Errors[0].Extensions
	if ext.Code != "RESOURCE_EXHAUSTED" {
		t.Errorf("error code=%q, want RESOURCE_EXHAUSTED (body=%s)", ext.Code, rr.Body.String())
	}
	if ext.RetryAfterSeconds < 5 || ext.RetryAfterSeconds > 12 {
		t.Errorf("retryAfterSeconds=%d, want ~emergencyTTL (~8)", ext.RetryAfterSeconds)
	}
}

// TestQuota_E2E_DeniedSurfaces429WithRetryAfter pins the ingress
// path: WithQuota wired → DENIED rejection → writeIngressDispatchError
// emits 429 + Retry-After header. Goes through gw.Handler() to
// confirm the HTTP error envelope shape adopters see.
func TestQuota_E2E_DeniedSurfaces429WithRetryAfter(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"abc","name":"thing"}`))
	}))
	t.Cleanup(be.Close)

	gw := New(
		WithoutMetrics(),
		WithoutBackpressure(),
		WithCallerIDPublic(),
		WithQuota(QuotaOptions{BlockSize: 1}),
	)
	t.Cleanup(gw.Close)

	// Replace the delegate call with a fake before any dispatch is
	// wired — we want every refill to come back DENIED so the very
	// first request hits the rejection surface.
	gw.quotaAuth.callDelegateFn = func(_ context.Context, _, _, _ string) (*qav1.AcquireBlockResponse, error) {
		return &qav1.AcquireBlockResponse{
			Code:       qav1.QuotaAuthCode_QUOTA_AUTH_CODE_DENIED,
			ValidUntil: timestamppb.New(time.Now().Add(12 * time.Second)),
			Reason:     "over quota",
		}, nil
	}

	if err := gw.AddOpenAPIBytes([]byte(minimalOpenAPISpec), To(be.URL), As("test")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/graphql",
		strings.NewReader(`{"query":"{ test { getThing(id:\"1\") { id } } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(PublicCallerIDHeader, "alice")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)

	// The GraphQL handler returns 200 with errors[] in the body —
	// it doesn't fail the HTTP status for resolver-level rejections.
	// What we want to verify is that the rejection carried code
	// RESOURCE_EXHAUSTED and the retryAfterSeconds extension landed
	// on the error envelope so codegen clients can surface a 429-
	// shaped error to the user.
	var body struct {
		Errors []struct {
			Message    string `json:"message"`
			Extensions struct {
				Code              string `json:"code"`
				RetryAfterSeconds int    `json:"retryAfterSeconds"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rr.Body.String())
	}
	if len(body.Errors) == 0 {
		t.Fatalf("expected errors[] in response, got %s", rr.Body.String())
	}
	ext := body.Errors[0].Extensions
	if ext.Code != "RESOURCE_EXHAUSTED" {
		t.Errorf("error code=%q, want RESOURCE_EXHAUSTED (body=%s)", ext.Code, rr.Body.String())
	}
	if ext.RetryAfterSeconds < 10 || ext.RetryAfterSeconds > 14 {
		t.Errorf("retryAfterSeconds=%d, want ~12", ext.RetryAfterSeconds)
	}
}
