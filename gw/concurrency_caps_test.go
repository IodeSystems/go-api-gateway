package gateway

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// TestMaxConcurrency_PerService_RejectsOnOverflow verifies that the
// per-service cap (set via MaxConcurrency) gates simultaneous unary
// dispatches at the registration's declared ceiling — not the
// gateway-wide BackpressureOptions default.
func TestMaxConcurrency_PerService_RejectsOnOverflow(t *testing.T) {
	// One slow backend that blocks until the test releases it.
	release := make(chan struct{})
	defer close(release)
	beLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{
		helloFn: func(ctx context.Context, _ *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
			select {
			case <-release:
				return &greeterv1.HelloResponse{Greeting: "ok"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	beSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(beSrv, greeter)
	go func() { _ = beSrv.Serve(beLis) }()
	t.Cleanup(beSrv.Stop)

	// Gateway with MaxInflight=64 default but the registration caps
	// the service at 2. Wait budget is short so the third request
	// fast-rejects rather than blocking the test.
	gw := New(
		WithoutMetrics(),
		WithBackpressure(BackpressureOptions{MaxInflight: 64, MaxWaitTime: 50 * time.Millisecond}),
		WithAdminToken([]byte("ignored")),
	)
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(beLis.Addr().String()),
		As("greeter"),
		MaxConcurrency(2),
	); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}
	_ = gw.Handler() // trigger schema assembly so dispatchers populate

	d := gw.dispatchers.Get("greeter/v1/Hello")
	if d == nil {
		t.Fatal("Hello dispatcher not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Saturate the cap with two slow dispatches.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = d.Dispatch(ctx, map[string]any{})
		}()
	}

	// Wait for both to be in-flight at the backend (cap is now full).
	deadline := time.Now().Add(2 * time.Second)
	for greeter.helloCalls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d in flight after 2s", greeter.helloCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Third dispatch should fast-reject within MaxWaitTime budget.
	start := time.Now()
	_, err = d.Dispatch(ctx, map[string]any{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected ResourceExhausted reject, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("third dispatch took %v — backpressure didn't fast-reject", elapsed)
	}
}

// TestMaxConcurrencyPerInstance_GatesSingleReplica verifies the
// per-replica cap blocks at the instance ceiling even when the
// service-level cap has plenty of room.
func TestMaxConcurrencyPerInstance_GatesSingleReplica(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	beLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	greeter := &fakeGreeterServer{
		helloFn: func(ctx context.Context, _ *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
			select {
			case <-release:
				return &greeterv1.HelloResponse{Greeting: "ok"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	beSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(beSrv, greeter)
	go func() { _ = beSrv.Serve(beLis) }()
	t.Cleanup(beSrv.Stop)

	gw := New(
		WithoutMetrics(),
		WithBackpressure(BackpressureOptions{MaxInflight: 64, MaxWaitTime: 50 * time.Millisecond}),
		WithAdminToken([]byte("ignored")),
	)
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(beLis.Addr().String()),
		As("greeter"),
		MaxConcurrencyPerInstance(1),
	); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}
	_ = gw.Handler()

	d := gw.dispatchers.Get("greeter/v1/Hello")
	if d == nil {
		t.Fatal("Hello dispatcher not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First dispatch saturates the single-replica instance cap.
	var rejected atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = d.Dispatch(ctx, map[string]any{})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for greeter.helloCalls.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("first dispatch never reached backend")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Second dispatch should fast-reject (per-instance cap = 1).
	start := time.Now()
	_, err = d.Dispatch(ctx, map[string]any{})
	if err != nil {
		rejected.Add(1)
	}
	if rejected.Load() != 1 {
		t.Fatalf("expected per-instance cap to reject, got nil error")
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("second dispatch took %v — per-instance backpressure didn't fast-reject", took)
	}
}

// TestMaxConcurrency_ControlPlaneRoundTrip verifies the
// cpv1.ServiceBinding.MaxConcurrency field reaches the live pool
// when a service registers via the control plane (standalone mode,
// in-process Register call) — the wire-up that lets external
// services declare their own caps.
func TestMaxConcurrency_ControlPlaneRoundTrip(t *testing.T) {
	beLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	beSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(beSrv, &fakeGreeterServer{})
	go func() { _ = beSrv.Serve(beLis) }()
	t.Cleanup(beSrv.Stop)

	gw := New(
		WithoutMetrics(),
		WithBackpressure(BackpressureOptions{MaxInflight: 64, MaxWaitTime: 50 * time.Millisecond}),
		WithAdminToken([]byte("ignored")),
	)
	t.Cleanup(gw.Close)

	cp := gw.ControlPlane()
	if _, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr:       beLis.Addr().String(),
		InstanceId: "greeter@1",
		Services: []*cpv1.ServiceBinding{{
			Namespace:                 "greeter",
			Version:                   "v1",
			ProtoSource:               testProtoBytes(t, "greeter.proto"),
			MaxConcurrency:            7,
			MaxConcurrencyPerInstance: 3,
		}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	gw.mu.Lock()
	p := gw.protoSlot(poolKey{namespace: "greeter", version: "v1"})
	gw.mu.Unlock()
	if p == nil {
		t.Fatal("pool not created")
	}
	if got := p.maxConcurrency; got != 7 {
		t.Fatalf("pool.maxConcurrency=%d want 7", got)
	}
	if got := p.maxConcurrencyPerInstance; got != 3 {
		t.Fatalf("pool.maxConcurrencyPerInstance=%d want 3", got)
	}
	if cap(p.sem) != 7 {
		t.Fatalf("pool sem cap=%d want 7", cap(p.sem))
	}
	rs := p.replicas.Load()
	if rs == nil || len(*rs) != 1 {
		t.Fatalf("expected 1 replica, got %v", rs)
	}
	if cap((*rs)[0].sem) != 3 {
		t.Fatalf("replica sem cap=%d want 3", cap((*rs)[0].sem))
	}
}

// TestMaxConcurrency_MismatchAcrossReplicasRejected ensures registration
// drift surfaces loudly: a second AddProto call that uses a different
// MaxConcurrency value than the first must fail rather than silently
// adopt the new cap (or worse, mix two interpretations).
func TestMaxConcurrency_MismatchAcrossReplicasRejected(t *testing.T) {
	beLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	beSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(beSrv, &fakeGreeterServer{})
	go func() { _ = beSrv.Serve(beLis) }()
	t.Cleanup(beSrv.Stop)

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("ignored")))
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(beLis.Addr().String()),
		As("greeter"),
		MaxConcurrency(8),
	); err != nil {
		t.Fatalf("first AddProtoDescriptor: %v", err)
	}
	err = gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"),
		To(beLis.Addr().String()),
		As("greeter"),
		MaxConcurrency(16),
	)
	if err == nil {
		t.Fatal("expected mismatched MaxConcurrency to be rejected")
	}
}
