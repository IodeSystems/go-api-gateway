package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// recMetrics records the four metric calls backpressureMiddleware
// makes. Embeds noopMetrics so the rest of the Metrics interface is
// satisfied without per-test fixture noise.
type recMetrics struct {
	noopMetrics
	mu      sync.Mutex
	dwell   []string
	backoff []string
	queue   []string
}

func (m *recMetrics) RecordDwell(ns, ver, method, kind string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dwell = append(m.dwell, ns+"/"+ver+":"+method+":"+kind)
}

func (m *recMetrics) RecordBackoff(ns, ver, method, kind, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backoff = append(m.backoff, ns+"/"+ver+":"+method+":"+kind+":"+reason)
}

func (m *recMetrics) SetQueueDepth(ns, ver, kind string, depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, ns+"/"+ver+":"+kind+":"+itoa(depth))
}

// SetReplicaQueueDepth captures the per-replica gauge updates so the
// queued-with-Replica test can assert SetReplicaQueueDepth fired
// instead of the per-pool SetQueueDepth.
func (m *recMetrics) SetReplicaQueueDepth(ns, ver, kind, replica string, depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, ns+"/"+ver+":"+kind+":"+replica+":"+itoa(depth))
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// nilSem → middleware is a no-op pass-through; metric calls don't
// happen, the inner runs once, errors propagate as-is.
func TestBackpressureMiddleware_NoSemPassthrough(t *testing.T) {
	m := &recMetrics{}
	wantErr := errors.New("inner")
	calls := 0
	wrapped := backpressureMiddleware(backpressureConfig{
		Sem:     nil,
		Metrics: m,
	})(ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		calls++
		return nil, wantErr
	}))

	if _, err := wrapped.Dispatch(context.Background(), nil); err != wantErr {
		t.Fatalf("err: want %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("calls: want 1, got %d", calls)
	}
	if len(m.dwell)+len(m.queue)+len(m.backoff) != 0 {
		t.Fatalf("no metrics expected on passthrough; got dwell=%v queue=%v backoff=%v", m.dwell, m.queue, m.backoff)
	}
}

// Fast-path: slot is immediately available → exactly one Dwell
// metric, no queue-depth / backoff calls, slot is released after.
func TestBackpressureMiddleware_FastPath(t *testing.T) {
	m := &recMetrics{}
	sem := make(chan struct{}, 1)
	q := &atomic.Int32{}
	wrapped := backpressureMiddleware(backpressureConfig{
		Sem:       sem,
		Queueing:  q,
		Metrics:   m,
		Namespace: "ns",
		Version:   "v1",
		Label:     "lbl",
		Kind:      "unary",
	})(ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		return "ok", nil
	}))

	out, err := wrapped.Dispatch(context.Background(), nil)
	if err != nil || out != "ok" {
		t.Fatalf("dispatch: out=%v err=%v", out, err)
	}
	if len(sem) != 0 {
		t.Fatalf("slot leaked: len(sem)=%d", len(sem))
	}
	if len(m.dwell) != 1 || m.dwell[0] != "ns/v1:lbl:unary" {
		t.Fatalf("dwell: want one ns/v1:lbl:unary, got %v", m.dwell)
	}
	if len(m.queue) != 0 {
		t.Fatalf("queue depth: want 0 calls, got %v", m.queue)
	}
	if len(m.backoff) != 0 {
		t.Fatalf("backoff: want 0 calls, got %v", m.backoff)
	}
}

// Queued path: slot is taken, then released; middleware should
// emit queue-depth ↑ then ↓ around the wait, plus a Dwell record.
func TestBackpressureMiddleware_QueuedThenAcquires(t *testing.T) {
	m := &recMetrics{}
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // hold the slot
	q := &atomic.Int32{}
	wrapped := backpressureMiddleware(backpressureConfig{
		Sem:         sem,
		Queueing:    q,
		MaxWaitTime: time.Second,
		Metrics:     m,
		Namespace:   "ns",
		Version:     "v1",
		Label:       "lbl",
		Kind:        "unary",
	})(ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		return "done", nil
	}))

	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Dispatch(context.Background(), nil)
		done <- err
	}()
	// Let the goroutine reach the queued branch before releasing.
	time.Sleep(20 * time.Millisecond)
	<-sem // release

	if err := <-done; err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	// queue depth: ↑ to 1, ↓ to 0
	if len(m.queue) != 2 || m.queue[0] != "ns/v1:unary:1" || m.queue[1] != "ns/v1:unary:0" {
		t.Fatalf("queue depth: want [ns/v1:unary:1, ns/v1:unary:0], got %v", m.queue)
	}
	if len(m.dwell) != 1 {
		t.Fatalf("dwell: want 1 entry, got %v", m.dwell)
	}
	if len(m.backoff) != 0 {
		t.Fatalf("backoff: want 0, got %v", m.backoff)
	}
}

// Timeout: slot held for the full MaxWaitTime → Reject with
// CodeResourceExhausted, queue-depth ↑↓, dwell, backoff.
func TestBackpressureMiddleware_TimeoutRejects(t *testing.T) {
	m := &recMetrics{}
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // permanently hold
	q := &atomic.Int32{}
	wrapped := backpressureMiddleware(backpressureConfig{
		Sem:         sem,
		Queueing:    q,
		MaxWaitTime: 30 * time.Millisecond,
		Metrics:     m,
		Namespace:   "ns",
		Version:     "v1",
		Label:       "lbl",
		Kind:        "unary",
	})(ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		t.Fatal("inner must not run on timeout")
		return nil, nil
	}))

	_, err := wrapped.Dispatch(context.Background(), nil)
	if err == nil {
		t.Fatal("want reject, got nil")
	}
	if codeOf(err) != CodeResourceExhausted {
		t.Fatalf("want CodeResourceExhausted, got %v (err=%v)", codeOf(err), err)
	}
	if len(m.backoff) != 1 || m.backoff[0] != "ns/v1:lbl:unary:wait_timeout" {
		t.Fatalf("backoff: want one wait_timeout, got %v", m.backoff)
	}
	if len(m.queue) != 2 || m.queue[0] != "ns/v1:unary:1" || m.queue[1] != "ns/v1:unary:0" {
		t.Fatalf("queue depth: want ↑↓, got %v", m.queue)
	}
	if q.Load() != 0 {
		t.Fatalf("queueing counter: want 0 after timeout, got %d", q.Load())
	}
}

func codeOf(err error) Code {
	var rj *rejection
	if errors.As(err, &rj) {
		return rj.Code
	}
	return Code(-1)
}

// TestBackpressureMiddleware_ReplicaSplitsQueueDepth pins the
// per-replica gauge label: a backpressureConfig with Replica set
// routes queue-depth updates through SetReplicaQueueDepth (replica
// label captured) instead of SetQueueDepth, so two saturated
// replicas in the same pool show up as separate rows when
// MaxConcurrencyPerInstance is configured.
func TestBackpressureMiddleware_ReplicaSplitsQueueDepth(t *testing.T) {
	m := &recMetrics{}
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // hold
	q := &atomic.Int32{}
	wrapped := backpressureMiddleware(backpressureConfig{
		Sem:         sem,
		Queueing:    q,
		MaxWaitTime: time.Second,
		Metrics:     m,
		Namespace:   "ns",
		Version:     "v1",
		Label:       "lbl",
		Kind:        "unary_instance",
		Replica:     "10.0.0.5:50051",
	})(ir.DispatcherFunc(func(_ context.Context, _ map[string]any) (any, error) {
		return "done", nil
	}))

	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Dispatch(context.Background(), nil)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	<-sem // release

	if err := <-done; err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	want := []string{
		"ns/v1:unary_instance:10.0.0.5:50051:1",
		"ns/v1:unary_instance:10.0.0.5:50051:0",
	}
	if len(m.queue) != 2 || m.queue[0] != want[0] || m.queue[1] != want[1] {
		t.Fatalf("queue depth (replica-labeled): want %v, got %v", want, m.queue)
	}
}
