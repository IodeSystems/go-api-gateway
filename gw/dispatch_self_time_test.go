package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// withDispatchAccumulator installs a fresh accumulator that
// addDispatchTime updates atomically. The plumbing is what each
// dispatcher hangs its self-time slice off of, so a unit test that
// covers the helpers + a dispatcher run is enough — every callsite
// shares this code path.
func TestDispatchAccumulator_AddSums(t *testing.T) {
	ctx, accum := withDispatchAccumulator(context.Background())
	addDispatchTime(ctx, 5*time.Millisecond)
	addDispatchTime(ctx, 7*time.Millisecond)
	addDispatchTime(ctx, 0)
	if got, want := time.Duration(accum.Sum.Load()), 12*time.Millisecond; got != want {
		t.Fatalf("accumulator Sum = %v, want %v", got, want)
	}
	if got, want := accum.Count.Load(), int64(3); got != want {
		t.Fatalf("accumulator Count = %d, want %d", got, want)
	}
}

// addDispatchTime is a no-op when no accumulator is installed —
// keeps in-process tests, control-plane RPCs, and any other below-
// ingress callers from panicking on a missing key.
func TestDispatchAccumulator_NoopWithoutAccumulator(t *testing.T) {
	addDispatchTime(context.Background(), 5*time.Millisecond)
	if got := dispatchAccumulatorFromContext(context.Background()); got != nil {
		t.Fatalf("expected nil accumulator on bare ctx, got %v", got)
	}
}

// Every dispatcher (proto / openapi / graphql) must call
// addDispatchTime alongside RecordDispatch. The proto-side dispatcher
// is the one we exercise end-to-end (in-process grpc.Server) — same
// shape as TestDispatchRegistry_PopulatedAfterSchemaBuild but we
// install an accumulator on the dispatch context and assert the
// observed total tracks dispatch_duration's denominator.
func TestProtoDispatcher_PopulatesAccumulator(t *testing.T) {
	f := newGRPCE2EFixture(t)
	d := f.gw.dispatchers.Get(ir.MakeSchemaID("greeter", "v1", "Hello"))
	if d == nil {
		t.Fatal("dispatcher missing for greeter/v1/Hello")
	}
	ctx, accum := withDispatchAccumulator(context.Background())
	if _, err := d.Dispatch(ctx, map[string]any{"name": "selftime"}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := time.Duration(accum.Sum.Load())
	if got <= 0 {
		t.Fatalf("accumulator did not advance after dispatch: %v", got)
	}
	// Sanity ceiling: a successful in-process Hello completes in
	// milliseconds; if the accumulator is grossly larger than the
	// test's wall-clock budget, the accumulation got wired wrong
	// (e.g. counting in seconds instead of nanoseconds).
	if got > time.Second {
		t.Fatalf("accumulator implausibly large: %v", got)
	}
}
