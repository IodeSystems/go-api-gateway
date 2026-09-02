package gateway

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// backpressureConfig is the per-dispatcher knob bag for
// backpressureMiddleware. One backpressureConfig describes one
// (pool / source, operation) coordinate: Sem and Queueing live on
// the pool / source struct (so least-loaded picks across replicas
// share state); Namespace/Version/Label go into metric labels.
//
// Sem == nil means unbounded (MaxInflight=0) → the middleware is a
// no-op pass-through. When Sem != nil, Queueing must be non-nil
// and Metrics must be set (use noopMetrics if needed).
type backpressureConfig struct {
	Sem         chan struct{}
	Queueing    *atomic.Int32
	MaxWaitTime time.Duration
	Metrics     Metrics

	Namespace string
	Version   string
	Label     string // method label for metrics; format-specific (e.g. proto path, HTTP "GET /foo")
	Kind      string // metric kind label; today always "unary" — streams have their own path

	// Replica is the per-instance addr (or "") that owns this sem.
	// Non-empty for per-replica acquireReplicaSlot configs (Kind
	// usually ends with "_instance"); empty for service-level pool
	// configs. When set, the queue-depth metric routes through
	// SetReplicaQueueDepth instead of SetQueueDepth so operators can
	// triage which specific replica is saturated on pools with
	// MaxConcurrencyPerInstance configured.
	Replica string
}

// backpressureMiddleware returns the middleware that wraps an
// ir.Dispatcher with the slot-acquisition + queue-depth + dwell-
// metric prologue currently duplicated inline in three places
// (gw/schema.go buildPoolMethodField, gw/openapi.go field resolver,
// gw/graphql_mirror.go forwarding resolver).
//
// Behavior on slot acquisition timeout: returns Reject with
// CodeResourceExhausted. The resolver boundary in gw/ keeps
// translating that to the GraphQL error envelope as it does today;
// the cutover (steps 3+) replaces the inline prologues but doesn't
// change observable behavior.
// setQueueDepthForCfg routes the depth update to the right metric:
// per-replica configs (Replica != "") use SetReplicaQueueDepth so
// operators can triage which specific replica is saturated; service-
// level configs use SetQueueDepth (one row per pool, kind="unary"
// or "stream").
func setQueueDepthForCfg(cfg backpressureConfig, depth int) {
	if cfg.Replica != "" {
		cfg.Metrics.SetReplicaQueueDepth(cfg.Namespace, cfg.Version, cfg.Kind, cfg.Replica, depth)
		return
	}
	cfg.Metrics.SetQueueDepth(cfg.Namespace, cfg.Version, cfg.Kind, depth)
}

func backpressureMiddleware(cfg backpressureConfig) ir.DispatcherMiddleware {
	return func(next ir.Dispatcher) ir.Dispatcher {
		if cfg.Sem == nil {
			return next
		}
		if appendNext, ok := next.(ir.AppendDispatcher); ok {
			return &backpressureAppendWrapper{cfg: cfg, next: appendNext}
		}
		return ir.DispatcherFunc(func(ctx context.Context, args map[string]any) (any, error) {
			release, err := acquireBackpressureSlot(ctx, cfg)
			if err != nil {
				return nil, err
			}
			defer release()
			return next.Dispatch(ctx, args)
		})
	}
}

// backpressureAppendWrapper carries the AppendDispatcher capability
// through the backpressure middleware so an upstream byte-splice
// dispatcher (graphqlGroupDispatcher, etc.) doesn't degrade to the
// Dispatch + json.Marshal path just because backpressure wrapped it.
// The slot-acquire + release wraps both surfaces; the metric tags
// (cfg.Label) cover both call shapes through addDispatchTime.
type backpressureAppendWrapper struct {
	cfg  backpressureConfig
	next ir.AppendDispatcher
}

func (b *backpressureAppendWrapper) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	release, err := acquireBackpressureSlot(ctx, b.cfg)
	if err != nil {
		return nil, err
	}
	defer release()
	return b.next.Dispatch(ctx, args)
}

func (b *backpressureAppendWrapper) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	release, err := acquireBackpressureSlot(ctx, b.cfg)
	if err != nil {
		return dst, err
	}
	defer release()
	return b.next.DispatchAppend(ctx, args, dst)
}

var _ ir.AppendDispatcher = (*backpressureAppendWrapper)(nil)

