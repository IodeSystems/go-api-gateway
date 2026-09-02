package gateway

import (
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// registerGraphQLDispatchersLocked walks the IR services produced by
// the slot-IR collection and registers one backpressure-wrapped
// graphQLDispatcher per Operation, keyed by op.SchemaID.
//
// The dispatcher captures a *graphQLMirror so it can reuse
// rewriteFieldForRemote / unprefixTypeName for AST forwarding —
// graphql→graphql forwarding can't reconstruct an upstream query from
// canonical args alone (it needs the local selection-set), so the
// mirror's AST helpers stay alive even though its build() / type
// construction path is replaced by RenderGraphQLRuntime. mirror.isLatest
// is set per-source so unprefixTypeName picks the right `<ns>_` /
// `<ns>_<vN>_` prefix when stripping local type names off inline
// fragments before forwarding.
//
// Caller holds g.mu.
func (g *Gateway) registerGraphQLDispatchersLocked(svcs []*ir.Service) error {
	metrics := g.cfg.metrics
	bp := g.cfg.backpressure

	// Compute the latest version per namespace so the mirror's
	// unprefixTypeName uses the matching prefix shape. Mirrors the
	// versionN sort the renderer applies.
	latestByNS := map[string]int{}
	for _, svc := range svcs {
		if svc.OriginKind != ir.KindGraphQL {
			continue
		}
		src := g.graphQLSlot(poolKey{namespace: svc.Namespace, version: svc.Version})
		if src == nil {
			continue
		}
		if v, ok := latestByNS[svc.Namespace]; !ok || src.versionN > v {
			latestByNS[svc.Namespace] = src.versionN
		}
	}

	// Stable order for reproducibility — register in the same order the
	// renderer walks namespaces and operations.
	sorted := append([]*ir.Service(nil), svcs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Namespace != sorted[j].Namespace {
			return sorted[i].Namespace < sorted[j].Namespace
		}
		return sorted[i].Version < sorted[j].Version
	})

	// Cross-format runtime middleware: render synth input descriptors
	// once (across every graphql-origin svc) when any Runtime middleware
	// is registered, so each dispatcher can wrap with the chain. Skip
	// when the chain is empty — the boundary roundtrip would only add
	// per-request alloc.
	var inputDescs map[ir.SchemaID]protoreflect.MessageDescriptor
	var chain Middleware
	if g.hasRuntimeMiddleware() {
		inputDescs = g.buildOpInputDescriptorsLocked(svcs)
		chain = g.runtimeChain()
	}

	for _, svc := range sorted {
		if svc.OriginKind != ir.KindGraphQL {
			continue
		}
		src := g.graphQLSlot(poolKey{namespace: svc.Namespace, version: svc.Version})
		if src == nil {
			continue
		}
		mirror := newGraphQLMirror(src)
		mirror.isLatest = src.versionN == latestByNS[svc.Namespace]
		quotaMW := g.quotaMiddleware(svc.Namespace, svc.Version)
		enforceMW := g.callerIDEnforceMiddleware()
		registerGraphQLOps(g.dispatchers, mirror, src, svc.Operations, metrics, bp, false, chain, inputDescs, svc.Namespace, svc.Version, quotaMW, enforceMW)
		for _, grp := range svc.Groups {
			// Each top-level group gets a graphqlGroupDispatcher that
			// captures the whole sub-selection and forwards in one
			// upstream call. Per-leaf dispatchers under graphql groups
			// stay registered for the canonical-args (HTTP/gRPC ingress)
			// path; they short-circuit to a "no AST" error when the
			// renderer's bypass-resolver path takes over for GraphQL
			// ingress.
			registerGraphQLGroupDispatcher(g.dispatchers, mirror, grp, metrics, bp, quotaMW, enforceMW)
			registerGraphQLGroupOps(g.dispatchers, mirror, src, grp, metrics, bp, chain, inputDescs, svc.Namespace, svc.Version, quotaMW, enforceMW)
		}
	}
	return nil
}

// registerGraphQLGroupDispatcher installs a graphqlGroupDispatcher at
// the group's SchemaID. Backpressure wraps it (one slot per upstream
// call, same posture as a per-leaf dispatch). Quota + caller-id
// enforce wrap the outside, matching the per-leaf wiring.
func registerGraphQLGroupDispatcher(registry *ir.DispatchRegistry, mirror *graphQLMirror, grp *ir.OperationGroup, metrics Metrics, bp BackpressureOptions, quotaMW, enforceMW ir.DispatcherMiddleware) {
	core := newGraphqlGroupDispatcher(mirror, grp, metrics)
	var dispatcher ir.Dispatcher = core
	dispatcher = backpressureMiddleware(graphQLBackpressureConfig(mirror.src, core.label, metrics, bp))(core)
	if quotaMW != nil {
		dispatcher = quotaMW(dispatcher)
	}
	if enforceMW != nil {
		dispatcher = enforceMW(dispatcher)
	}
	registry.Set(grp.SchemaID, dispatcher)
}

func registerGraphQLOps(registry *ir.DispatchRegistry, mirror *graphQLMirror, src *graphQLSource, ops []*ir.Operation, metrics Metrics, bp BackpressureOptions, isGrouped bool, chain Middleware, inputDescs map[ir.SchemaID]protoreflect.MessageDescriptor, namespace, version string, quotaMW, enforceMW ir.DispatcherMiddleware) {
	for _, op := range ops {
		opLabel := graphQLOpLabel(op.Kind)
		core := newGraphQLDispatcher(mirror, op, opLabel, metrics, isGrouped)
		// Subscriptions skip backpressureMiddleware: src.sem is the
		// per-source unary slot count, not a stream lifetime gauge,
		// and the pre-cutover subscribingResolver bypassed it for the
		// same reason. Stream rate-control rides on the subscription
		// broker / per-replica inflight instead.
		var dispatcher ir.Dispatcher = core
		if op.Kind != ir.OpSubscription {
			dispatcher = backpressureMiddleware(graphQLBackpressureConfig(src, core.label, metrics, bp))(core)
		}
		if chain != nil && op.Kind != ir.OpSubscription {
			if md, ok := inputDescs[op.SchemaID]; ok {
				dispatcher = wrapCanonicalDispatcherWithChain(dispatcher, chain, md, namespace, version, op.Name)
			}
		}
		// Quota gate runs outermost so over-quota callers reject
		// before paying the backpressureMiddleware queue cost.
		// Subscriptions skip the gate (matches their
		// backpressureMiddleware skip — stream lifetime is gauged
		// elsewhere). Caller-id enforce wraps quota so the cheaper
		// unauth rejection short-circuits in front of the per-caller
		// bucket lookup.
		if op.Kind != ir.OpSubscription && quotaMW != nil {
			dispatcher = quotaMW(dispatcher)
		}
		if op.Kind != ir.OpSubscription && enforceMW != nil {
			dispatcher = enforceMW(dispatcher)
		}
		registry.Set(op.SchemaID, dispatcher)
	}
}

func registerGraphQLGroupOps(registry *ir.DispatchRegistry, mirror *graphQLMirror, src *graphQLSource, grp *ir.OperationGroup, metrics Metrics, bp BackpressureOptions, chain Middleware, inputDescs map[ir.SchemaID]protoreflect.MessageDescriptor, namespace, version string, quotaMW, enforceMW ir.DispatcherMiddleware) {
	// Grouped ops live under a namespace-shaped upstream object; the
	// canonical-args path can't synthesize the nested call shape from
	// a leaf op alone, so isGrouped=true short-circuits canonicalQuery
	// and the dispatcher returns the existing "no AST" error when an
	// HTTP/gRPC ingress reaches one.
	registerGraphQLOps(registry, mirror, src, grp.Operations, metrics, bp, true, chain, inputDescs, namespace, version, quotaMW, enforceMW)
	for _, sub := range grp.Groups {
		registerGraphQLGroupOps(registry, mirror, src, sub, metrics, bp, chain, inputDescs, namespace, version, quotaMW, enforceMW)
	}
}

func graphQLOpLabel(kind ir.OpKind) string {
	switch kind {
	case ir.OpMutation:
		return "mutation"
	case ir.OpSubscription:
		return "subscription"
	default:
		return "query"
	}
}
