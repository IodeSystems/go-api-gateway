package gateway

import (
	"fmt"

	"github.com/getkin/kin-openapi/openapi3"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// registerOpenAPIDispatchersLocked walks IR services from the
// slot-IR collection and registers one backpressure-wrapped
// openAPIDispatcher in g.dispatchers per Operation, keyed by
// op.SchemaID. RenderGraphQLRuntime's resolvers look up dispatchers
// via the same SchemaID; keeping registration paired with the same
// IR walk that feeds the renderer is the simplest way to keep them
// in sync as ops are added / hidden / filtered.
//
// Each Operation carries `*openapi3.Operation` on op.Origin (set by
// IngestOpenAPI) so the dispatcher's parameter encoding keeps using
// kin-openapi's shape. The IR is the type contract; the OpenAPI doc
// stays the wire contract.
//
// Caller holds g.mu.
func (g *Gateway) registerOpenAPIDispatchersLocked(svcs []*ir.Service) error {
	metrics := g.cfg.metrics
	bp := g.cfg.backpressure
	headers := g.headerInjectorSnapshot()

	// Cross-format runtime middleware: when any Transform.Runtime is
	// registered, pre-render synth input descriptors so each
	// dispatcher can wrap with `wrapCanonicalDispatcherWithChain`. No
	// runtime middlewares = identity chain = no wrap (saves the per-
	// request argsToMessage / messageToMap roundtrip).
	var inputDescs map[ir.SchemaID]protoreflect.MessageDescriptor
	var chain Middleware
	if g.hasRuntimeMiddleware() {
		inputDescs = g.buildOpInputDescriptorsLocked(svcs)
		chain = g.runtimeChain()
	}

	// Compute latest version per namespace so the dispatcher can emit
	// the same `<ns>_` or `<ns>_<vN>_` __typename prefix the renderer
	// builds. Mirrors graphql_register.go's latestByNS pass.
	latestByNS := map[string]int{}
	for _, svc := range svcs {
		if svc.OriginKind != ir.KindOpenAPI {
			continue
		}
		src := g.openAPISlot(poolKey{namespace: svc.Namespace, version: svc.Version})
		if src == nil {
			continue
		}
		if v, ok := latestByNS[svc.Namespace]; !ok || src.versionN > v {
			latestByNS[svc.Namespace] = src.versionN
		}
	}

	for _, svc := range svcs {
		if svc.OriginKind != ir.KindOpenAPI {
			continue
		}
		src := g.openAPISlot(poolKey{namespace: svc.Namespace, version: svc.Version})
		if src == nil {
			continue
		}
		isLatest := src.versionN == latestByNS[svc.Namespace]
		typePrefix := svc.Namespace + "_"
		if !isLatest && svc.Version != "" {
			typePrefix = svc.Namespace + "_" + svc.Version + "_"
		}
		for _, op := range svc.Operations {
			openAPIOp, _ := op.Origin.(*openapi3.Operation)
			if openAPIOp == nil {
				return fmt.Errorf("openapi: ingest dropped op origin for %s/%s/%s", svc.Namespace, svc.Version, op.Name)
			}
			core := newOpenAPIDispatcher(src, openAPIOp, op.HTTPMethod, op.HTTPPath, headers, metrics, bp)
			core.svc = svc
			core.irOp = op
			core.typePrefix = typePrefix
			var dispatcher ir.Dispatcher = backpressureMiddleware(openAPIBackpressureConfig(src, core.label, metrics, bp))(core)
			if chain != nil {
				if md, ok := inputDescs[op.SchemaID]; ok {
					dispatcher = wrapCanonicalDispatcherWithChain(dispatcher, chain, md, svc.Namespace, svc.Version, op.Name)
				}
			}
			dispatcher = g.quotaMiddleware(svc.Namespace, svc.Version)(dispatcher)
			dispatcher = g.callerIDEnforceMiddleware()(dispatcher)
			g.dispatchers.Set(op.SchemaID, dispatcher)
		}
	}
	return nil
}
