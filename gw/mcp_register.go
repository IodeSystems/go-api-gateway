package gateway

import (
	"sort"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// registerMCPDispatchersLocked walks the IR services produced by slot-IR
// collection and registers one mcpDispatcher per MCP tool, keyed by
// op.SchemaID. Quota + caller-id-enforce middleware wrap each dispatcher,
// matching the proto / openapi / graphql registration posture. MCP has
// no per-source backpressure semaphore, so backpressureMiddleware is
// not applied — tool-call volume against CLI-registered upstreams is
// low, and the upstream owns its own concurrency.
//
// Caller holds g.mu.
func (g *Gateway) registerMCPDispatchersLocked(svcs []*ir.Service) {
	sorted := append([]*ir.Service(nil), svcs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Namespace != sorted[j].Namespace {
			return sorted[i].Namespace < sorted[j].Namespace
		}
		return sorted[i].Version < sorted[j].Version
	})
	for _, svc := range sorted {
		if svc.OriginKind != ir.KindMCP {
			continue
		}
		src := g.mcpSlot(poolKey{namespace: svc.Namespace, version: svc.Version})
		if src == nil {
			continue
		}
		quotaMW := g.quotaMiddleware(svc.Namespace, svc.Version)
		enforceMW := g.callerIDEnforceMiddleware()
		for _, op := range svc.Operations {
			toolName := op.Name
			if origin, ok := op.Origin.(ir.MCPToolOrigin); ok && origin.ToolName != "" {
				toolName = origin.ToolName
			}
			var dispatcher ir.Dispatcher = &mcpDispatcher{
				client:    src.client,
				toolName:  toolName,
				namespace: svc.Namespace,
				version:   svc.Version,
				op:        op.Name,
			}
			if quotaMW != nil {
				dispatcher = quotaMW(dispatcher)
			}
			if enforceMW != nil {
				dispatcher = enforceMW(dispatcher)
			}
			g.dispatchers.Set(op.SchemaID, dispatcher)
		}
	}
}
