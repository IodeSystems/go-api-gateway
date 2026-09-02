package gateway

import (
	"github.com/iodesystems/gwag/v2/gw/ir"
)

// registerProtoDispatchersLocked walks every proto pool matching
// `filter`, builds a backpressure-wrapped protoDispatcher per unary
// RPC (and a protoDirectSubscriptionDispatcher per server-streaming RPC),
// and registers each one in g.dispatchers under
// MakeSchemaID(ns, ver, methodName). The methodName is the wire-level
// PascalCase name (e.g. "Hello", "Greetings") to match what IR's
// IngestProto + PopulateSchemaIDs stamp on op.SchemaID —
// RenderGraphQLRuntime's resolvers look up dispatchers by op.SchemaID
// at call time, so the two sides have to agree on the key shape.
//
// Client-streaming / bidi RPCs are filtered out at registration
// time (control.go) so they never reach a pool — no dispatcher is
// emitted for them here.
//
// Caller holds g.mu.
func (g *Gateway) registerProtoDispatchersLocked(filter schemaFilter) {
	chain := g.runtimeChain()
	headers := g.headerInjectorSnapshot()
	metrics := g.cfg.metrics
	bp := g.cfg.backpressure
	uploadStore := g.cfg.uploadStore
	uploadLimit := g.cfg.uploadLimit
	for _, slot := range g.slots {
		switch slot.kind {
		case slotKindProto:
			p := slot.proto
			if !filter.matchPool(p.key) {
				continue
			}
			if g.isInternal(p.key.namespace) {
				continue
			}
			services := p.file.Services()
			for i := 0; i < services.Len(); i++ {
				sd := services.Get(i)
				methods := sd.Methods()
				for j := 0; j < methods.Len(); j++ {
					md := methods.Get(j)
					if md.IsStreamingClient() {
						continue
					}
					sid := ir.MakeSchemaID(p.key.namespace, p.key.version, string(md.Name()))
					if md.IsStreamingServer() {
						g.dispatchers.Set(sid, newProtoDirectSubscriptionDispatcher(g, p, sd, md))
						continue
					}
					label := methodLabel(sd, md)
					core := newProtoDispatcher(p, sd, md, chain, headers, metrics, bp, uploadStore, uploadLimit)
					dispatcher := backpressureMiddleware(poolBackpressureConfig(p, label, metrics, bp))(core)
					dispatcher = g.quotaMiddleware(p.key.namespace, p.key.version)(dispatcher)
					dispatcher = g.callerIDEnforceMiddleware()(dispatcher)
					g.dispatchers.Set(sid, dispatcher)
				}
			}
		case slotKindInternalProto:
			src := slot.internalProto
			if !filter.matchPool(slot.key) {
				continue
			}
			if g.isInternal(slot.key.namespace) {
				continue
			}
			services := src.file.Services()
			for i := 0; i < services.Len(); i++ {
				sd := services.Get(i)
				methods := sd.Methods()
				for j := 0; j < methods.Len(); j++ {
					md := methods.Get(j)
					if md.IsStreamingClient() {
						continue
					}
					sid := ir.MakeSchemaID(slot.key.namespace, slot.key.version, string(md.Name()))
					if md.IsStreamingServer() {
						// Server-streaming on the internal-proto kind
						// is the Subscription path (e.g. PubSub.Sub).
						// The slot's subscriptionHandlers map carries
						// the in-process broker function; if the
						// method has no handler entry, the dispatcher
						// is omitted and the field resolves to a
						// "no dispatcher" reject — same posture as a
						// missing unary handler.
						if src.subscriptionHandlers[string(md.Name())] == nil {
							continue
						}
						g.dispatchers.Set(sid, newInternalProtoSubscriptionDispatcher(src, md))
						continue
					}
					dispatcher := newInternalProtoDispatcher(src, sd, md, chain, metrics)
					var d ir.Dispatcher = dispatcher
					d = g.quotaMiddleware(slot.key.namespace, slot.key.version)(d)
					d = g.callerIDEnforceMiddleware()(d)
					g.dispatchers.Set(sid, d)
				}
			}
		}
	}
}


