package gateway

import (
	"github.com/iodesystems/gwag/v2/gw/ir"
)

// gatewayServicesAsIR returns the gateway's pre-baked slot IR
// filtered by `selectors`. Each slot's `slot.ir` is already
// transformed (HideInternal, schema rewrites, proto subscription
// auth args, SchemaIDs populated) at registration time — see
// `bakeSlotIRLocked` in slot.go. This function is thin by design:
// the cost of producing IR for the gateway moved from O(N
// rebuilds × N ingests) to O(1 ingest at registration).
//
// Caller passes the ParseSelectors output (or nil for "all").
func (g *Gateway) gatewayServicesAsIR(selectors []ir.Selector) ([]*ir.Service, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := []*ir.Service{}
	for _, s := range g.slots {
		out = append(out, s.ir...)
	}
	return ir.Filter(out, selectors), nil
}

// irSelectorsFromSchema converts a parsed []serviceSelector (the
// existing parseProtoSelectors output) into the IR's []Selector
// shape. Trivially structural — kept as a tiny adapter so handler
// code reads cleanly.
func irSelectorsFromSchema(s []serviceSelector) []ir.Selector {
	out := make([]ir.Selector, 0, len(s))
	for _, sel := range s {
		out = append(out, ir.Selector{Namespace: sel.namespace, Version: sel.version})
	}
	return out
}
