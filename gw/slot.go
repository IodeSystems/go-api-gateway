package gateway

import (
	"fmt"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// slotKind tags which registration form occupies a slot. Plan §4's
// tier model is single-slot per (namespace, version), and a single
// occupant means single kind: the same coordinate cannot be claimed
// by both proto and openapi at once. Cross-kind collision is what
// the slot index catches that the per-kind maps couldn't.
type slotKind int

const (
	slotKindProto slotKind = iota + 1
	slotKindOpenAPI
	slotKindGraphQL
	// slotKindInternalProto is a proto-shaped slot whose dispatch
	// resolves in-process: no replicas, no grpc.ClientConn, no
	// upstream. The slot owns a FileDescriptor (so IR / SDL / MCP /
	// admin-listing render the same way they would for any other
	// proto slot) plus a per-method handler map for direct Go-call
	// dispatch. Used by the gateway's own pub/sub primitive
	// (`gwag.ps.v1.PubSub`); future migrations of admin operations
	// off huma/OpenAPI can ride the same machinery.
	slotKindInternalProto
	// slotKindMCP is a slot fronting a downstream MCP server. The slot
	// owns a live MCP client (stdio subprocess or HTTP/SSE connection)
	// and the raw `tools/list` JSON; each tool ingests as a GraphQL
	// Mutation. Boot-time introspection only — no control-plane path.
	slotKindMCP
)

func (k slotKind) String() string {
	switch k {
	case slotKindProto:
		return "proto"
	case slotKindOpenAPI:
		return "openapi"
	case slotKindGraphQL:
		return "graphql"
	case slotKindInternalProto:
		return "internalproto"
	case slotKindMCP:
		return "mcp"
	default:
		return "unknown"
	}
}

// slot is the per-(namespace, version) registration record. One slot
// per (ns, ver); the slot's kind picks which per-kind dispatch
// handle (`proto` / `openapi` / `graphql`) carries the format-specific
// state. The slot is the canonical management-side index — every
// registration goes through `registerSlotLocked`, every iteration
// (schema rebuild, admin lists, FDS export, dispatcher registration,
// inject inventory, reconciler delete-routing) goes through
// `g.slots`.
//
// `ir` is the request-ready IR baked at slot creation: ingest +
// internal-flag stamp + `applySchemaRewrites` (HideType / HidePath /
// NullableType / NullablePath — the schema-half of every Transform) +
// proto subscription auth-arg injection + `PopulateSchemaIDs`. Schema
// rebuild reads this directly; no per-kind walk, no transform pass.
// `Use(...)` is the single invalidator: it appends to `g.transforms`
// and re-bakes every slot's IR (cheap; ingest + transforms over
// already-loaded handles).
type slot struct {
	key  poolKey
	kind slotKind

	// Schema-equality material. Two registrations with identical hash
	// and caps are interchangeable (multi-replica add); anything else
	// is either a swap (unstable) or a reject (vN).
	hash                      [32]byte
	maxConcurrency            int
	maxConcurrencyPerInstance int

	// deprecationReason is the operator-supplied reason set via the
	// Deprecate admin RPC. Empty means "not manually deprecated" —
	// auto-deprecation (older vN cuts) still applies independently.
	// `bakeSlotIRLocked` stamps this onto every IR Service so the
	// renderer can OR-combine. Set by setDeprecationLocked; survives
	// across schema rebuilds (the slot itself owns it, not the IR).
	deprecationReason string

	// Request-ready IR. Populated by `bakeSlotIRLocked` on slot
	// creation and re-baked on `Use(...)`. nil only between slot
	// allocation and the per-kind handle being attached (a transient
	// in-mu state inside `joinPoolLocked` / `addOpenAPISourceLocked`
	// / `addGraphQLSourceLocked`).
	ir []*ir.Service

	// Exactly one of these is non-nil based on kind. The per-kind
	// struct holds the format handle (`*pool.file`, `*openAPISource.doc`,
	// `*graphQLSource.introspection`), the original schema bytes for
	// `/schema/*` re-emit, the replicas, and the backpressure sems.
	// `internalProto` is the in-process analogue of `proto`: same
	// FileDescriptor / hash / raw-source shape, but a handler map
	// stands in for replicas + grpc.ClientConn.
	proto         *pool
	openapi       *openAPISource
	graphql       *graphQLSource
	internalProto *internalProtoSource
	mcp           *mcpSource

	// channelBindings is the slot-level canonical list of pub/sub
	// channel→payload-type pairings carried by this slot's source
	// (proto / internalproto only; openapi / graphql are always nil).
	// Set once at `registerSlotLocked` time from the incoming
	// FileDescriptor and read back at bake to stamp each IR Service
	// and at cross-slot uniqueness check time to enforce the pre-1.0
	// "two slots can't claim the same pattern" rule. A re-register
	// that matches kind+hash+caps re-uses the prior bindings (hash
	// captures the proto source bytes, which captures the bindings);
	// an `unstable` swap installs fresh bindings alongside the new
	// per-kind handle.
	channelBindings []ir.ChannelBinding
}

// registerSlotLocked applies the §4 tier policy at registration time.
// The four outcomes:
//
//   - No slot exists: insert + return existed=false. Caller does its
//     kind-specific creation (allocate sems, build pool/source, add
//     replica, schema rebuild).
//   - Slot exists, identical (kind + hash + caps): return
//     existed=true. Caller does its kind-specific add-replica only.
//   - Slot exists, version="unstable", anything different: evict the
//     prior slot's per-kind storage and insert fresh. Returns
//     existed=false so the caller proceeds with creation. Plan §4
//     single-overwrite-slot semantics.
//   - Slot exists, version="vN", anything different: reject with a
//     descriptive error — cross-kind, schema-hash, or caps as the
//     differentiator. vN is locked once registered.
//
// Caller holds g.mu.
func (g *Gateway) registerSlotLocked(kind slotKind, key poolKey, hash [32]byte, maxConcurrency, maxConcurrencyPerInstance int, bindings []ir.ChannelBinding) (existed bool, err error) {
	if err := g.checkVersionTierAllowed(key.version); err != nil {
		return false, err
	}
	return g.registerSlotLockedSkipTierCheck(kind, key, hash, maxConcurrency, maxConcurrencyPerInstance, bindings)
}

// registerSlotLockedSkipTierCheck is the body of registerSlotLocked
// without the --allow-tier policy gate. Used by gateway-bundled
// internal-proto installs (e.g. installPubSubSlot) — those slots are
// gateway code, not operator/user registrations, so the tier policy
// (which exists to constrain *user* registrations) doesn't apply.
// Caller holds g.mu.
func (g *Gateway) registerSlotLockedSkipTierCheck(kind slotKind, key poolKey, hash [32]byte, maxConcurrency, maxConcurrencyPerInstance int, bindings []ir.ChannelBinding) (existed bool, err error) {
	if g.slots == nil {
		g.slots = map[poolKey]*slot{}
	}
	s, ok := g.slots[key]
	if !ok {
		if err := g.checkCrossSlotBindingsLocked(key, bindings); err != nil {
			return false, err
		}
		g.slots[key] = &slot{
			key:                       key,
			kind:                      kind,
			hash:                      hash,
			maxConcurrency:            maxConcurrency,
			maxConcurrencyPerInstance: maxConcurrencyPerInstance,
			deprecationReason:         g.deprecation[key],
			channelBindings:           bindings,
		}
		return false, nil
	}
	sameKind := s.kind == kind
	sameHash := s.hash == hash
	sameCaps := s.maxConcurrency == maxConcurrency && s.maxConcurrencyPerInstance == maxConcurrencyPerInstance
	if sameKind && sameHash && sameCaps {
		return true, nil
	}
	if key.version != "unstable" {
		var rej error
		switch {
		case !sameKind:
			rej = fmt.Errorf("%s/%s already registered as %s; cannot re-register as %s",
				key.namespace, key.version, s.kind, kind)
		case !sameHash:
			rej = fmt.Errorf("%s/%s already registered with different schema hash; vN is locked once registered (cut v(N+1) for a new schema, or use unstable for the mutable slot)",
				key.namespace, key.version)
		default:
			rej = fmt.Errorf("%s/%s already registered with different concurrency caps (have max=%d/inst=%d, got max=%d/inst=%d)",
				key.namespace, key.version, s.maxConcurrency, s.maxConcurrencyPerInstance, maxConcurrency, maxConcurrencyPerInstance)
		}
		g.recordRejectedJoinLocked(key, s, rej.Error(), maxConcurrency, maxConcurrencyPerInstance)
		return false, rej
	}
	// unstable swap: validate cross-slot binding uniqueness *before*
	// eviction so a rejected swap leaves the prior slot intact. The
	// outgoing bindings (this slot's own) are skipped by the check
	// since they're about to be replaced.
	if err := g.checkCrossSlotBindingsLocked(key, bindings); err != nil {
		return false, err
	}
	// unstable swap: evict the prior occupant — including its kind-
	// specific storage if the kind changed — and install a fresh slot
	// record. The caller's creation path then builds a fresh per-kind
	// struct under the new shape. Old replicas tracked by their owner
	// registration get cleaned up lazily on Deregister /
	// heartbeat-eviction; conn refcounts (cp.conns standalone,
	// reconciler.conns cluster) decrement on the same path.
	g.evictSlotLocked(s)
	g.slots[key] = &slot{
		key:                       key,
		kind:                      kind,
		hash:                      hash,
		maxConcurrency:            maxConcurrency,
		maxConcurrencyPerInstance: maxConcurrencyPerInstance,
		channelBindings:           bindings,
	}
	return false, nil
}

// checkCrossSlotBindingsLocked enforces the pre-1.0 rule that no two
// `(namespace, version)` slots can claim the same channel binding
// pattern. Same-slot conflicts are out of scope here: the schema-hash
// collision rule already rejects same-`vN` rebakes that change the
// bound pattern set (changing a `(gwag.ps.binding)` option mutates the
// proto source bytes, which mutates the hash), and `unstable` rebakes
// install the new bindings wholesale.
//
// Caller holds g.mu. `key` is the slot key being registered; its own
// existing bindings (if any) are skipped so an unstable swap doesn't
// self-conflict on a pattern it already declares.
func (g *Gateway) checkCrossSlotBindingsLocked(key poolKey, bindings []ir.ChannelBinding) error {
	if len(bindings) == 0 {
		return nil
	}
	incoming := make(map[string]struct{}, len(bindings))
	for _, b := range bindings {
		incoming[b.Pattern] = struct{}{}
	}
	for otherKey, other := range g.slots {
		if otherKey == key {
			continue
		}
		for _, ob := range other.channelBindings {
			if _, hit := incoming[ob.Pattern]; hit {
				return fmt.Errorf("channel binding pattern %q already claimed by %s/%s; cannot also be claimed by %s/%s (cross-slot pattern conflict)",
					ob.Pattern, otherKey.namespace, otherKey.version, key.namespace, key.version)
			}
		}
	}
	return nil
}

// evictSlotLocked drops the per-kind dispatch storage and IR cache
// backing a slot. Used by unstable swaps when the prior occupant is
// being replaced. The slot index entry is overwritten by the swap
// path with the new occupant. Caller holds g.mu.
func (g *Gateway) evictSlotLocked(s *slot) {
	g.publishServiceChange(adminEventsActionDeregistered, s.key.namespace, s.key.version, "", 0)
	s.proto = nil
	s.openapi = nil
	s.graphql = nil
	s.internalProto = nil
	s.ir = nil
}

// releaseSlotLocked drops the slot index entry. Called by per-kind
// removal helpers (`removeReplicaByIDLocked`,
// `removeReplicasByOwnerLocked`, and the OpenAPI / GraphQL analogues)
// when a pool/source empties on the natural deregister path. Caller
// holds g.mu.
//
// Also clears any rejected-join counter for the key — the slot is
// gone, so a fresh registration with new caps will succeed; the
// stale-config triage signal isn't relevant anymore.
func (g *Gateway) releaseSlotLocked(key poolKey) {
	delete(g.slots, key)
	delete(g.rejectedJoins, key)
}

// rejectedJoinsSnapshot returns a copy of the per-slot rejection
// summaries safe to read outside g.mu (the values are pointers, but
// the per-slot struct is copied so a concurrent
// recordRejectedJoinLocked won't tear the read).
func (g *Gateway) rejectedJoinsSnapshot() map[poolKey]*rejectedJoinSummary {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.rejectedJoins) == 0 {
		return nil
	}
	out := make(map[poolKey]*rejectedJoinSummary, len(g.rejectedJoins))
	for k, v := range g.rejectedJoins {
		copy := *v
		out[k] = &copy
	}
	return out
}

// recordRejectedJoinLocked records a vN registerSlotLocked rejection
// against the key's running summary so /admin/services can surface
// "rejected N joins with caps X, currently running Y" without an
// operator having to profile to find the stale-config issue. Caller
// holds g.mu.
func (g *Gateway) recordRejectedJoinLocked(key poolKey, occupant *slot, reason string, attemptedMax, attemptedPerInst int) {
	if g.rejectedJoins == nil {
		g.rejectedJoins = map[poolKey]*rejectedJoinSummary{}
	}
	sum := g.rejectedJoins[key]
	if sum == nil {
		sum = &rejectedJoinSummary{}
		g.rejectedJoins[key] = sum
	}
	sum.Count++
	sum.LastReason = reason
	sum.LastUnixMs = time.Now().UnixMilli()
	sum.LastMaxConcurrency = attemptedMax
	sum.LastMaxConcurrencyPerInstance = attemptedPerInst
	if occupant != nil {
		sum.CurrentMaxConcurrency = occupant.maxConcurrency
		sum.CurrentMaxConcurrencyPerInstance = occupant.maxConcurrencyPerInstance
	}
}

// protoSlot returns the proto pool stored on the slot at `key`, or
// nil if no slot exists at `key` or the slot's kind is not proto.
// Caller holds g.mu.
func (g *Gateway) protoSlot(key poolKey) *pool {
	s := g.slots[key]
	if s == nil || s.kind != slotKindProto {
		return nil
	}
	return s.proto
}

// openAPISlot returns the OpenAPI source stored on the slot at `key`,
// or nil. Caller holds g.mu.
func (g *Gateway) openAPISlot(key poolKey) *openAPISource {
	s := g.slots[key]
	if s == nil || s.kind != slotKindOpenAPI {
		return nil
	}
	return s.openapi
}

// graphQLSlot returns the GraphQL source stored on the slot at `key`,
// or nil. Caller holds g.mu.
func (g *Gateway) graphQLSlot(key poolKey) *graphQLSource {
	s := g.slots[key]
	if s == nil || s.kind != slotKindGraphQL {
		return nil
	}
	return s.graphql
}

// internalProtoSlot returns the internal-proto source stored on the
// slot at `key`, or nil. Caller holds g.mu.
func (g *Gateway) internalProtoSlot(key poolKey) *internalProtoSource {
	s := g.slots[key]
	if s == nil || s.kind != slotKindInternalProto {
		return nil
	}
	return s.internalProto
}

// mcpSlot returns the MCP source stored on the slot at `key`, or nil.
// Caller holds g.mu.
func (g *Gateway) mcpSlot(key poolKey) *mcpSource {
	s := g.slots[key]
	if s == nil || s.kind != slotKindMCP {
		return nil
	}
	return s.mcp
}

// bakeSlotIRLocked fills `s.ir` from the slot's per-kind handle,
// applying the gateway's current schema-half transforms in the same
// order the legacy `*ServicesAsIRLocked` walks did:
//
//  1. Ingest the format handle into raw `[]*ir.Service`.
//  2. Stamp Namespace / Version / Internal on each.
//  3. Apply `HideInternal` (drops services whose namespace is `_*`
//     so they never reach the public schema).
//  4. Apply every Transform.Schema rewrite (HideType / HidePath /
//     NullableType / NullablePath — the IR-mutating injectors).
//  5. Inject HMAC subscription auth args (proto only).
//  6. Populate SchemaIDs so dispatchers can be looked up by
//     op.SchemaID at request time.
//
// Caller holds g.mu. The slot's per-kind handle (`s.proto` /
// `s.openapi` / `s.graphql`) must be set before calling.
func (g *Gateway) bakeSlotIRLocked(s *slot) {
	var raw []*ir.Service
	switch s.kind {
	case slotKindProto:
		if s.proto == nil {
			return
		}
		raw = ir.IngestProto(s.proto.file)
	case slotKindOpenAPI:
		if s.openapi == nil {
			return
		}
		raw = []*ir.Service{ir.IngestOpenAPI(s.openapi.doc)}
	case slotKindGraphQL:
		if s.graphql == nil {
			return
		}
		svc, err := ir.IngestGraphQL(s.graphql.rawIntrospection)
		if err != nil {
			s.ir = nil
			return
		}
		raw = []*ir.Service{svc}
	case slotKindInternalProto:
		if s.internalProto == nil {
			return
		}
		raw = ir.IngestProto(s.internalProto.file)
	case slotKindMCP:
		if s.mcp == nil {
			return
		}
		svc, err := ir.IngestMCP(s.mcp.rawTools)
		if err != nil {
			s.ir = nil
			return
		}
		raw = []*ir.Service{svc}
	default:
		return
	}
	for _, svc := range raw {
		svc.Namespace = s.key.namespace
		svc.Version = s.key.version
		svc.Internal = g.isInternal(s.key.namespace)
		svc.Deprecated = s.deprecationReason
		svc.ChannelBindings = s.channelBindings
	}
	raw = ir.HideInternal(raw)
	g.applySchemaRewrites(raw)
	for _, svc := range raw {
		ir.PopulateSchemaIDs(svc)
	}
	s.ir = raw
}

// rebakeAllSlotsLocked re-bakes every slot's IR. Use(...) calls this
// after appending to `g.transforms` so the cached IR reflects the
// new injector set without waiting for a register/dereg event.
// Caller holds g.mu.
func (g *Gateway) rebakeAllSlotsLocked() {
	for _, s := range g.slots {
		g.bakeSlotIRLocked(s)
	}
}

// collectSlotIRLocked walks every slot whose key matches `filter` and
// returns the concatenation of each slot's pre-baked IR. The result
// is the same shape the legacy `*ServicesAsIRLocked` functions
// produced — already post-transform, post-internal-filter, with
// SchemaIDs populated — but without the per-kind ingest each rebuild.
// Caller holds g.mu.
func (g *Gateway) collectSlotIRLocked(filter schemaFilter) []*ir.Service {
	out := make([]*ir.Service, 0, len(g.slots))
	for _, s := range g.slots {
		if !filter.matchPool(s.key) {
			continue
		}
		out = append(out, s.ir...)
	}
	return out
}
