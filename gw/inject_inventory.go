package gateway

import (
	"fmt"
	"runtime"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// injectorKind identifies which constructor produced an InjectorRecord.
type injectorKind string

const (
	injectorKindType   injectorKind = "type"
	injectorKindPath   injectorKind = "path"
	injectorKindHeader injectorKind = "header"
)

// injectorState captures the inventory entry's resolution against the
// live schema at the moment InjectorInventory() was called.
type injectorState string

const (
	// injectorStateActive: at least one schema landing matches (for
	// type-keyed and path-keyed) or — for header injectors — registration
	// is well-formed.
	injectorStateActive injectorState = "active"

	// injectorStateDormant: no schema landing currently resolves. The
	// rule activates if a future schema rebuild brings the target into
	// existence; harmless otherwise.
	injectorStateDormant injectorState = "dormant"
)

// injectorRecord is the registration-time view of one InjectType /
// InjectPath / InjectHeader call. Captured by the Inject* constructors
// and surfaced through Transform.Inventory + Gateway.InjectorInventory.
type injectorRecord struct {
	Kind injectorKind

	// Exactly one of the following identifies the rule, depending on
	// Kind:
	//   - Kind=InjectorKindType   → TypeName (IR-named-type, proto FullName for proto messages)
	//   - Kind=InjectorKindPath   → Path ("namespace.op.arg")
	//   - Kind=InjectorKindHeader → HeaderName (literal HTTP header / gRPC metadata key)
	TypeName   string
	Path       string
	HeaderName string

	// Hide reflects the visibility flag at registration. true means the
	// arg/header is stripped from the external schema; false means the
	// resolver inspects what the caller sent.
	Hide bool

	// Nullable reflects the Nullable(true) option (paired with
	// Hide(false) for InjectType/InjectPath; rejected for InjectHeader).
	Nullable bool

	// RegisteredAt is the user's call site (file:line + function name)
	// captured via runtime.Caller at construction time.
	RegisteredAt injectorFrame
}

// injectorFrame is one captured stack frame.
type injectorFrame struct {
	File     string
	Line     int
	Function string
}

func (f injectorFrame) String() string {
	if f.File == "" {
		return ""
	}
	if f.Function != "" {
		return fmt.Sprintf("%s:%d (%s)", f.File, f.Line, f.Function)
	}
	return fmt.Sprintf("%s:%d", f.File, f.Line)
}

// captureInjectorFrame walks `skip` frames above the caller to find the
// user's registration site. Returns a zeroed frame on lookup failure.
func captureInjectorFrame(skip int) injectorFrame {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return injectorFrame{}
	}
	fn := ""
	if f := runtime.FuncForPC(pc); f != nil {
		fn = f.Name()
	}
	return injectorFrame{File: file, Line: line, Function: fn}
}

// injectorLanding is one concrete arg/field/header that an inventory
// entry currently affects. Renderable as a row in the admin UI.
type injectorLanding struct {
	// Kind is "arg", "field", or "header".
	Kind string

	// For Kind=="arg": Namespace, Version, Op, ArgName populated.
	// For Kind=="field": Namespace, Version, TypeName, FieldName populated.
	// For Kind=="header": HeaderName populated.
	Namespace string
	Version   string
	Op        string
	TypeName  string
	FieldName string
	ArgName   string
	HeaderName string
}

// injectorEntry is one row of the admin inventory: an InjectorRecord
// plus its current schema landings + state.
type injectorEntry struct {
	injectorRecord
	State    injectorState
	Landings []injectorLanding
}

// injectorInventory enumerates every Inject* registration on this
// gateway, paired with where it currently lands in the live (un-rewritten)
// IR. Powers the admin inventory endpoint + UI tab.
//
// Walks pools / OpenAPI sources / GraphQL ingest sources to compute the
// pre-rewrite IR (so HidePath/HideType "landings" are visible — once
// rewrites apply, hidden args are gone and the inventory would lose its
// answer to "what got hidden, where?"). Caller need not hold g.mu.
func (g *Gateway) injectorInventory() ([]injectorEntry, error) {
	g.mu.Lock()
	svcs, err := g.collectIRRawLocked()
	records := collectInjectorRecords(g.transforms)
	g.mu.Unlock()
	if err != nil {
		return nil, err
	}

	entries := make([]injectorEntry, 0, len(records))
	for _, rec := range records {
		entry := injectorEntry{injectorRecord: rec}
		switch rec.Kind {
		case injectorKindType:
			entry.Landings = landingsForType(svcs, rec.TypeName)
		case injectorKindPath:
			entry.Landings = landingsForPath(svcs, rec.Path)
		case injectorKindHeader:
			entry.Landings = []injectorLanding{{Kind: "header", HeaderName: rec.HeaderName}}
		}
		entry.State = injectorStateDormant
		if len(entry.Landings) > 0 {
			entry.State = injectorStateActive
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// collectInjectorRecords flattens every Transform.Inventory entry in
// registration order. Caller holds g.mu.
func collectInjectorRecords(transforms []Transform) []injectorRecord {
	var out []injectorRecord
	for _, tx := range transforms {
		out = append(out, tx.inventory...)
	}
	return out
}

// collectIRRawLocked walks proto pools + OpenAPI + GraphQL sources and
// produces the raw (no rewrites applied) IR snapshot used by the
// inventory. Caller holds g.mu. Mirrors gatewayServicesAsIR but skips
// applySchemaRewrites + PopulateSchemaIDs (neither is needed for
// landing lookup, and rewrites would erase what we're trying to surface).
func (g *Gateway) collectIRRawLocked() ([]*ir.Service, error) {
	out := []*ir.Service{}

	for k, s := range g.slots {
		switch s.kind {
		case slotKindProto:
			if s.proto == nil {
				continue
			}
			svcs := ir.IngestProto(s.proto.file)
			for _, svc := range svcs {
				svc.Namespace = k.namespace
				svc.Version = k.version
				svc.Internal = g.isInternal(k.namespace)
				out = append(out, svc)
			}
		case slotKindOpenAPI:
			if s.openapi == nil {
				continue
			}
			svc := ir.IngestOpenAPI(s.openapi.doc)
			svc.Namespace = k.namespace
			svc.Version = k.version
			svc.Internal = g.isInternal(k.namespace)
			out = append(out, svc)
		case slotKindGraphQL:
			if s.graphql == nil {
				continue
			}
			svc, err := ir.IngestGraphQL(s.graphql.rawIntrospection)
			if err != nil {
				continue
			}
			svc.Namespace = k.namespace
			svc.Version = k.version
			svc.Internal = g.isInternal(k.namespace)
			out = append(out, svc)
		case slotKindInternalProto:
			if s.internalProto == nil {
				continue
			}
			svcs := ir.IngestProto(s.internalProto.file)
			for _, svc := range svcs {
				svc.Namespace = k.namespace
				svc.Version = k.version
				svc.Internal = g.isInternal(k.namespace)
				out = append(out, svc)
			}
		}
	}
	return out, nil
}

// landingsForType returns every (svc, op, arg) and (svc, type, field)
// in svcs whose IR-named type matches name. Walks the same shapes
// NullableTypeRewrite walks; sibling logic.
func landingsForType(svcs []*ir.Service, name string) []injectorLanding {
	if name == "" {
		return nil
	}
	var out []injectorLanding
	for _, svc := range svcs {
		// Args on top-level operations.
		for _, op := range svc.Operations {
			for _, a := range op.Args {
				if a.Type.IsNamed() && a.Type.Named == name {
					out = append(out, injectorLanding{
						Kind: "arg", Namespace: svc.Namespace, Version: svc.Version,
						Op: op.Name, ArgName: a.Name,
					})
				}
			}
		}
		// Args on grouped operations.
		for _, grp := range svc.Groups {
			collectTypeArgsInGroup(grp, svc, name, &out)
		}
		// Fields on object/input types.
		for _, t := range svc.Types {
			if t.TypeKind != ir.TypeObject && t.TypeKind != ir.TypeInput {
				continue
			}
			for _, f := range t.Fields {
				if f.Type.IsNamed() && f.Type.Named == name {
					out = append(out, injectorLanding{
						Kind: "field", Namespace: svc.Namespace, Version: svc.Version,
						TypeName: t.Name, FieldName: f.Name,
					})
				}
			}
		}
	}
	return out
}

func collectTypeArgsInGroup(grp *ir.OperationGroup, svc *ir.Service, name string, out *[]injectorLanding) {
	for _, op := range grp.Operations {
		for _, a := range op.Args {
			if a.Type.IsNamed() && a.Type.Named == name {
				*out = append(*out, injectorLanding{
					Kind: "arg", Namespace: svc.Namespace, Version: svc.Version,
					Op: op.Name, ArgName: a.Name,
				})
			}
		}
	}
	for _, sub := range grp.Groups {
		collectTypeArgsInGroup(sub, svc, name, out)
	}
}

// landingsForPath returns the (svc, op, arg) triples that "ns.op.arg"
// resolves to today, across every version of the namespace.
func landingsForPath(svcs []*ir.Service, path string) []injectorLanding {
	ns, opName, arg, ok := splitInjectPath(path)
	if !ok {
		return nil
	}
	var out []injectorLanding
	for _, svc := range svcs {
		if svc.Namespace != ns {
			continue
		}
		collectPathArgInOps(svc.Operations, svc, opName, arg, &out)
		for _, grp := range svc.Groups {
			collectPathArgInGroup(grp, svc, opName, arg, &out)
		}
	}
	return out
}

func collectPathArgInOps(ops []*ir.Operation, svc *ir.Service, opName, arg string, out *[]injectorLanding) {
	for _, op := range ops {
		if op.Name != opName {
			continue
		}
		for _, a := range op.Args {
			if a.Name == arg {
				*out = append(*out, injectorLanding{
					Kind: "arg", Namespace: svc.Namespace, Version: svc.Version,
					Op: op.Name, ArgName: a.Name,
				})
			}
		}
	}
}

func collectPathArgInGroup(grp *ir.OperationGroup, svc *ir.Service, opName, arg string, out *[]injectorLanding) {
	collectPathArgInOps(grp.Operations, svc, opName, arg, out)
	for _, sub := range grp.Groups {
		collectPathArgInGroup(sub, svc, opName, arg, out)
	}
}

// injectPathTransition is one InjectPath state change observed by
// evalInjectPathStatesLocked. Returned to callers (Use,
// assembleLocked) so logging stays separable from state tracking;
// tests inspect transitions without capturing stdout.
type injectPathTransition struct {
	Path     string
	Previous injectorState // zero value when Initial=true
	Current  injectorState
	Initial  bool // true on the first evaluation for Path
}

// evalInjectPathStatesLocked diff-checks every InjectPath rule in
// g.transforms against the live raw IR and returns the transitions
// to emit. The first evaluation surfaces a transition when a rule
// registers dormant (no schema landing matches); subsequent
// evaluations emit on dormant→active and active→dormant. Hooked
// from Use(...) (registration-time pass) and from assembleLocked
// (every schema rebuild).
//
// Skipped silently when collectIRRawLocked errors — the inventory
// endpoint surfaces the same data on demand, and a transient
// snapshot failure shouldn't block schema assembly. Caller holds
// g.mu.
func (g *Gateway) evalInjectPathStatesLocked() []injectPathTransition {
	svcs, err := g.collectIRRawLocked()
	if err != nil {
		return nil
	}
	if g.injectPathStates == nil {
		g.injectPathStates = map[string]injectorState{}
	}
	var transitions []injectPathTransition
	seen := map[string]bool{}
	for _, tx := range g.transforms {
		for _, rec := range tx.inventory {
			if rec.Kind != injectorKindPath || rec.Path == "" {
				continue
			}
			if seen[rec.Path] {
				continue
			}
			seen[rec.Path] = true

			current := injectorStateDormant
			if len(landingsForPath(svcs, rec.Path)) > 0 {
				current = injectorStateActive
			}
			previous, known := g.injectPathStates[rec.Path]
			switch {
			case !known:
				if current == injectorStateDormant {
					transitions = append(transitions, injectPathTransition{
						Path: rec.Path, Current: current, Initial: true,
					})
				}
			case previous != current:
				transitions = append(transitions, injectPathTransition{
					Path: rec.Path, Previous: previous, Current: current,
				})
			}
			g.injectPathStates[rec.Path] = current
		}
	}
	return transitions
}

// logInjectPathTransitions emits one log line per transition.
// Routed through the embedded NATS warn channel when a cluster is
// configured (mirrors warnSubscribeDelegateDeprecated); fmt.Println
// otherwise. Caller need not hold g.mu.
func (g *Gateway) logInjectPathTransitions(transitions []injectPathTransition) {
	for _, t := range transitions {
		var msg string
		switch {
		case t.Initial && t.Current == injectorStateDormant:
			msg = fmt.Sprintf("gateway: InjectPath(%q) registered dormant — no schema landing matches; rule activates if a future schema rebuild brings the path into existence.", t.Path)
		case t.Current == injectorStateActive:
			msg = fmt.Sprintf("gateway: InjectPath(%q) activated — schema rebuild brought the path into existence.", t.Path)
		case t.Current == injectorStateDormant:
			msg = fmt.Sprintf("gateway: InjectPath(%q) deactivated — schema rebuild removed the path; rule no-ops until it returns.", t.Path)
		default:
			continue
		}
		if g.cfg.cluster != nil {
			g.cfg.cluster.Server.Warnf("%s", msg)
		} else {
			fmt.Println(msg)
		}
	}
}
