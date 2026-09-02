package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/IodeSystems/graphql-go/v2"
	"github.com/IodeSystems/graphql-go/v2/language/ast"
	"github.com/IodeSystems/graphql-go/v2/language/printer"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// graphQLForwardInfoKey carries the resolver's ResolveInfo (FieldASTs +
// Operation + VariableValues) into the dispatcher. GraphQL→GraphQL
// forwarding can't reconstruct an equivalent upstream query from
// canonical args alone — it needs the user's selection-set so
// nested subselections forward verbatim. The other ingest paths
// (HTTP/JSON, gRPC) won't have this; the dispatcher falls back to a
// pre-printed canonicalQuery synthesized at construction time, when
// available.
type graphQLForwardInfoKey struct{}

func withGraphQLForwardInfo(ctx context.Context, info *graphql.ResolveInfo) context.Context {
	return context.WithValue(ctx, graphQLForwardInfoKey{}, info)
}

func graphQLForwardInfoFrom(ctx context.Context) *graphql.ResolveInfo {
	if v, _ := ctx.Value(graphQLForwardInfoKey{}).(*graphql.ResolveInfo); v != nil {
		return v
	}
	return ir.GraphQLResolveInfoFrom(ctx)
}

// withoutGraphQLForwardInfo overrides any existing forward-info on
// `ctx` with an empty *graphql.ResolveInfo so dispatchUnary's gate
// (`info != nil && len(info.FieldASTs) > 0`) falls through to the
// canonicalQuery / canonical-args dispatch path. Used by the
// cross-format runtime middleware wrapper: when a Runtime middleware
// is registered, the chain modifies the canonical args map and the
// inner dispatcher must read those modifications via vars rather
// than blind-forwarding the caller's AST (which would carry the
// pre-mutation literal). Side effect: per-request selection-set
// granularity is lost — the synthesized query uses scalar leaves
// only. Operators registering Runtime middleware accept this
// trade-off in exchange for the runtime hook firing.
func withoutGraphQLForwardInfo(ctx context.Context) context.Context {
	return context.WithValue(ctx, graphQLForwardInfoKey{}, &graphql.ResolveInfo{})
}

// graphQLDispatcher implements ir.Dispatcher for one downstream
// GraphQL source + one mirrored field. Everything inside the
// pre-cutover forwardingResolver closure (AST rewrite, doc print,
// pickReplica, dispatchGraphQL, response decode) lives here;
// backpressureMiddleware wraps the outside.
//
// canonicalQuery + canonicalArgNames are populated at construction
// for top-level (non-grouped, non-subscription) ops. They drive the
// canonical-args dispatch path used when no graphQLForwardInfo is in
// context (HTTP/JSON or gRPC ingress, where there is no caller AST).
// Grouped ops are skipped because the upstream's namespace-shaped
// nesting can't be re-synthesized from a leaf op alone.
type graphQLDispatcher struct {
	mirror          *graphQLMirror
	remoteFieldName string
	opLabel         string // "query" / "mutation" / "subscription"
	metrics         Metrics
	ns              string
	ver             string
	label           string // "<opLabel> <remoteFieldName>"

	canonicalQuery    string
	canonicalArgNames []string
}

func newGraphQLDispatcher(m *graphQLMirror, op *ir.Operation, opLabel string, metrics Metrics, isGrouped bool) *graphQLDispatcher {
	d := &graphQLDispatcher{
		mirror:          m,
		remoteFieldName: op.Name,
		opLabel:         opLabel,
		metrics:         metrics,
		ns:              m.src.namespace,
		ver:             m.src.version,
		label:           opLabel + " " + op.Name,
	}
	if !isGrouped && op.Kind != ir.OpSubscription {
		d.canonicalQuery, d.canonicalArgNames = buildGraphQLCanonicalQuery(m, op, opLabel)
	}
	return d
}

func (d *graphQLDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.graphql",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
	)
	defer span.End()
	if d.opLabel == "subscription" {
		return d.dispatchSubscribe(ctx)
	}
	return d.dispatchUnary(ctx, args)
}

func (d *graphQLDispatcher) dispatchUnary(ctx context.Context, args map[string]any) (any, error) {
	start := time.Now()
	record := func(err error) error {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return err
	}

	info := graphQLForwardInfoFrom(ctx)
	var printed string
	var vars map[string]any
	if info != nil && len(info.FieldASTs) > 0 {
		rewritten := d.mirror.rewriteFieldForRemote(info.FieldASTs[0], d.remoteFieldName)
		opType := ast.OperationTypeQuery
		var varDefs []*ast.VariableDefinition
		if op, ok := info.Operation.(*ast.OperationDefinition); ok && op != nil {
			if op.Operation == ast.OperationTypeMutation {
				opType = ast.OperationTypeMutation
			}
			varDefs = op.VariableDefinitions
		}
		opDef := ast.NewOperationDefinition(&ast.OperationDefinition{
			Operation:           opType,
			VariableDefinitions: varDefs,
			SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
				Selections: []ast.Selection{rewritten},
			}),
		})
		doc := ast.NewDocument(&ast.Document{Definitions: []ast.Node{opDef}})
		raw := printer.Print(doc)
		got, ok := raw.(string)
		if !ok {
			return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: printer returned %T (%v)", raw, raw)))
		}
		printed = got
		vars = info.VariableValues
	} else if d.canonicalQuery != "" {
		// HTTP/JSON or gRPC ingress: no AST, fall back to the
		// construction-time synthesized query. Variables are read out
		// of the canonical args map by IR Arg name (Op.Args index by
		// name = upstream variable name).
		printed = d.canonicalQuery
		if len(d.canonicalArgNames) > 0 && len(args) > 0 {
			vars = make(map[string]any, len(d.canonicalArgNames))
			for _, name := range d.canonicalArgNames {
				if v, ok := args[name]; ok {
					vars[name] = v
				}
			}
		}
	} else {
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no AST for %s", d.remoteFieldName)))
	}

	src := d.mirror.src
	r := src.pickReplica()
	if r == nil {
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no live replicas for %s", d.ns)))
	}
	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	resp, err := dispatchGraphQL(ctx, r.httpClient, r.endpoint, printed, vars, src.forwardHeaders)
	if err != nil {
		return nil, record(err)
	}
	if len(resp.Errors) > 0 {
		// Surface the first error back to the local client so they
		// see what the remote complained about. Application-level
		// remote errors classify as INTERNAL — there's no portable
		// status code in the GraphQL error envelope.
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql remote: %s", resp.Errors[0])))
	}
	var data map[string]any
	if len(resp.Data) > 0 {
		if err := jsonUnmarshalLoose(resp.Data, &data); err != nil {
			return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql decode data: %s", err.Error())))
		}
	}
	record(nil)
	return data[d.remoteFieldName], nil
}

// DispatchAppend is the byte-splice variant of dispatchUnary. Same
// upstream call, but extracts the per-field JSON without recursing
// into nested objects (single-level map[string]json.RawMessage) and
// appends the bytes directly to dst. Subscriptions don't take this
// path — the renderer only installs ResolveAppend for unary fields.
//
// The append path captures most of the per-request alloc reclaim
// from the projected wedge: ~970 allocs → ~200 on
// BenchmarkProtoSchemaExec by skipping the full-tree map walk that
// Dispatch performs.
func (d *graphQLDispatcher) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.graphql",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
	)
	defer span.End()
	if d.opLabel == "subscription" {
		return dst, fmt.Errorf("graphql ingest: DispatchAppend not supported for subscriptions")
	}

	start := time.Now()
	record := func(err error) error {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return err
	}

	info := graphQLForwardInfoFrom(ctx)
	var printed string
	var vars map[string]any
	if info != nil && len(info.FieldASTs) > 0 {
		rewritten := d.mirror.rewriteFieldForRemote(info.FieldASTs[0], d.remoteFieldName)
		opType := ast.OperationTypeQuery
		var varDefs []*ast.VariableDefinition
		if op, ok := info.Operation.(*ast.OperationDefinition); ok && op != nil {
			if op.Operation == ast.OperationTypeMutation {
				opType = ast.OperationTypeMutation
			}
			varDefs = op.VariableDefinitions
		}
		opDef := ast.NewOperationDefinition(&ast.OperationDefinition{
			Operation:           opType,
			VariableDefinitions: varDefs,
			SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
				Selections: []ast.Selection{rewritten},
			}),
		})
		doc := ast.NewDocument(&ast.Document{Definitions: []ast.Node{opDef}})
		raw := printer.Print(doc)
		got, ok := raw.(string)
		if !ok {
			return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: printer returned %T (%v)", raw, raw)))
		}
		printed = got
		vars = info.VariableValues
	} else if d.canonicalQuery != "" {
		printed = d.canonicalQuery
		if len(d.canonicalArgNames) > 0 && len(args) > 0 {
			vars = make(map[string]any, len(d.canonicalArgNames))
			for _, name := range d.canonicalArgNames {
				if v, ok := args[name]; ok {
					vars[name] = v
				}
			}
		}
	} else {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no AST for %s", d.remoteFieldName)))
	}

	src := d.mirror.src
	r := src.pickReplica()
	if r == nil {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no live replicas for %s", d.ns)))
	}
	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	resp, err := dispatchGraphQL(ctx, r.httpClient, r.endpoint, printed, vars, src.forwardHeaders)
	if err != nil {
		return dst, record(err)
	}
	if len(resp.Errors) > 0 {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql remote: %s", resp.Errors[0])))
	}
	if len(resp.Data) == 0 {
		record(nil)
		return append(dst, "null"...), nil
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql decode data: %s", err.Error())))
	}
	rawField, ok := data[d.remoteFieldName]
	if !ok || len(rawField) == 0 {
		record(nil)
		return append(dst, "null"...), nil
	}
	// Re-prefix upstream __typename values so the local response
	// shows the gateway-prefixed type name (matches the local
	// schema). graphql-go's __typename handling runs on the per-
	// field-resolver path, which ResolveAppend bypasses.
	rawField = d.mirror.prefixResponseTypenames(rawField)
	record(nil)
	return append(dst, rawField...), nil
}

var _ ir.AppendDispatcher = (*graphQLDispatcher)(nil)

// graphqlGroupDispatcher forwards an entire local sub-selection rooted
// at a GraphQL-origin OperationGroup as a single upstream request.
// Wired by `emitGroupContainer` for graphql-origin groups so the
// caller-side query `{ ns { greeter { hello echo } } }` becomes one
// upstream POST `{ greeter { hello echo } }` instead of N round
// trips per leaf — preserves GraphQL's batching property across
// sibling fields under a namespace.
//
// One graphqlGroupDispatcher per top-level group per source. Nested
// sub-groups don't get their own dispatcher; they're folded into the
// parent group's sub-selection and dereferenced from the response
// map via graphql-go's DefaultResolveFn.
//
// canonical-args ingress (HTTP/JSON, gRPC) doesn't reach this
// dispatcher — the per-leaf graphQLDispatcher still owns those
// ingress paths and falls back to its "no AST" error for grouped
// ops. That's a separate limitation worth fixing if HTTP/gRPC clients
// need to reach a namespace-grouped GraphQL upstream.
type graphqlGroupDispatcher struct {
	mirror  *graphQLMirror
	group   *ir.OperationGroup
	metrics Metrics
	ns      string
	ver     string
	label   string // e.g. "query greeter"
}

func newGraphqlGroupDispatcher(m *graphQLMirror, group *ir.OperationGroup, metrics Metrics) *graphqlGroupDispatcher {
	label := "query"
	if group.Kind == ir.OpMutation {
		label = "mutation"
	}
	return &graphqlGroupDispatcher{
		mirror:  m,
		group:   group,
		metrics: metrics,
		ns:      m.src.namespace,
		ver:     m.src.version,
		label:   label + " " + group.Name,
	}
}

func (d *graphqlGroupDispatcher) Dispatch(ctx context.Context, _ map[string]any) (any, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.graphql",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
	)
	defer span.End()

	start := time.Now()
	record := func(err error) error {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return err
	}

	info := graphQLForwardInfoFrom(ctx)
	if info == nil || len(info.FieldASTs) == 0 {
		// Canonical-args ingress reaches this only when an HTTP/gRPC
		// client targets a leaf op inside a graphql-origin group —
		// surface the gap rather than fabricate a selection.
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no AST for group %s (canonical-args ingress on a nested-namespace GraphQL upstream)", d.group.Name)))
	}

	// Build the upstream field: same name as the local group field
	// (graphql-origin: local and remote share the namespace path
	// structure post-introspection), with the local sub-selection
	// rewritten via the mirror so inline-fragment type conditions get
	// unprefixed.
	localField := info.FieldASTs[0]
	groupField := ast.NewField(&ast.Field{
		Alias:      localField.Alias,
		Name:       ast.NewName(&ast.Name{Value: d.group.Name}),
		Arguments:  localField.Arguments,
		Directives: localField.Directives,
	})
	if localField.SelectionSet != nil {
		groupField.SelectionSet = d.mirror.rewriteSelectionSet(localField.SelectionSet)
	}

	opType := ast.OperationTypeQuery
	var varDefs []*ast.VariableDefinition
	if op, ok := info.Operation.(*ast.OperationDefinition); ok && op != nil {
		if op.Operation == ast.OperationTypeMutation {
			opType = ast.OperationTypeMutation
		}
		varDefs = op.VariableDefinitions
	}
	opDef := ast.NewOperationDefinition(&ast.OperationDefinition{
		Operation:           opType,
		VariableDefinitions: varDefs,
		SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
			Selections: []ast.Selection{groupField},
		}),
	})
	doc := ast.NewDocument(&ast.Document{Definitions: []ast.Node{opDef}})
	raw := printer.Print(doc)
	printed, ok := raw.(string)
	if !ok {
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: printer returned %T (%v)", raw, raw)))
	}

	src := d.mirror.src
	r := src.pickReplica()
	if r == nil {
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no live replicas for %s", d.ns)))
	}
	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	resp, err := dispatchGraphQL(ctx, r.httpClient, r.endpoint, printed, info.VariableValues, src.forwardHeaders)
	if err != nil {
		return nil, record(err)
	}
	if len(resp.Errors) > 0 {
		return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql remote: %s", resp.Errors[0])))
	}
	var data map[string]any
	if len(resp.Data) > 0 {
		if err := jsonUnmarshalLoose(resp.Data, &data); err != nil {
			return nil, record(Reject(CodeInternal, fmt.Sprintf("graphql decode data: %s", err.Error())))
		}
	}
	record(nil)
	// Response key is the alias if present, else the field name.
	respKey := d.group.Name
	if localField.Alias != nil && localField.Alias.Value != "" {
		respKey = localField.Alias.Value
	}
	return data[respKey], nil
}

// DispatchAppend emits the upstream's response data for this group
// field directly into dst, skipping the full-tree unmarshal that
// Dispatch performs. The upstream's JSON for `data["greeter"]` is
// already exactly the shape this field's value should take in the
// local response (we forward the LOCAL selection upstream, so
// projection matches by construction), so we splice the bytes
// verbatim.
//
// One outer Unmarshal into map[string]json.RawMessage keeps the
// values as []byte without recursing into nested objects — saves
// the full-tree map allocation that Dispatch performs. The
// AppendDispatcher caller (graphql-go's writePlannedField) drops
// us into the response buffer at the right offset.
func (d *graphqlGroupDispatcher) DispatchAppend(ctx context.Context, _ map[string]any, dst []byte) ([]byte, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.graphql",
		namespaceAttr(d.ns),
		versionAttr(d.ver),
		methodAttr(d.label),
	)
	defer span.End()

	start := time.Now()
	record := func(err error) error {
		elapsed := time.Since(start)
		d.metrics.RecordDispatch(ctx, d.ns, d.ver, d.label, elapsed, err)
		addDispatchTime(ctx, elapsed)
		return err
	}

	info := graphQLForwardInfoFrom(ctx)
	if info == nil || len(info.FieldASTs) == 0 {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no AST for group %s (canonical-args ingress on a nested-namespace GraphQL upstream)", d.group.Name)))
	}

	localField := info.FieldASTs[0]
	groupField := ast.NewField(&ast.Field{
		Alias:      localField.Alias,
		Name:       ast.NewName(&ast.Name{Value: d.group.Name}),
		Arguments:  localField.Arguments,
		Directives: localField.Directives,
	})
	if localField.SelectionSet != nil {
		groupField.SelectionSet = d.mirror.rewriteSelectionSet(localField.SelectionSet)
	}

	opType := ast.OperationTypeQuery
	var varDefs []*ast.VariableDefinition
	if op, ok := info.Operation.(*ast.OperationDefinition); ok && op != nil {
		if op.Operation == ast.OperationTypeMutation {
			opType = ast.OperationTypeMutation
		}
		varDefs = op.VariableDefinitions
	}
	opDef := ast.NewOperationDefinition(&ast.OperationDefinition{
		Operation:           opType,
		VariableDefinitions: varDefs,
		SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
			Selections: []ast.Selection{groupField},
		}),
	})
	doc := ast.NewDocument(&ast.Document{Definitions: []ast.Node{opDef}})
	raw := printer.Print(doc)
	printed, ok := raw.(string)
	if !ok {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: printer returned %T (%v)", raw, raw)))
	}

	src := d.mirror.src
	r := src.pickReplica()
	if r == nil {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql ingest: no live replicas for %s", d.ns)))
	}
	r.inflight.Add(1)
	defer r.inflight.Add(-1)
	resp, err := dispatchGraphQL(ctx, r.httpClient, r.endpoint, printed, info.VariableValues, src.forwardHeaders)
	if err != nil {
		return dst, record(err)
	}
	if len(resp.Errors) > 0 {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql remote: %s", resp.Errors[0])))
	}

	// One-level unmarshal: keep each field value as raw JSON bytes
	// rather than recursing into nested maps. Savings vs Dispatch's
	// full-tree map[string]any unmarshal scale with response size.
	respKey := d.group.Name
	if localField.Alias != nil && localField.Alias.Value != "" {
		respKey = localField.Alias.Value
	}
	if len(resp.Data) == 0 {
		record(nil)
		return append(dst, "null"...), nil
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return dst, record(Reject(CodeInternal, fmt.Sprintf("graphql decode data: %s", err.Error())))
	}
	raw2, ok := data[respKey]
	if !ok || len(raw2) == 0 {
		record(nil)
		return append(dst, "null"...), nil
	}
	raw2 = d.mirror.prefixResponseTypenames(raw2)
	record(nil)
	return append(dst, raw2...), nil
}

// Compile-time assertion: graphqlGroupDispatcher implements both
// ir.Dispatcher and ir.AppendDispatcher.
var _ ir.AppendDispatcher = (*graphqlGroupDispatcher)(nil)

// buildGraphQLCanonicalQuery synthesizes a printable graphql operation
// for canonical-args dispatch. Returns ("", nil) when synthesis can't
// produce something the upstream will accept (output type missing
// from introspection); the dispatcher falls back to the "no AST"
// error in that case so the gap is visible rather than silently
// returning nulls.
//
// Variable names match IR Arg names (canonical-args dispatch keys
// the args map the same way). Selection set: scalar/enum fields on
// the unwrapped output object plus __typename; fields with required
// arguments are skipped (we have nothing to bind them to). Union /
// interface output types degrade to __typename only.
func buildGraphQLCanonicalQuery(m *graphQLMirror, op *ir.Operation, opLabel string) (string, []string) {
	intro := m.src.introspection
	selection := buildCanonicalSelectionSet(intro, op.Output)
	// Object/Interface/Union output with no selectable fields means
	// we can't synthesize a valid query — graphql requires a
	// non-empty selection set on composite return types.
	if selection == "" && op.Output != nil && op.Output.IsNamed() {
		if t := intro.Types[op.Output.Named]; t != nil {
			switch t.Kind {
			case "OBJECT", "INTERFACE", "UNION":
				return "", nil
			}
		}
	}
	var b strings.Builder
	b.WriteString(opLabel)
	argNames := make([]string, 0, len(op.Args))
	if len(op.Args) > 0 {
		b.WriteString("(")
		for i, a := range op.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("$")
			b.WriteString(a.Name)
			b.WriteString(": ")
			b.WriteString(graphQLArgTypeString(a))
			argNames = append(argNames, a.Name)
		}
		b.WriteString(")")
	}
	b.WriteString(" { ")
	b.WriteString(op.Name)
	if len(op.Args) > 0 {
		b.WriteString("(")
		for i, a := range op.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(a.Name)
			b.WriteString(": $")
			b.WriteString(a.Name)
		}
		b.WriteString(")")
	}
	if selection != "" {
		b.WriteString(" ")
		b.WriteString(selection)
	}
	b.WriteString(" }")
	return b.String(), argNames
}

func graphQLArgTypeString(a *ir.Arg) string {
	inner := graphQLTypeNameForRef(a.Type)
	if a.Repeated {
		item := inner
		if a.ItemRequired {
			item += "!"
		}
		listed := "[" + item + "]"
		if a.Required {
			listed += "!"
		}
		return listed
	}
	if a.Required {
		return inner + "!"
	}
	return inner
}

func graphQLTypeNameForRef(r ir.TypeRef) string {
	if r.IsNamed() {
		return r.Named
	}
	switch r.Builtin {
	case ir.ScalarBool:
		return "Boolean"
	case ir.ScalarInt32, ir.ScalarUInt32, ir.ScalarInt64, ir.ScalarUInt64:
		return "Int"
	case ir.ScalarFloat, ir.ScalarDouble:
		return "Float"
	case ir.ScalarID:
		return "ID"
	case ir.ScalarTimestamp, ir.ScalarBytes, ir.ScalarString, ir.ScalarUnknown:
		return "String"
	}
	return "String"
}

// buildCanonicalSelectionSet returns "{ field1 field2 __typename }"
// for object/interface output types, "{ __typename }" for unions, and
// "" for scalars/enums (no selection set required) or missing types.
// Fields with required arguments are skipped — canonical-args dispatch
// has nothing to bind them to.
func buildCanonicalSelectionSet(intro *introspectionSchema, out *ir.TypeRef) string {
	if out == nil || intro == nil {
		return ""
	}
	if out.IsBuiltin() || !out.IsNamed() {
		return ""
	}
	t := intro.Types[out.Named]
	if t == nil {
		return ""
	}
	switch t.Kind {
	case "SCALAR", "ENUM":
		return ""
	case "OBJECT", "INTERFACE":
		fields := []string{"__typename"}
		for _, f := range t.Fields {
			if !introspectionFieldHasOnlyOptionalArgs(f) {
				continue
			}
			if isLeafIntrospectionType(f.Type) {
				fields = append(fields, f.Name)
			}
		}
		return "{ " + strings.Join(fields, " ") + " }"
	case "UNION":
		return "{ __typename }"
	}
	return ""
}

// introspectionFieldHasOnlyOptionalArgs reports whether every arg on
// the field is nullable. A canonical-args selection set can't supply
// arguments for nested fields (we only have args for the top op), so
// fields with NON_NULL args have to be skipped.
func introspectionFieldHasOnlyOptionalArgs(f *introspectionField) bool {
	for _, a := range f.Args {
		if a.Type != nil && a.Type.Kind == "NON_NULL" {
			return false
		}
	}
	return true
}

func isLeafIntrospectionType(r *introspectionTypeRef) bool {
	cur := r
	for cur != nil {
		switch cur.Kind {
		case "NON_NULL", "LIST":
			cur = cur.OfType
		case "SCALAR", "ENUM":
			return true
		default:
			return false
		}
	}
	return false
}

// dispatchSubscribe opens a multiplexed subscription against the
// upstream and returns a chan any of pre-plucked frames. The
// renderer's Subscribe path treats the dispatcher result as the
// subscribe channel; its Resolve closure surfaces rp.Source directly,
// so this goroutine plucks `frame.Result[remoteFieldName]` before
// emitting (matching the pre-cutover subscribingResolver shape where
// the local Resolve picked the field out of the data envelope).
//
// Backpressure middleware is intentionally not wrapped around the
// subscribe path — src.sem is per-source unary capacity, not stream
// lifetime. The pre-cutover subscribingResolver bypassed
// backpressureMiddleware for the same reason; the multiplexer broker
// is the rate-control story for streams.
func (d *graphQLDispatcher) dispatchSubscribe(ctx context.Context) (any, error) {
	info := graphQLForwardInfoFrom(ctx)
	if info == nil || len(info.FieldASTs) == 0 {
		return nil, fmt.Errorf("graphql ingest: no AST for subscription %s", d.remoteFieldName)
	}
	rewritten := d.mirror.rewriteFieldForRemote(info.FieldASTs[0], d.remoteFieldName)
	var varDefs []*ast.VariableDefinition
	if op, ok := info.Operation.(*ast.OperationDefinition); ok && op != nil {
		varDefs = op.VariableDefinitions
	}
	opDef := ast.NewOperationDefinition(&ast.OperationDefinition{
		Operation:           ast.OperationTypeSubscription,
		VariableDefinitions: varDefs,
		SelectionSet: ast.NewSelectionSet(&ast.SelectionSet{
			Selections: []ast.Selection{rewritten},
		}),
	})
	doc := ast.NewDocument(&ast.Document{Definitions: []ast.Node{opDef}})
	raw := printer.Print(doc)
	printed, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("graphql ingest: printer returned %T (%v)", raw, raw)
	}

	src := d.mirror.src
	r := src.pickReplica()
	if r == nil {
		return nil, fmt.Errorf("graphql ingest: no live replicas for %s", src.namespace)
	}
	broker := src.getSubBroker()
	upstream, release, err := broker.acquire(ctx, r.endpoint, printed, info.VariableValues, src.forwardHeaders)
	if err != nil {
		return nil, err
	}
	out := make(chan any, 8)
	remote := d.remoteFieldName
	go func() {
		defer close(out)
		defer release()
		for {
			select {
			case f, ok := <-upstream:
				if !ok {
					return
				}
				if f == nil {
					continue
				}
				if len(f.Errors) > 0 {
					select {
					case out <- fmt.Errorf("graphql remote: %s", f.Errors[0]):
					case <-ctx.Done():
						return
					}
					if f.Done {
						return
					}
					continue
				}
				if f.Result != nil {
					select {
					case out <- f.Result[remote]:
					case <-ctx.Done():
						return
					}
				}
				if f.Done {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// graphQLBackpressureConfig bundles a graphql source's per-dispatch
// knobs for backpressureMiddleware. Sibling of pool / openAPI
// equivalents.
func graphQLBackpressureConfig(src *graphQLSource, label string, metrics Metrics, bp BackpressureOptions) backpressureConfig {
	return backpressureConfig{
		Sem:         src.sem,
		Queueing:    &src.queueing,
		MaxWaitTime: bp.MaxWaitTime,
		Metrics:     metrics,
		Namespace:   src.namespace,
		Version:     src.version,
		Label:       label,
		Kind:        "unary",
	}
}

var _ ir.Dispatcher = (*graphQLDispatcher)(nil)
