package ir

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/IodeSystems/graphql-go/v2"
)

// RuntimeOptions configures RenderGraphQLRuntime.
//
// Stability: stable
type RuntimeOptions struct {
	JSONType           *graphql.Scalar
	LongType           *graphql.Scalar
	SharedProtoBuilder *IRTypeBuilder
	StableVN           map[string]int
	// UploadType is the GraphQL type used for IR args of TypeRef{Builtin:
	// ScalarUpload}. The gateway passes gw.UploadScalar() here; gat or
	// minimal embedders can omit it (then ScalarUpload falls back to
	// graphql.String, matching the ScalarBytes mapping).
	UploadType graphql.Output

	// AnnotationSink, when non-nil, is populated during the build with
	// the per-element SDL annotations keyed by their final (printed)
	// type/field names. Pass the same index to PrintSchemaSDL to emit
	// the directives + their declarations. Nil disables annotation
	// carriage into the GraphQL egress.
	AnnotationSink *AnnotationIndex

	// NonNullRequiredLists renders required *output* list fields as `[T!]!`
	// (see IRTypeBuilderOptions.NonNullRepeatedRequired). Pair with a
	// dispatch that coerces nil slices to `[]`.
	NonNullRequiredLists bool
}

// RenderGraphQLRuntime walks `svcs` into a fully-wired graphql.Schema.
//
// Stability: stable
func RenderGraphQLRuntime(svcs []*Service, registry *DispatchRegistry, opts RuntimeOptions) (*graphql.Schema, error) {
	queries, mutations, subs, err := RenderGraphQLRuntimeFields(svcs, registry, opts)
	if err != nil {
		return nil, err
	}

	if len(queries) == 0 {
		queries["_status"] = &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return "no services registered", nil
			},
		}
	}

	cfg := graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{Name: "Query", Fields: queries}),
	}
	if len(mutations) > 0 {
		cfg.Mutation = graphql.NewObject(graphql.ObjectConfig{Name: "Mutation", Fields: mutations})
	}
	if len(subs) > 0 {
		cfg.Subscription = graphql.NewObject(graphql.ObjectConfig{Name: "Subscription", Fields: subs})
	}
	schema, err := graphql.NewSchema(cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: graphql.NewSchema: %w", err)
	}
	return &schema, nil
}

// RenderGraphQLRuntimeFields returns the Query / Mutation /
// Subscription field maps.
//
// Stability: stable
func RenderGraphQLRuntimeFields(svcs []*Service, registry *DispatchRegistry, opts RuntimeOptions) (graphql.Fields, graphql.Fields, graphql.Fields, error) {
	if registry == nil {
		return nil, nil, nil, fmt.Errorf("runtime: nil DispatchRegistry")
	}

	byNS := map[string][]*Service{}
	for _, svc := range svcs {
		byNS[svc.Namespace] = append(byNS[svc.Namespace], svc)
	}
	namespaces := make([]string, 0, len(byNS))
	for ns := range byNS {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)

	queries := graphql.Fields{}
	mutations := graphql.Fields{}
	subs := graphql.Fields{}

	for _, ns := range namespaces {
		services := byNS[ns]
		sort.SliceStable(services, func(i, j int) bool {
			return ParseRuntimeVersionN(services[i].Version) < ParseRuntimeVersionN(services[j].Version)
		})
		latest := services[len(services)-1]
		latestReason := fmt.Sprintf("%s is current", latest.Version)

		builders := make(map[*Service]*IRTypeBuilder, len(services))
		for _, svc := range services {
			tb, err := newRuntimeTypeBuilder(svc, opts, svc == latest)
			if err != nil {
				return nil, nil, nil, err
			}
			builders[svc] = tb
		}

		stableSvc := pickStableSvc(services, opts.StableVN[ns])
		for _, kind := range []OpKind{OpQuery, OpMutation} {
			field, err := buildNamespaceFold(ns, services, latest, latestReason, builders, kind, registry, stableSvc)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("runtime: ns %s kind %v: %w", ns, kind, err)
			}
			if field == nil {
				continue
			}
			switch kind {
			case OpQuery:
				queries[ns] = field
			case OpMutation:
				mutations[ns] = field
			}
		}

		for _, svc := range services {
			isLatest := svc == latest
			autoReason := ""
			if !isLatest {
				autoReason = latestReason
			}
			depReason := CombineDepReason(svc.Deprecated, autoReason)
			prefix := ns + "_"
			if !isLatest {
				prefix = ns + "_" + svc.Version + "_"
			}
			if err := addSubscriptionFlat(subs, svc, builders[svc], prefix, depReason, registry); err != nil {
				return nil, nil, nil, fmt.Errorf("runtime: ns %s subscription: %w", ns, err)
			}
		}
		if stableSvc != nil {
			autoReason := ""
			if stableSvc != latest {
				autoReason = latestReason
			}
			depReason := CombineDepReason(stableSvc.Deprecated, autoReason)
			if err := addSubscriptionFlat(subs, stableSvc, builders[stableSvc], ns+"_stable_", depReason, registry); err != nil {
				return nil, nil, nil, fmt.Errorf("runtime: ns %s stable subscription: %w", ns, err)
			}
		}
	}

	return queries, mutations, subs, nil
}

// CombineDepReason returns the @deprecated reason: manual wins, auto is fallback.
//
// Stability: stable
func CombineDepReason(manual, auto string) string {
	if manual != "" {
		return manual
	}
	return auto
}

func buildNamespaceFold(ns string, services []*Service, latest *Service, latestReason string, builders map[*Service]*IRTypeBuilder, kind OpKind, registry *DispatchRegistry, stableSvc *Service) (*graphql.Field, error) {
	nsPath := pascalCaseRuntime(ns)
	kindSfx := kindSuffixForRuntime(kind)

	nsFields := graphql.Fields{}
	latestDep := CombineDepReason(latest.Deprecated, "")
	if err := addServiceContent(nsFields, latest, builders[latest], kind, nsPath, latestDep, registry); err != nil {
		return nil, fmt.Errorf("latest content: %w", err)
	}

	for _, svc := range services {
		isLatest := svc == latest
		autoReason := ""
		if !isLatest {
			autoReason = latestReason
		}
		depReason := CombineDepReason(svc.Deprecated, autoReason)
		verPath := nsPath + pascalCaseRuntime(svc.Version)
		verFields := graphql.Fields{}
		if err := addServiceContent(verFields, svc, builders[svc], kind, verPath, depReason, registry); err != nil {
			return nil, fmt.Errorf("version %s: %w", svc.Version, err)
		}
		if len(verFields) == 0 {
			continue
		}
		verContainer := graphql.NewObject(graphql.ObjectConfig{
			Name:   verPath + kindSfx + "Namespace",
			Fields: verFields,
		})
		verField := &graphql.Field{
			Type:    graphql.NewNonNull(verContainer),
			Resolve: func(rp graphql.ResolveParams) (any, error) { return struct{}{}, nil },
		}
		if depReason != "" {
			verField.DeprecationReason = depReason
		}
		if _, exists := nsFields[svc.Version]; exists {
			return nil, fmt.Errorf("version sub-group %q collides with op", svc.Version)
		}
		nsFields[svc.Version] = verField
	}

	if stableSvc != nil {
		autoReason := ""
		if stableSvc != latest {
			autoReason = latestReason
		}
		depReason := CombineDepReason(stableSvc.Deprecated, autoReason)
		stablePath := nsPath + "Stable"
		stableFields := graphql.Fields{}
		if err := addServiceContent(stableFields, stableSvc, builders[stableSvc], kind, stablePath, depReason, registry); err != nil {
			return nil, fmt.Errorf("stable: %w", err)
		}
		if len(stableFields) > 0 {
			stableContainer := graphql.NewObject(graphql.ObjectConfig{
				Name:   stablePath + kindSfx + "Namespace",
				Fields: stableFields,
			})
			stableField := &graphql.Field{
				Type:    graphql.NewNonNull(stableContainer),
				Resolve: func(rp graphql.ResolveParams) (any, error) { return struct{}{}, nil },
			}
			if depReason != "" {
				stableField.DeprecationReason = depReason
			}
			if _, exists := nsFields["stable"]; exists {
				return nil, fmt.Errorf("stable sub-group collides with op")
			}
			nsFields["stable"] = stableField
		}
	}

	if len(nsFields) == 0 {
		return nil, nil
	}

	nsContainer := graphql.NewObject(graphql.ObjectConfig{
		Name:   nsPath + kindSfx + "Namespace",
		Fields: nsFields,
	})
	return &graphql.Field{
		Type:    graphql.NewNonNull(nsContainer),
		Resolve: func(rp graphql.ResolveParams) (any, error) { return struct{}{}, nil },
	}, nil
}

func addServiceContent(dst graphql.Fields, svc *Service, tb *IRTypeBuilder, kind OpKind, parentPath, depReason string, registry *DispatchRegistry) error {
	rename := opNameForRuntime(svc)
	containerName := parentPath + kindSuffixForRuntime(kind) + "Namespace"
	for _, op := range svc.Operations {
		if op.Kind != kind {
			continue
		}
		if err := emitOperation(dst, op, tb, depReason, registry, rename, false, containerName); err != nil {
			return err
		}
	}
	for _, g := range svc.Groups {
		if g.Kind != kind {
			continue
		}
		if err := emitGroupContainer(dst, g, tb, kind, parentPath, depReason, registry, rename, false); err != nil {
			return err
		}
	}
	return nil
}

// emitGroupContainer renders one OperationGroup as a nested
// graphql.Object field.
//
// When the group is GraphQL-origin and we're at the top of the
// graphql chain (`inGraphQLGroup` arrives as false), the field's
// Resolve forwards the entire local sub-selection upstream as one
// call (via the graphqlGroupDispatcher registered at g.SchemaID)
// and returns the response map. Children are rendered with
// `inGraphQLGroup=true` so their own Resolve hooks are skipped —
// graphql-go's DefaultResolveFn dereferences from the parent's
// map[string]any source.
//
// For non-graphql groups (proto / openapi don't produce groups
// today, but the structural shape is preserved) and for inner
// passthrough levels, the field's Resolve returns `struct{}{}` so
// graphql-go drives into the inner Resolve chain — each leaf op
// owns its own dispatcher in that path.
func emitGroupContainer(dst graphql.Fields, g *OperationGroup, tb *IRTypeBuilder, kind OpKind, parentPath, depReason string, registry *DispatchRegistry, rename func(string) string, inGraphQLGroup bool) error {
	childPath := parentPath + pascalCaseRuntime(g.Name)
	// Once we cross into a graphql-origin group, every descendant
	// resolves via map dereference. If `inGraphQLGroup` is already
	// true (sub-group inside an outer graphql group), keep it; if
	// this group is itself graphql-origin, flip it for descendants.
	childInGroup := inGraphQLGroup || g.OriginKind == KindGraphQL
	containerName := childPath + kindSuffixForRuntime(kind) + "Namespace"
	fields := graphql.Fields{}
	for _, op := range g.Operations {
		if err := emitOperation(fields, op, tb, depReason, registry, rename, childInGroup, containerName); err != nil {
			return fmt.Errorf("group %s: %w", g.Name, err)
		}
	}
	for _, sub := range g.Groups {
		if err := emitGroupContainer(fields, sub, tb, kind, childPath, depReason, registry, rename, childInGroup); err != nil {
			return err
		}
	}
	if len(fields) == 0 {
		fields["_empty"] = &graphql.Field{Type: graphql.String}
	}
	container := graphql.NewObject(graphql.ObjectConfig{
		Name:        childPath + kindSuffixForRuntime(kind) + "Namespace",
		Description: g.Description,
		Fields:      fields,
	})
	field := &graphql.Field{
		Type:        graphql.NewNonNull(container),
		Description: g.Description,
	}
	switch {
	case g.OriginKind == KindGraphQL && !inGraphQLGroup:
		// Top of a graphql-origin chain: install the group dispatcher
		// so one upstream call captures the whole sub-selection.
		sid := g.SchemaID
		d := registry.Get(sid)
		_, canAppend := d.(AppendDispatcher)
		field.Resolve = func(rp graphql.ResolveParams) (any, error) {
			d := registry.Get(sid)
			if d == nil {
				return nil, fmt.Errorf("gateway: no dispatcher for group %s", sid)
			}
			ctx := rp.Context
			if ctx != nil {
				ctx = context.WithValue(ctx, graphqlResolveInfoKey{}, &rp.Info)
			}
			return d.Dispatch(ctx, rp.Args)
		}
		if canAppend {
			field.ResolveAppend = func(rp graphql.ResolveParams, dst []byte) ([]byte, error) {
				d := registry.Get(sid)
				ad, ok := d.(AppendDispatcher)
				if !ok {
					return dst, fmt.Errorf("gateway: group dispatcher %s lost AppendDispatcher capability", sid)
				}
				ctx := rp.Context
				if ctx != nil {
					ctx = context.WithValue(ctx, graphqlResolveInfoKey{}, &rp.Info)
				}
				return ad.DispatchAppend(ctx, rp.Args, dst)
			}
		}
	case inGraphQLGroup:
		// Sub-group inside an outer graphql group: dereference from
		// the parent map by the response key (alias-or-name) so
		// aliases at the group boundary round-trip correctly.
		// DefaultResolveFn keys by FieldName, which loses aliases.
		field.Resolve = graphqlGroupChildResolver
	default:
		// Proto/OpenAPI passthrough: each leaf op owns its own
		// dispatcher; this container just yields a sentinel so
		// graphql-go drives the inner resolvers.
		field.Resolve = func(rp graphql.ResolveParams) (any, error) { return struct{}{}, nil }
	}
	if depReason != "" {
		field.DeprecationReason = depReason
	}
	if _, exists := dst[g.Name]; exists {
		return fmt.Errorf("group field collision: %s", g.Name)
	}
	dst[g.Name] = field
	return nil
}

func emitOperation(dst graphql.Fields, op *Operation, tb *IRTypeBuilder, depReason string, registry *DispatchRegistry, rename func(string) string, inGraphQLGroup bool, containerName string) error {
	field, err := buildRuntimeOperation(tb, op, registry, inGraphQLGroup)
	if err != nil {
		return fmt.Errorf("op %s: %w", op.Name, err)
	}
	if depReason != "" {
		field.DeprecationReason = depReason
	}
	name := rename(op.Name)
	if _, exists := dst[name]; exists {
		return fmt.Errorf("op field collision: %s", name)
	}
	dst[name] = field
	tb.options.AnnotationSink.recordField(containerName, name, op.Annotations)
	return nil
}

func buildRuntimeOperation(tb *IRTypeBuilder, op *Operation, registry *DispatchRegistry, inGraphQLGroup bool) (*graphql.Field, error) {
	args := graphql.FieldConfigArgument{}
	for _, a := range op.Args {
		t, err := tb.Input(a.Type, a.Repeated, a.Required, a.ItemRequired)
		if err != nil {
			return nil, fmt.Errorf("arg %s: %w", a.Name, err)
		}
		args[a.Name] = &graphql.ArgumentConfig{
			Type:         t,
			Description:  a.Description,
			DefaultValue: a.Default,
		}
	}
	var out graphql.Output = graphql.String
	if op.Output != nil {
		o, err := tb.Output(*op.Output, op.OutputRepeated, op.OutputRequired, op.OutputItemRequired)
		if err != nil {
			return nil, fmt.Errorf("output: %w", err)
		}
		out = o
	}
	sid := op.SchemaID
	// Capability check at render time. Dispatchers are registered
	// before RenderGraphQLRuntimeFields runs (gw/schema.go's
	// assembleLocked sequences register*DispatchersLocked → render),
	// so the registry is fully populated by the time we get here.
	// Adopters with hot-reload semantics rebuild the schema after each
	// re-registration, so the registry state at render time matches
	// the request-time state until the next rebuild.
	d := registry.Get(sid)
	_, canAppend := d.(AppendDispatcher)
	if canAppend && outputHasAbstractType(out) && op.OriginKind == KindProto {
		// Abstract returns (Union / Interface) need __typename
		// resolution. graphql-origin dispatchers handle this naturally:
		// the upstream graphql server resolves __typename against its
		// schema, and prefixResponseTypenames in the gateway rewrites
		// the upstream type names to the local prefixed form before
		// emit. openAPI's append walker carries the IR Service +
		// type prefix so it can discriminate Union variants
		// (DiscriminatorProperty + Mapping, then a required-fields
		// heuristic) and emit __typename with the local prefix —
		// matching IRTypeBuilder.unionFor's ResolveType. Proto-origin
		// dispatchers haven't been updated to discriminate yet
		// (proto ingest doesn't synthesize TypeUnion today, but
		// adopters can construct them via IR transforms), so they
		// stay on the Dispatch path for now.
		canAppend = false
	}

	var dispatch graphql.FieldResolveFn
	var appendDispatch graphql.FieldResolveAppendFn
	if op.OriginKind == KindGraphQL {
		if canAppend {
			appendDispatch = func(rp graphql.ResolveParams, dst []byte) ([]byte, error) {
				d := registry.Get(sid)
				ad, ok := d.(AppendDispatcher)
				if !ok {
					return dst, fmt.Errorf("gateway: dispatcher %s lost AppendDispatcher capability", sid)
				}
				ctx := rp.Context
				if ctx != nil {
					ctx = context.WithValue(ctx, graphqlResolveInfoKey{}, &rp.Info)
				}
				return ad.DispatchAppend(ctx, rp.Args, dst)
			}
		}
		dispatch = func(rp graphql.ResolveParams) (any, error) {
			d := registry.Get(sid)
			if d == nil {
				return nil, fmt.Errorf("gateway: no dispatcher for %s", sid)
			}
			ctx := rp.Context
			if ctx != nil {
				ctx = context.WithValue(ctx, graphqlResolveInfoKey{}, &rp.Info)
			}
			return d.Dispatch(ctx, rp.Args)
		}
	} else {
		if canAppend {
			appendDispatch = func(rp graphql.ResolveParams, dst []byte) ([]byte, error) {
				d := registry.Get(sid)
				ad, ok := d.(AppendDispatcher)
				if !ok {
					return dst, fmt.Errorf("gateway: dispatcher %s lost AppendDispatcher capability", sid)
				}
				// Proto / OpenAPI append dispatchers need the local
				// selection AST to project the upstream response down
				// to just the selected fields. Plumb rp.Info via the
				// same context key the graphql-origin path uses.
				ctx := rp.Context
				if ctx != nil {
					ctx = context.WithValue(ctx, graphqlResolveInfoKey{}, &rp.Info)
				}
				return ad.DispatchAppend(ctx, rp.Args, dst)
			}
		}
		dispatch = func(rp graphql.ResolveParams) (any, error) {
			d := registry.Get(sid)
			if d == nil {
				return nil, fmt.Errorf("gateway: no dispatcher for %s", sid)
			}
			return d.Dispatch(rp.Context, rp.Args)
		}
	}
	if op.Kind == OpSubscription {
		return &graphql.Field{
			Type:              out,
			Args:              args,
			Description:       withRef(op.Description, op.Ref),
			DeprecationReason: op.Deprecated,
			Subscribe:         dispatch,
			Resolve: func(rp graphql.ResolveParams) (any, error) {
				return rp.Source, nil
			},
		}, nil
	}
	if inGraphQLGroup {
		// Parent graphql group dispatcher already forwarded the whole
		// sub-selection upstream and returned a map[string]any keyed
		// by alias-or-name (the response keys the upstream emitted;
		// we preserve aliases on the way out so same-field-different-
		// args round-trips correctly). graphqlGroupChildResolver keys
		// by FieldASTs[0].Alias.Value so aliases match; DefaultResolveFn
		// keys by FieldName and would miss aliased entries.
		return &graphql.Field{
			Type:              out,
			Args:              args,
			Description:       withRef(op.Description, op.Ref),
			DeprecationReason: op.Deprecated,
			Resolve:           graphqlGroupChildResolver,
		}, nil
	}
	return &graphql.Field{
		Type:              out,
		Args:              args,
		Description:       withRef(op.Description, op.Ref),
		DeprecationReason: op.Deprecated,
		Resolve:           dispatch,
		ResolveAppend:     appendDispatch,
	}, nil
}

// outputHasAbstractType walks a graphql.Output (unwrapping NonNull
// and List layers) and reports whether the underlying type is — or
// contains — a Union or Interface. The renderer uses this to decide
// whether ResolveAppend is safe for the field: those abstract types
// require graphql-go's __typename machinery to fire on per-field
// resolution, which ResolveAppend bypasses.
func outputHasAbstractType(o graphql.Output) bool {
	switch t := o.(type) {
	case *graphql.NonNull:
		return outputHasAbstractType(t.OfType)
	case *graphql.List:
		return outputHasAbstractType(t.OfType)
	case *graphql.Union, *graphql.Interface:
		return true
	}
	return false
}

// graphqlGroupChildResolver dereferences a field's value from the
// parent map[string]any source by the response key (alias-or-name).
// Wired by emitGroupContainer + buildRuntimeOperation when rendering
// descendants of a graphql-origin group whose top-level resolver
// already forwarded the whole sub-selection upstream and returned
// the response map.
//
// We key by FieldASTs[0].Alias.Value (falling back to FieldName when
// unaliased), matching the upstream response's shape because we
// forward the FieldAST verbatim with aliases preserved.
// DefaultResolveFn keys by FieldName and would drop aliased entries.
func graphqlGroupChildResolver(rp graphql.ResolveParams) (any, error) {
	src, ok := rp.Source.(map[string]any)
	if !ok {
		return nil, nil
	}
	key := rp.Info.FieldName
	if len(rp.Info.FieldASTs) > 0 {
		if alias := rp.Info.FieldASTs[0].Alias; alias != nil && alias.Value != "" {
			key = alias.Value
		}
	}
	return src[key], nil
}

// graphqlResolveInfoKey is a context key for passing graphql.ResolveInfo
// to dispatchers that need it (graphql-ingest forwarding).
type graphqlResolveInfoKey struct{}

// GraphQLResolveInfoFrom extracts the ResolveInfo from context, set by
// buildRuntimeOperation for KindGraphQL operations. Returns nil if absent.
//
// Stability: stable
func GraphQLResolveInfoFrom(ctx context.Context) *graphql.ResolveInfo {
	v, _ := ctx.Value(graphqlResolveInfoKey{}).(*graphql.ResolveInfo)
	return v
}

func addSubscriptionFlat(dst graphql.Fields, svc *Service, tb *IRTypeBuilder, prefix, depReason string, registry *DispatchRegistry) error {
	rename := opNameForRuntime(svc)
	for _, op := range svc.Operations {
		if op.Kind != OpSubscription {
			continue
		}
		f, err := buildRuntimeOperation(tb, op, registry, false)
		if err != nil {
			return fmt.Errorf("op %s: %w", op.Name, err)
		}
		if depReason != "" {
			f.DeprecationReason = depReason
		}
		name := prefix + rename(op.Name)
		if _, exists := dst[name]; exists {
			return fmt.Errorf("subscription field collision: %s", name)
		}
		dst[name] = f
		tb.options.AnnotationSink.recordField("Subscription", name, op.Annotations)
	}
	for _, g := range svc.Groups {
		if g.Kind != OpSubscription {
			continue
		}
		if err := flattenSubGroupWithPrefix(dst, g, prefix, depReason, registry, tb, rename); err != nil {
			return err
		}
	}
	return nil
}

func flattenSubGroupWithPrefix(dst graphql.Fields, g *OperationGroup, prefix, depReason string, registry *DispatchRegistry, tb *IRTypeBuilder, rename func(string) string) error {
	pre := prefix + g.Name + "_"
	for _, op := range g.Operations {
		f, err := buildRuntimeOperation(tb, op, registry, false)
		if err != nil {
			return fmt.Errorf("op %s: %w", op.Name, err)
		}
		if depReason != "" {
			f.DeprecationReason = depReason
		}
		name := pre + rename(op.Name)
		if _, exists := dst[name]; exists {
			return fmt.Errorf("subscription field collision: %s", name)
		}
		dst[name] = f
		tb.options.AnnotationSink.recordField("Subscription", name, op.Annotations)
	}
	for _, sub := range g.Groups {
		if err := flattenSubGroupWithPrefix(dst, sub, pre, depReason, registry, tb, rename); err != nil {
			return err
		}
	}
	return nil
}

func opNameForRuntime(svc *Service) func(string) string {
	if svc.OriginKind == KindProto {
		return lowerCamel
	}
	return identityName
}

func newRuntimeTypeBuilder(svc *Service, opts RuntimeOptions, isLatest bool) (*IRTypeBuilder, error) {
	switch svc.OriginKind {
	case KindProto:
		if opts.SharedProtoBuilder != nil {
			opts.SharedProtoBuilder.options.AnnotationSink = opts.AnnotationSink
			return opts.SharedProtoBuilder, nil
		}
		return NewIRTypeBuilder(svc, IRTypeNaming{
			ObjectName: exportedName,
			EnumName:   exportedName,
			UnionName:  exportedName,
			InputName:  func(s string) string { return exportedName(s) + "_Input" },
			FieldName:  lowerCamel,
		}, IRTypeBuilderOptions{
			UploadType:              opts.UploadType,
			AnnotationSink:          opts.AnnotationSink,
			NonNullRepeatedRequired: opts.NonNullRequiredLists,
		}), nil

	case KindOpenAPI:
		prefix := svc.Namespace + "_"
		if !isLatest && svc.Version != "" {
			prefix = svc.Namespace + "_" + svc.Version + "_"
		}
		naming := IRTypeNaming{
			ObjectName:    func(s string) string { return prefix + s },
			InputName:     func(s string) string { return prefix + s + "Input" },
			EnumName:      func(s string) string { return prefix + s },
			UnionName:     func(s string) string { return prefix + s },
			InterfaceName: func(s string) string { return prefix + s },
			ScalarName:    func(s string) string { return prefix + s },
			FieldName:     lowerCamel,
			// OpenAPI/JSON enum data carries the enum *name* (a string),
			// so the graphql-go runtime Value must be the name — not the
			// default ordinal Number, which no JSON value would match.
			EnumValueValue: func(v EnumValue) any { return v.Name },
		}
		return NewIRTypeBuilder(svc, naming, IRTypeBuilderOptions{
			Int64Type:               opts.LongType,
			UInt64Type:              opts.LongType,
			MapType:                 opts.JSONType,
			JSONType:                opts.JSONType,
			UploadType:              opts.UploadType,
			AnnotationSink:          opts.AnnotationSink,
			NonNullRepeatedRequired: opts.NonNullRequiredLists,
		}), nil

	case KindGraphQL:
		prefix := svc.Namespace + "_"
		if !isLatest && svc.Version != "" {
			prefix = svc.Namespace + "_" + svc.Version + "_"
		}
		naming := IRTypeNaming{
			ObjectName:     func(s string) string { return prefix + s },
			InputName:      func(s string) string { return prefix + s },
			EnumName:       func(s string) string { return prefix + s },
			UnionName:      func(s string) string { return prefix + s },
			InterfaceName:  func(s string) string { return prefix + s },
			ScalarName:     func(s string) string { return prefix + s },
			FieldName:      identityName,
			EnumValueName:  identityName,
			EnumValueValue: func(v EnumValue) any { return v.Name },
		}
		return NewIRTypeBuilder(svc, naming, IRTypeBuilderOptions{
			MapType:                 opts.JSONType,
			JSONType:                opts.JSONType,
			UploadType:              opts.UploadType,
			AnnotationSink:          opts.AnnotationSink,
			NonNullRepeatedRequired: opts.NonNullRequiredLists,
		}), nil

	case KindMCP:
		// Synthesised object / enum Types (nested tool-input schemas)
		// are namespace-prefixed so two upstream MCP servers can both
		// declare an "Address" without colliding. Tool args keep their
		// JSON Schema property names verbatim.
		prefix := svc.Namespace + "_"
		if !isLatest && svc.Version != "" {
			prefix = svc.Namespace + "_" + svc.Version + "_"
		}
		naming := IRTypeNaming{
			ObjectName:    func(s string) string { return prefix + s },
			InputName:     func(s string) string { return prefix + s + "Input" },
			EnumName:      func(s string) string { return prefix + s },
			UnionName:     func(s string) string { return prefix + s },
			InterfaceName: func(s string) string { return prefix + s },
			ScalarName:    func(s string) string { return prefix + s },
			FieldName:     identityName,
		}
		return NewIRTypeBuilder(svc, naming, IRTypeBuilderOptions{
			MapType:                 opts.JSONType,
			JSONType:                opts.JSONType,
			UploadType:              opts.UploadType,
			AnnotationSink:          opts.AnnotationSink,
			NonNullRepeatedRequired: opts.NonNullRequiredLists,
		}), nil

	default:
		return nil, fmt.Errorf("runtime: unsupported OriginKind %v on %s/%s", svc.OriginKind, svc.Namespace, svc.Version)
	}
}

func pickStableSvc(services []*Service, vN int) *Service {
	if vN <= 0 {
		return nil
	}
	for _, svc := range services {
		if ParseRuntimeVersionN(svc.Version) == vN {
			return svc
		}
	}
	return nil
}

// ParseRuntimeVersionN extracts a numeric version index from a
// "vN" / "N" / "" string. Empty / unparseable inputs return 0.
//
// Stability: stable
func ParseRuntimeVersionN(s string) int {
	if s == "" {
		return 0
	}
	digits := s
	if digits[0] == 'v' || digits[0] == 'V' {
		digits = digits[1:]
	}
	if n, err := strconv.Atoi(digits); err == nil {
		return n
	}
	return 0
}

func kindSuffixForRuntime(kind OpKind) string {
	switch kind {
	case OpMutation:
		return "Mutation"
	case OpSubscription:
		return "Subscription"
	}
	return "Query"
}

func pascalCaseRuntime(s string) string {
	if s == "" {
		return ""
	}
	r := []rune(s)
	if r[0] >= 'a' && r[0] <= 'z' {
		r[0] -= 'a' - 'A'
	}
	return string(r)
}
