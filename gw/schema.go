package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// assembleLocked builds the gateway's canonical (unfiltered) GraphQL
// schema and atomically swaps it into g.schema. Caller holds g.mu.
//
// The DispatchRegistry is rebuilt fresh: every schema rebuild
// re-creates dispatchers (because the user runtime middleware chain
// can change between rebuilds), so any stale entries from a prior
// rebuild must go. Filtered schema builds (via /api/schema/graphql
// `?service=`) reuse this same registry — the per-call schemas
// produced by buildSchemaLocked share dispatcher state across all
// callers since the dispatcher's runtime behavior is independent of
// which subset is rendered.
func (g *Gateway) assembleLocked() error {
	g.dispatchers = ir.NewDispatchRegistry()
	schema, annotations, err := g.buildSchemaLocked(schemaFilter{})
	if err != nil {
		return err
	}
	g.schema.Store(schema)
	g.schemaAnnotations.Store(annotations)
	if !g.cfg.disableGraphiQL {
		g.graphiqlHandler.Store(newGraphiqlServer(schema))
	}
	g.rebuildIngressLocked()
	g.rebuildGRPCIngressLocked()
	g.rebuildChannelBindingIndexLocked()
	// Re-check InjectPath landings against the rebuilt IR so any
	// dormant→active or active→dormant transition surfaces exactly
	// once. Same evaluator runs on Use(...) for the registration-time
	// warning.
	transitions := g.evalInjectPathStatesLocked()
	g.logInjectPathTransitions(transitions)
	return nil
}

// buildSchemaLocked walks the registered pools / OpenAPI sources /
// downstream-GraphQL sources matching `filter` and produces a fresh
// graphql.Schema. An empty filter (zero value) matches everything —
// that's what assembleLocked uses. /schema/graphql passes a populated
// filter so codegen consumers can fetch a single namespace's slice
// without rebuilding the gateway-wide schema.
//
// Caller holds g.mu. Returns the built schema + its SDL annotation
// index; does NOT store them.
func (g *Gateway) buildSchemaLocked(filter schemaFilter) (*graphql.Schema, *ir.AnnotationIndex, error) {
	hidesSet := map[string]bool{}
	for _, name := range g.hiddenTypeNames() {
		hidesSet[name] = true
	}

	// Share one *IRTypeBuilder across every proto pool so cyclic
	// message refs collapse to a single *graphql.Object across Query,
	// Mutation, and Subscription roots — and so v1 + v2 of the same
	// proto package don't trip graphql-go's duplicate-named-type
	// rejection (proto FullNames are globally unique, so a single
	// merged Types map is collision-free).
	protoTB := newProtoIRTypeBuilder(g.slots, hidesSet, UploadScalar())

	rootFields := graphql.Fields{}

	// Proto + OpenAPI + GraphQL ingest all go through
	// RenderGraphQLRuntimeFields in a single call so the multi-version
	// fold sees every service in a namespace together (avoiding
	// separate render calls that would each emit a `<ns>` root field
	// and collide on merge). Per-kind dispatcher registration runs
	// first, then service distillation, then the render.
	// IR is pre-baked on each slot (see bakeSlotIRLocked). Schema
	// rebuild is now a single iteration over g.slots — no per-kind
	// walk, no transform pass.
	allSvcs := g.collectSlotIRLocked(filter)
	g.registerProtoDispatchersLocked(filter)
	if err := g.registerOpenAPIDispatchersLocked(allSvcs); err != nil {
		return nil, nil, err
	}
	if err := g.registerGraphQLDispatchersLocked(allSvcs); err != nil {
		return nil, nil, err
	}
	g.registerMCPDispatchersLocked(allSvcs)
	long, jsonScalar := openAPISharedScalars()
	var stableSnap map[string]int
	if g.effectiveAllowedTiers().Stable {
		stableSnap = g.stableSnapshotLocked()
	}
	annotations := ir.NewAnnotationIndex()
	queries, mutations, runtimeSubs, err := ir.RenderGraphQLRuntimeFields(allSvcs, g.dispatchers, ir.RuntimeOptions{
		SharedProtoBuilder: protoTB,
		LongType:           long,
		JSONType:           jsonScalar,
		StableVN:           stableSnap,
		UploadType:         UploadScalar(),
		AnnotationSink:     annotations,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("runtime render: %w", err)
	}
	for k, v := range queries {
		rootFields[k] = v
	}

	if len(rootFields) == 0 {
		rootFields["_status"] = &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return "no services registered", nil
			},
		}
	}

	queryObj := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: rootFields,
	})

	cfg := graphql.SchemaConfig{
		Query: queryObj,
		// Force-include the `Upload` scalar so SDL + introspection
		// expose it even before any ingested field is typed Upload.
		// Clients declaring `mutation ($file: Upload!) { … }` against
		// an upload-capable field rely on the scalar being a known
		// named type in the schema; without this it'd only appear
		// once a downstream service binds a multipart body to a field
		// (which lands with chunk 3).
		Types: []graphql.Type{UploadScalar()},
	}

	mutationFields := mutations
	if len(mutationFields) > 0 {
		cfg.Mutation = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutationFields,
		})
	}

	// Subscriptions: every kind (proto server-streaming, graphql-ingest
	// subscriptions) renders through RenderGraphQLRuntimeFields above.
	// OpenAPI has no subscription concept.
	if len(runtimeSubs) > 0 {
		cfg.Subscription = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Subscription",
			Fields: runtimeSubs,
		})
	}

	schema, err := graphql.NewSchema(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("graphql.NewSchema: %w", err)
	}
	return &schema, annotations, nil
}

// waitForSlot blocks until sem has capacity or the per-dispatch
// MaxWaitTime budget expires. Returns the dwell time and an error
// when the wait times out (or the request context cancels). Used for
// both unary in-flight slots and stream slots.
func waitForSlot(ctx context.Context, sem chan struct{}, maxWait time.Duration) (time.Duration, error) {
	start := time.Now()
	if maxWait <= 0 {
		select {
		case sem <- struct{}{}:
			return time.Since(start), nil
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		}
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case sem <- struct{}{}:
		return time.Since(start), nil
	case <-timer.C:
		return time.Since(start), fmt.Errorf("could not acquire slot in %s", maxWait)
	case <-ctx.Done():
		return time.Since(start), ctx.Err()
	}
}

// runtimeChain returns the composed Middleware for runtime hooks.
// Transforms without a Runtime half are skipped.
func (g *Gateway) runtimeChain() Middleware {
	return func(next Handler) Handler {
		h := next
		for i := len(g.transforms) - 1; i >= 0; i-- {
			if g.transforms[i].Runtime != nil {
				h = g.transforms[i].Runtime(h)
			}
		}
		return h
	}
}

// headerInjectorSnapshot returns the flat list of header injectors
// across every Transform, in registration order. Captured at schema
// build time and baked into each dispatcher; rebuilds on Use.
//
// Caller holds g.mu.
func (g *Gateway) headerInjectorSnapshot() []headerInjector {
	if len(g.transforms) == 0 {
		return nil
	}
	var n int
	for _, t := range g.transforms {
		n += len(t.headers)
	}
	if n == 0 {
		return nil
	}
	out := make([]headerInjector, 0, n)
	for _, t := range g.transforms {
		out = append(out, t.headers...)
	}
	return out
}

