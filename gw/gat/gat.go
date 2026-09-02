// Package gat (GraphQL API Translator) is gwag's embedded sibling: a
// single-server, in-process translator that turns OpenAPI / proto /
// GraphQL specs into a GraphQL surface mountable on the adopter's
// existing huma router. No NATS, no cluster, no admin endpoints, no
// MCP — just spec-to-GraphQL with HTTP dispatch.
package gat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// Gateway is the embedded GraphQL translator. Given service specs
// (OpenAPI, proto, or custom dispatchers), it produces a GraphQL
// schema and serves queries.
//
// Two registration paths feed it:
//
//   - BYO IR: gat.New(ServiceRegistration{...}) takes pre-ingested
//     ir.Service values plus dispatchers (HTTP loopback or custom).
//     Schema is built eagerly.
//
//   - Paired huma: gat.New() returns an empty gateway. Each operation
//     is added with gat.Register (a drop-in for huma.Register that
//     also captures the handler ref for in-process dispatch). When the
//     adopter calls gat.RegisterHuma(api, g, prefix), gat reads
//     api.OpenAPI(), ingests it into IR, wires the captured handlers
//     as dispatchers, builds the schema, and mounts the GraphQL +
//     schema-view endpoints onto the adopter's huma router.
//
// Stability: experimental
type Gateway struct {
	schema      *graphql.Schema
	annotations *ir.AnnotationIndex
	registry    *ir.DispatchRegistry
	services    []*ir.Service

	// planCache holds parsed + validated + planned query state so a
	// repeated query skips straight to execution. Built by build();
	// nil only on a gateway whose schema never assembled, which the
	// handler treats as "not ready".
	planCache *graphql.PlanCache

	// captured holds ops registered via gat.Register for paired huma
	// dispatch. Empty when only the BYO-IR path is used.
	captured []*capturedOp

	// built is true once the schema has been assembled (either by
	// New(regs...) or by RegisterHuma). Subsequent registrations are
	// rejected.
	built bool

	// longAsNumber opts the Long scalar into honest number|string output —
	// a JSON number when JS-safe (|v| <= 2^53-1), a decimal string when
	// larger. Default false = legacy always-string. Set via LongAsNumber()
	// before the schema builds (i.e. before New(regs...) returns or before
	// RegisterHuma on the empty-gateway path). Input stays lenient either way.
	longAsNumber bool

	// emptyNilSlices (default true) makes required output list fields render
	// non-null (`[T!]!`) and coerces nil Go slices to `[]` at dispatch, so an
	// empty/absent slice serializes as `[]` rather than null — clients never
	// see a null list (better DX). Set EmptyNilSlices(false) for the legacy
	// behavior (nullable lists, nil → JSON null).
	emptyNilSlices bool

	// allowEmbeddedParams downgrades the embedded-parameter check from a
	// mount-time error to a log warning. Off by default: the condition
	// silently drops request parameters, so failing loudly is the safer
	// default.
	allowEmbeddedParams bool

	// observe, when set (ObserveDispatch), is invoked after every in-process
	// operation dispatch for per-operation metrics. nil = no observation.
	observe DispatchObserver

	// pubsub is gat's in-process publish/subscribe primitive, always
	// available via PubSub(). mesh is the optional best-effort
	// cross-node fanout layer — nil until EnablePeerMesh is called.
	// subAuth is the HMAC secret gating the WebSocket subscribe
	// endpoint — nil until EnableSubscribeAuth is called (endpoint
	// open).
	pubsub  *pubSub
	mesh    *peerMesh
	subAuth []byte
}

// ServiceRegistration pairs an IR service with its dispatch config.
//
// Stability: experimental
type ServiceRegistration struct {
	Service     *ir.Service
	BaseURL     string                        // upstream HTTP base URL for OpenAPI dispatch
	Dispatchers map[ir.SchemaID]ir.Dispatcher // custom dispatchers keyed by SchemaID
}

// New constructs a gat gateway. Two usage patterns:
//
// Paired huma (recommended):
//
//	g := gat.New()
//	gat.Register(api, g, huma.Operation{...}, listProjectsHandler)
//	gat.Register(api, g, huma.Operation{...}, getProjectHandler)
//	gat.RegisterHuma(api, g, "/api")  // mounts /api/graphql + /api/schema/*
//
// BYO IR (advanced; bring pre-ingested services + dispatchers):
//
//	doc, _ := loader.LoadFromData(specBytes)
//	svc := ir.IngestOpenAPI(doc)
//	svc.Namespace = "pets"
//	svc.Version = "v1"
//	ir.PopulateSchemaIDs(svc)
//	gw, _ := gat.New(gat.ServiceRegistration{
//	    Service: svc,
//	    BaseURL: "http://localhost:8081",
//	})
//	http.Handle("/graphql", gw.Handler())
//
// Stability: experimental
func New(regs ...ServiceRegistration) (*Gateway, error) {
	g := &Gateway{
		registry:       ir.NewDispatchRegistry(),
		pubsub:         newPubSub(),
		emptyNilSlices: true,
	}
	if len(regs) == 0 {
		// Empty gateway — adopter will populate via gat.Register and
		// finalize via gat.RegisterHuma.
		return g, nil
	}
	if err := g.addRegistrations(regs); err != nil {
		return nil, err
	}
	if err := g.build(); err != nil {
		return nil, err
	}
	return g, nil
}

// LongAsNumber opts this gateway's Long scalar into honest number|string output —
// a JSON number when JS-safe (|v| <= 2^53-1), a decimal string when larger. Call it
// before the schema builds (empty New() → LongAsNumber(true) → Register… → RegisterHuma).
// Returns g for chaining; a no-op once built. Input is lenient (number or string) either way.
func (g *Gateway) LongAsNumber(b bool) *Gateway {
	g.longAsNumber = b
	return g
}

// EmptyNilSlices toggles the empty-list contract (default on). When on, required
// output list fields render non-null (`[T!]!`) and the dispatch coerces nil Go
// slices to `[]`, so clients never receive a null list. Off restores the legacy
// behavior (nullable lists; a nil slice serializes to JSON null). Call before the
// schema builds. Returns g for chaining; a no-op once built.
func (g *Gateway) EmptyNilSlices(b bool) *Gateway {
	g.emptyNilSlices = b
	return g
}

// DispatchObserver is called after every in-process operation dispatch with the operation id,
// how long it took, and its error (nil on success). It is the per-operation join point — a
// server-side timing hook around each resolver→handler call — so consumers can emit per-operation
// metrics (rate/errors/latency) the way an AOP pointcut did. Keep it cheap; it runs on the hot path.
type DispatchObserver func(operationID string, dur time.Duration, err error)

// ObserveDispatch sets the per-operation dispatch observer (see DispatchObserver). Call before the
// schema builds (the dispatchers capture it at build time). Returns g for chaining; a no-op once
// built. nil disables observation. Stability: experimental.
func (g *Gateway) ObserveDispatch(fn DispatchObserver) *Gateway {
	g.observe = fn
	return g
}

// addRegistrations attaches BYO-IR services and wires their
// dispatchers onto g. Caller is responsible for invoking g.build()
// once all registrations are in.
func (g *Gateway) addRegistrations(regs []ServiceRegistration) error {
	for _, reg := range regs {
		svc := reg.Service
		if svc.Namespace == "" {
			svc.Namespace = "openapi"
		}
		if svc.Version == "" {
			svc.Version = "v1"
		}
		g.services = append(g.services, svc)

		for _, op := range svc.FlatOperations() {
			if d, ok := reg.Dispatchers[op.SchemaID]; ok {
				g.registry.Set(op.SchemaID, d)
				continue
			}
			if reg.BaseURL != "" && svc.OriginKind == ir.KindOpenAPI {
				g.registry.Set(op.SchemaID, newOpenAPIDispatcher(reg.BaseURL, svc, op))
			}
		}
	}
	return nil
}

// build assembles the GraphQL schema from g.services and g.registry.
// Idempotent: callers may invoke it multiple times if more services
// are added between rounds (RegisterHuma does this when paired
// captured ops are present).
func (g *Gateway) build() error {
	longScalar, jsonScalar := ir.StandardScalarsWith(g.longAsNumber)
	annotations := ir.NewAnnotationIndex()
	schema, err := ir.RenderGraphQLRuntime(g.services, g.registry, ir.RuntimeOptions{
		LongType:             longScalar,
		JSONType:             jsonScalar,
		AnnotationSink:       annotations,
		NonNullRequiredLists: g.emptyNilSlices,
	})
	if err != nil {
		return fmt.Errorf("gat: build schema: %w", err)
	}
	g.schema = schema
	g.annotations = annotations
	// The cache is keyed on (schema, query, operationName), so it is
	// safe to build once here — the schema is immutable after build.
	// Defaults mirror gw/'s: gat has no knobs to expose them through.
	g.planCache = graphql.NewPlanCache(graphql.PlanCacheOptions{
		MaxEntries: planCacheEntries,
	})
	g.built = true
	return nil
}

// Handler returns an http.Handler that serves GraphQL queries and
// mutations. POST /graphql with JSON body; GET with ?query= param.
// WebSocket subscriptions are not supported in gat mode.
//
// Stability: experimental
func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			http.Error(w, "upgrade not supported", http.StatusNotFound)
			return
		}

		var (
			query         string
			variables     map[string]any
			operationName string
		)

		if r.Method == http.MethodPost {
			var req struct {
				Query         string         `json:"query"`
				Variables     map[string]any `json:"variables"`
				OperationName string         `json:"operationName"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			query = req.Query
			variables = req.Variables
			operationName = req.OperationName
		} else {
			query = r.URL.Query().Get("query")
			operationName = r.URL.Query().Get("operationName")
		}

		if query == "" {
			writeError(w, "missing query", http.StatusBadRequest)
			return
		}

		ctx, cookies := withCookieSink(r.Context())
		pr := g.planCache.Get(g.schema, query, operationName)

		// Emit any Set-Cookie a handler produced (login/logout) before the
		// body — GraphQL transport otherwise drops per-op response headers.
		//
		// Status is 200 whatever the query did, matching gw/ and the
		// GraphQL-over-HTTP spec: a query that resolved to errors is a
		// successful transport exchange carrying an errors envelope, not a
		// failed request. The two 400s above are transport-level (the body
		// wasn't JSON, or carried no query at all) and stay 400.
		cookies.emit(w)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		switch {
		case len(pr.Errors) > 0:
			// Parse / validate failed, so there is no plan to run. Rare
			// path with a small payload — the encoder is fine here.
			_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: pr.Errors})
		case pr.Plan != nil:
			args := variables
			if len(pr.SynthArgs) > 0 {
				merged := make(map[string]any, len(args)+len(pr.SynthArgs))
				for k, v := range args {
					merged[k] = v
				}
				for k, v := range pr.SynthArgs {
					merged[k] = v
				}
				args = merged
			}
			// Hot path: walk the cached plan and emit response JSON
			// straight into a pooled buffer, skipping the
			// map[string]any result tree and the final Encode.
			buf := graphqlBufPool.Get().(*[]byte)
			body, errs := graphql.ExecutePlanAppend(pr.Plan, graphql.ExecuteParams{
				Schema:        *g.schema,
				OperationName: operationName,
				Args:          args,
				Context:       ctx,
			}, (*buf)[:0])
			if len(errs) > 0 {
				// Spec-level failures (variable coercion and friends) that
				// happened before any data was assembled. Emit a clean
				// errors envelope rather than the partial bytes.
				_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: errs})
			} else {
				// Field-level errors are already inside body's `errors`
				// array — ExecutePlanAppend writes them there.
				_, _ = w.Write(body)
			}
			// Recycle the (possibly grown) backing array unless it
			// ballooned; a one-off large response shouldn't pin a fat
			// allocation for the process lifetime.
			if cap(body) <= graphqlBufPoolMax {
				*buf = body[:0]
				graphqlBufPool.Put(buf)
			}
		default:
			// No plan and no errors shouldn't happen, but fall back to
			// the full parse-and-execute path rather than serving an
			// empty body.
			_ = json.NewEncoder(w).Encode(graphql.Do(graphql.Params{
				Schema:         *g.schema,
				RequestString:  query,
				VariableValues: variables,
				OperationName:  operationName,
				Context:        ctx,
			}))
		}
	})
}

// Schema returns the assembled graphql.Schema for introspection.
//
// Stability: experimental
func (g *Gateway) Schema() *graphql.Schema {
	return g.schema
}

// GraphQLSDL renders the gateway's schema as SDL WITH its annotation index (@derived,
// @deprecated, etc.). Prefer this over ir.PrintSchemaSDL(g.Schema()), which omits the
// annotations — the SDL/graphql HTTP view uses this same path.
func (g *Gateway) GraphQLSDL() string {
	return ir.PrintSchemaSDL(g.schema, g.annotations)
}

// Services returns the registered IR services.
//
// Stability: experimental
func (g *Gateway) Services() []*ir.Service {
	return g.services
}

// PubSub returns the gateway's publish/subscribe handle. It is always
// available — single-node (in-process) out of the box. Call
// EnablePeerMesh to add best-effort cross-node fanout to a set of
// peer gat instances.
//
// Stability: experimental
func (g *Gateway) PubSub() *PubSub {
	return &PubSub{g: g}
}

// Close releases gateway resources: it stops the peer-mesh fanout
// goroutines (if EnablePeerMesh was called) and cancels every active
// pubsub subscription. A gateway is single-use after Close.
//
// Stability: experimental
func (g *Gateway) Close() {
	if g.mesh != nil {
		g.mesh.stop()
	}
	g.pubsub.closeAll()
}

// PubSub is the publish/subscribe handle returned by Gateway.PubSub.
// Publish delivers to local subscribers and, when a peer mesh is
// configured, fans out best-effort to peer gat instances; Subscribe
// is always local-only.
//
// Stability: experimental
type PubSub struct {
	g *Gateway
}

// Publish delivers payload to every local subscriber whose pattern
// matches channel.
//
// Stability: experimental
func (p *PubSub) Publish(channel string, payload []byte) {
	p.g.publish(channel, payload)
}

// Subscribe registers a subscriber for channels matching pattern
// (NATS-style: `*` one segment, `>` the rest). Returns an event
// channel and a cancel func. Subscriptions are local to this gateway
// — peers deliver into it via the mesh receive path, not by
// subscribing remotely.
//
// Stability: experimental
func (p *PubSub) Subscribe(pattern string) (<-chan Event, func()) {
	return p.g.pubsub.Subscribe(pattern)
}

// publish is the gateway-internal publish entry point behind the
// PubSub facade: local fanout, plus best-effort cross-node fanout
// when a peer mesh is configured.
func (g *Gateway) publish(channel string, payload []byte) {
	g.pubsub.publishLocal(channel, payload)
	if g.mesh != nil {
		g.mesh.fanout(channel, payload)
	}
}

// planCacheEntries bounds the parsed-query cache. gat fronts one
// binary's own operations, so the realistic working set is the query
// set that binary's clients ship — small. Matches gw/'s default.
const planCacheEntries = 1000

// graphqlBufPool reuses []byte buffers across GraphQL responses to
// keep allocs off the hot path. New buffers start at 4 KB — enough
// for most responses; ExecutePlanAppend grows the slice as needed.
// graphqlBufPoolMax caps what we hand back so a one-off megabyte
// response doesn't pin a fat allocation.
const graphqlBufPoolMax = 64 * 1024

var graphqlBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func writeError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"errors": []map[string]any{{"message": msg}},
	})
}

// contextKey for attaching *http.Request to context.
type contextKey int

const httpRequestKey contextKey = iota

// WithHTTPRequest attaches the inbound *http.Request to ctx.
//
// Stability: experimental
func WithHTTPRequest(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, httpRequestKey, r)
}

// HTTPRequestFromContext extracts the *http.Request from ctx.
//
// Stability: experimental
func HTTPRequestFromContext(ctx context.Context) *http.Request {
	r, _ := ctx.Value(httpRequestKey).(*http.Request)
	return r
}
