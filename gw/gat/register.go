package gat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/getkin/kin-openapi/openapi3"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// capturedOp is one paired Register entry. It records the huma
// Operation alongside type-erased input/output reflection plumbing
// and an invoker that types-asserts the input back to *I before
// calling the original handler.
//
// irOp is populated by ingestHuma once the IR projection is built —
// it lets the dispatch and gRPC layers share one binding path.
type capturedOp struct {
	op         huma.Operation
	inputType  reflect.Type // I (the value type behind *I)
	outputType reflect.Type // O (the value type behind *O)
	invoke     func(ctx context.Context, in any) (any, error)
	irOp       *ir.Operation // assigned during ingestHuma; nil until then
}

// Register is the paired drop-in for huma.Register. It registers the
// operation with huma exactly as huma.Register would, and also
// captures the handler ref on g so RegisterHuma can wire an
// in-process dispatcher (no loopback HTTP) for the GraphQL and gRPC
// surfaces gat builds on top of huma.
//
// Use this in place of huma.Register for any operation you want
// surfaced via gat. Operations registered with plain huma.Register
// are unaffected — they continue to serve REST/OpenAPI as before, but
// gat has no view into them.
//
// All Register calls must happen before RegisterHuma. Calling
// Register after the schema is built returns silently (huma
// registration still succeeds; the capture is dropped) — a hard
// panic was rejected because tests sometimes register more after
// inspecting the schema. Order matters in production code.
//
// Stability: experimental
func Register[I, O any](api huma.API, g *Gateway, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	huma.Register(api, op, handler)
	if g.built {
		return
	}
	g.captured = append(g.captured, &capturedOp{
		op:         op,
		inputType:  reflect.TypeOf((*I)(nil)).Elem(),
		outputType: reflect.TypeOf((*O)(nil)).Elem(),
		invoke: func(ctx context.Context, in any) (any, error) {
			typed, ok := in.(*I)
			if !ok {
				return nil, fmt.Errorf("gat: input type mismatch for %s: got %T", op.OperationID, in)
			}
			return handler(ctx, typed)
		},
	})
}

// RegisterHuma finalizes a paired-mode gateway: ingests huma's
// OpenAPI document into IR, wires in-process dispatchers for every
// captured operation, builds the GraphQL schema, then mounts the
// GraphQL endpoint and schema-view endpoints onto the adopter's huma
// router under prefix.
//
// Routes registered (prefix prepended):
//
//	POST {prefix}/graphql              — GraphQL queries / mutations
//	GET  {prefix}/schema/graphql       — SDL (default) or introspection JSON
//	GET  {prefix}/schema/proto         — FileDescriptorSet (binary)
//	GET  {prefix}/schema/openapi       — re-emitted OpenAPI document
//
// Pass an empty prefix to mount at root. The prefix should not have
// a trailing slash; "/api" → "/api/graphql".
//
// Stability: experimental
func RegisterHuma(api huma.API, g *Gateway, prefix string) error {
	if g.built {
		return fmt.Errorf("gat: gateway already finalized")
	}
	prefix = strings.TrimRight(prefix, "/")

	// huma skips unexported fields during parameter discovery, so a parameter on
	// an unexported field — or inside an embedded unexported type — is absent
	// from the schema and never bound. Report before building, not after
	// shipping.
	if found := g.hiddenParams(); len(found) > 0 {
		if !g.allowEmbeddedParams {
			return hiddenParamError(found)
		}
		for _, e := range found {
			log.Printf("gat: warning: %s", e)
		}
	}

	if len(g.captured) > 0 {
		if err := g.ingestHuma(api); err != nil {
			return err
		}
	}

	if err := g.build(); err != nil {
		return err
	}

	mountSchemaEndpoints(api, g, prefix)
	mountGraphQLEndpoint(api, g, prefix)
	mountSubscribe(api, g, prefix)
	if g.mesh != nil {
		mountPeerPublish(api, g, prefix)
	}
	return nil
}

// ingestHuma reads the huma API's accumulated OpenAPI doc, ingests it
// into a fresh IR service, and wires an in-process dispatcher for each
// captured operation (matched by OperationID).
func (g *Gateway) ingestHuma(api huma.API) error {
	specBytes, err := json.Marshal(api.OpenAPI())
	if err != nil {
		return fmt.Errorf("gat: marshal huma OpenAPI: %w", err)
	}

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(specBytes)
	if err != nil {
		return fmt.Errorf("gat: parse huma OpenAPI: %w", err)
	}

	svc := ir.IngestOpenAPI(doc)
	if svc.Namespace == "" {
		// Derive the namespace from the OpenAPI title via the shared
		// ir.SanitizeNamespace rule (case preserved). Adopters can
		// override via huma's config.
		if oapi := api.OpenAPI(); oapi != nil && oapi.Info != nil && oapi.Info.Title != "" {
			svc.Namespace = ir.SanitizeNamespace(oapi.Info.Title)
		}
		if svc.Namespace == "" {
			svc.Namespace = "huma"
		}
	}
	if svc.Version == "" {
		svc.Version = "v1"
	}
	ir.PopulateSchemaIDs(svc)

	captured := make(map[string]*capturedOp, len(g.captured))
	for _, c := range g.captured {
		captured[c.op.OperationID] = c
	}

	for _, op := range svc.FlatOperations() {
		c, ok := captured[op.Name]
		if !ok {
			continue
		}
		c.irOp = op
		// @derived: declare the codegen provenance of this generated SDL field — it
		// derives from the Go huma operation with this OperationID. Lets a consumer
		// (poly-lsp-mcp) follow the real edge to the source instead of guessing the
		// field↔OperationID mapping by replicating gat's naming rule.
		op.Annotations = append(op.Annotations, ir.Annotation{
			Name: "derived",
			Args: []ir.AnnotationArg{{Name: "operationId", Kind: ir.AnnString, Value: op.Name}},
		})
		g.registry.Set(op.SchemaID, newInprocDispatcher(c, op, g.emptyNilSlices, g.observe))
	}

	g.services = append(g.services, svc)
	return nil
}

// mountGraphQLEndpoint registers POST {prefix}/graphql on api as a
// huma operation that delegates to g.Handler.
func mountGraphQLEndpoint(api huma.API, g *Gateway, prefix string) {
	api.Adapter().Handle(&huma.Operation{
		Method: http.MethodPost,
		Path:   prefix + "/graphql",
	}, func(ctx huma.Context) {
		// Adapter-level handler: bypass huma's body codec since
		// GraphQL has its own request shape, and reuse g.Handler
		// which already understands query/mutation envelopes.
		g.Handler().ServeHTTP(newAdapterResponseWriter(ctx), adapterRequest(ctx))
	})
}

// mountSubscribe registers GET {prefix}/_gat/subscribe as a huma
// adapter handler — the WebSocket pubsub stream. Always mounted; the
// in-process pubsub primitive is always available. The WS upgrade
// relies on adapterResponseWriter forwarding Hijack to the
// underlying writer.
func mountSubscribe(api huma.API, g *Gateway, prefix string) {
	h := g.subscribeWSHandler()
	api.Adapter().Handle(&huma.Operation{
		Method: http.MethodGet,
		Path:   prefix + SubscribePath,
	}, func(ctx huma.Context) {
		h.ServeHTTP(newAdapterResponseWriter(ctx), adapterRequest(ctx))
	})
}

// mountPeerPublish registers POST {prefix}/_gat/publish as a huma
// adapter handler — the peer-mesh receive endpoint. Mounted only when
// EnablePeerMesh was called.
func mountPeerPublish(api huma.API, g *Gateway, prefix string) {
	h := g.peerPublishHandler()
	api.Adapter().Handle(&huma.Operation{
		Method: http.MethodPost,
		Path:   prefix + PeerPublishPath,
	}, func(ctx huma.Context) {
		h.ServeHTTP(newAdapterResponseWriter(ctx), adapterRequest(ctx))
	})
}

// mountSchemaEndpoints registers the three /schema/{graphql,proto,openapi}
// views as huma adapter handlers (no body codec; raw byte payloads).
func mountSchemaEndpoints(api huma.API, g *Gateway, prefix string) {
	mount := func(path string, h http.Handler) {
		api.Adapter().Handle(&huma.Operation{
			Method: http.MethodGet,
			Path:   prefix + path,
		}, func(ctx huma.Context) {
			h.ServeHTTP(newAdapterResponseWriter(ctx), adapterRequest(ctx))
		})
	}
	mount("/schema/graphql", schemaGraphQLHandler(g))
	mount("/schema/proto", schemaProtoHandler(g))
	mount("/schema/openapi", schemaOpenAPIHandler(g))

	// Also answer introspection POSTs on /schema/graphql by delegating to the
	// GraphQL handler (same as {prefix}/graphql). graphql-codegen introspects
	// via POST, so pointing it at the documented schema URL then works instead
	// of 405-ing. (The GET op above stays the SDL / ?format=json view.)
	api.Adapter().Handle(&huma.Operation{
		Method: http.MethodPost,
		Path:   prefix + "/schema/graphql",
	}, func(ctx huma.Context) {
		g.Handler().ServeHTTP(newAdapterResponseWriter(ctx), adapterRequest(ctx))
	})
}
