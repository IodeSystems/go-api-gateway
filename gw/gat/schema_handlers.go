package gat

import (
	"encoding/json"
	"net/http"

	"github.com/IodeSystems/graphql-go/v2"
	"google.golang.org/protobuf/proto"

	"github.com/iodesystems/gwag/gw/ir"
)

// schemaGraphQLHandler serves the gateway's GraphQL surface as SDL
// (default) or introspection JSON (`?format=json`). Codegen tools can
// point straight at this URL.
func schemaGraphQLHandler(g *Gateway) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// POST → delegate to the GraphQL execution handler. graphql-codegen
		// and similar tools introspect via a POST query; pointing them at
		// this documented schema URL then "just works" instead of 405-ing.
		if r.Method == http.MethodPost {
			g.Handler().ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if g.schema == nil {
			http.Error(w, "schema not assembled", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Query().Get("format") {
		case "json":
			result := graphql.Do(graphql.Params{
				Schema:        *g.schema,
				RequestString: ir.IntrospectionQuery,
			})
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = ir.WriteJSON(w, result)
		default:
			w.Header().Set("Content-Type", "application/graphql; charset=utf-8")
			_, _ = w.Write([]byte(ir.PrintSchemaSDL(g.schema, g.annotations)))
		}
	})
}

// schemaProtoHandler renders gat's IR services as a FileDescriptorSet
// (default, `application/protobuf`) so proto-codegen tools can consume
// the same surface as GraphQL.
func schemaProtoHandler(g *Gateway) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fds, err := ir.RenderProtoFiles(g.services)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out, err := proto.MarshalOptions{Deterministic: true}.Marshal(fds)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/protobuf")
		w.Header().Set("Content-Disposition", `attachment; filename="services.fds"`)
		_, _ = w.Write(out)
	})
}

// schemaOpenAPIHandler re-emits the OpenAPI projection of every IR
// service. When the source was OpenAPI (e.g. huma's spec) the round
// trip is near-identity; when the source was proto/GraphQL the
// projection produces a synthesized spec.
func schemaOpenAPIHandler(g *Gateway) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if len(g.services) == 0 {
			http.Error(w, "no services", http.StatusServiceUnavailable)
			return
		}
		// Single-service case: emit the one spec directly.
		if len(g.services) == 1 {
			doc, err := ir.RenderOpenAPI(g.services[0])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(w).Encode(doc)
			return
		}
		// Multi-service: emit a map keyed by namespace.
		bundle := map[string]any{}
		for _, svc := range g.services {
			doc, err := ir.RenderOpenAPI(svc)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			bundle[svc.Namespace] = doc
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(bundle)
	})
}
