// gwag serve: front a single upstream service with one CLI invocation.
// Two flavors:
//
//   - --openapi PATH --to URL       embedded gat translator (no NATS,
//   - --proto   PATH --to HOST:PORT  no admin, no metrics)
//   - --graphql URL                  full gateway — metrics, backpressure,
//                                    subscription proxy, optional --mcp
//
// --mcp is supported with every source. With --graphql it rides on the
// full gateway path that's already there; with --openapi / --proto it
// promotes the run from the lite gat path onto the full gateway so the
// /mcp mount has the same dispatcher, plan cache, and metrics every
// other ingress hits.
//
// The smallest possible "typed clients in three formats over my
// existing service" CLI — useful for proxies, demos, and
// graphql-codegen targets backed by a single backend.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"

	gateway "github.com/iodesystems/gwag/v2/gw"
	"github.com/iodesystems/gwag/v2/gw/gat"
	"github.com/iodesystems/gwag/v2/gw/ir"
)

func serveCmd(args []string) int {
	fs := flag.NewFlagSet("gwag serve", flag.ContinueOnError)
	openapiPath := fs.String("openapi", "", "Path to an OpenAPI 3.x spec (JSON or YAML)")
	protoPath := fs.String("proto", "", "Path to a .proto file")
	graphqlURL := fs.String("graphql", "", "URL of a downstream GraphQL endpoint (introspects on boot)")
	target := fs.String("to", "", "Upstream target — http(s):// for --openapi, host:port for --proto. Not used with --graphql.")
	addr := fs.String("addr", ":8080", "HTTP listen address")
	prefix := fs.String("prefix", "/", "URL prefix to mount /graphql + /schema/* under (ignored for --graphql and for any run with --mcp)")
	namespace := fs.String("namespace", "", "Override the GraphQL namespace; defaults to spec-derived")
	version := fs.String("version", "", "Override the GraphQL version; defaults to spec-derived (\"v1\" for OpenAPI / GraphQL)")
	enableMCP := fs.Bool("mcp", false, "Mount /mcp (admin-bearer gated). With --openapi / --proto, promotes the run from the lite gat path onto the full gateway so /mcp shares dispatch and metrics with every other ingress.")
	mcpInclude := &stringListValue{}
	fs.Var(mcpInclude, "mcp-include", "MCP allowlist glob (repeatable). Implies --mcp.")
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "Usage: gwag serve {--openapi PATH --to URL | --proto PATH --to HOST:PORT | --graphql URL} [flags]")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Fronts one upstream service as typed proto / OpenAPI / GraphQL clients.")
		fmt.Fprintln(out, "  --openapi / --proto:  embedded gat translator (no metrics, no NATS, no admin).")
		fmt.Fprintln(out, "  --graphql:            full gateway (metrics, backpressure, subscription proxy).")
		fmt.Fprintln(out, "  --mcp:                works with every source; with --openapi / --proto it promotes to the full gateway path.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// Exactly one source flag.
	sources := 0
	if *openapiPath != "" {
		sources++
	}
	if *protoPath != "" {
		sources++
	}
	if *graphqlURL != "" {
		sources++
	}
	if sources != 1 {
		fmt.Fprintln(os.Stderr, "gwag serve: exactly one of --openapi, --proto, or --graphql is required")
		fs.Usage()
		return 2
	}
	if *graphqlURL == "" && *target == "" {
		fmt.Fprintln(os.Stderr, "gwag serve: --to is required with --openapi / --proto")
		fs.Usage()
		return 2
	}

	mcpOn := *enableMCP || len(mcpInclude.values) > 0

	// --graphql always uses the full gateway. --openapi / --proto promote
	// to the full gateway when --mcp is set, so /mcp shares the
	// dispatcher and metrics path with every other ingress. Otherwise
	// they stay on the lite gat path.
	if *graphqlURL != "" || mcpOn {
		src := fullGatewaySource{
			graphqlURL:  *graphqlURL,
			openapiPath: *openapiPath,
			protoPath:   *protoPath,
			target:      *target,
		}
		return serveFullGateway(src, *addr, *namespace, *version, mcpOn, mcpInclude.values)
	}
	return serveGat(*openapiPath, *protoPath, *target, *addr, *prefix, *namespace, *version)
}

// fullGatewaySource carries exactly one source (graphqlURL, openapiPath,
// or protoPath) plus the upstream target string used by AddOpenAPI /
// AddProto. The exact-one invariant is enforced upstream by serveCmd.
type fullGatewaySource struct {
	graphqlURL  string
	openapiPath string
	protoPath   string
	target      string
}

// serveFullGateway boots a full *gateway.Gateway (metrics, backpressure,
// subscription proxy) wrapping one upstream of any supported kind.
// /mcp is optional via --mcp. No NATS, no admin UI, no cluster — those
// ride on `gwag up`.
func serveFullGateway(src fullGatewaySource, addr, namespace, version string, enableMCP bool, mcpIncludes []string) int {
	gwOpts := []gateway.Option{}
	if enableMCP && len(mcpIncludes) > 0 {
		gwOpts = append(gwOpts, gateway.WithMCPInclude(mcpIncludes...))
	}
	gw := gateway.New(gwOpts...)
	defer gw.Close()

	addOpts := []gateway.ServiceOption{}
	if namespace != "" {
		addOpts = append(addOpts, gateway.As(namespace))
	}
	if version != "" {
		addOpts = append(addOpts, gateway.Version(version))
	}

	var label string
	switch {
	case src.graphqlURL != "":
		label = src.graphqlURL
		if err := gw.AddGraphQL(src.graphqlURL, addOpts...); err != nil {
			fmt.Fprintf(os.Stderr, "gwag serve: AddGraphQL: %v\n", err)
			return 1
		}
	case src.openapiPath != "":
		label = src.openapiPath
		if err := gw.AddOpenAPI(src.openapiPath, append(addOpts, gateway.To(src.target))...); err != nil {
			fmt.Fprintf(os.Stderr, "gwag serve: AddOpenAPI: %v\n", err)
			return 1
		}
	case src.protoPath != "":
		label = src.protoPath
		if err := gw.AddProto(src.protoPath, append(addOpts, gateway.To(src.target))...); err != nil {
			fmt.Fprintf(os.Stderr, "gwag serve: AddProto: %v\n", err)
			return 1
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/api/graphql", gw.Handler())
	mux.Handle("/api/schema/graphql", gw.SchemaHandler())
	mux.Handle("/api/schema/proto", gw.SchemaProtoHandler())
	mux.Handle("/api/schema/openapi", gw.SchemaOpenAPIHandler())
	mux.Handle("/api/metrics", gw.MetricsHandler())
	mux.Handle("/api/health", gw.HealthHandler())
	if enableMCP {
		gw.MountMCP(mux)
	}

	log.Printf("gwag serve: %s → POST %s/api/graphql", label, strings.TrimRight(addr, "/"))
	if enableMCP {
		log.Printf("gwag serve: MCP at /mcp  (admin token = %s)", gw.AdminTokenHex())
	}
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "gwag serve: %v\n", err)
		return 1
	}
	return 0
}

// serveGat is the lite path used by --openapi / --proto: embedded gat
// translator, no metrics, no admin.
func serveGat(openapiPath, protoPath, target, addr, prefix, namespace, version string) int {
	var (
		regs []gat.ServiceRegistration
		err  error
	)
	switch {
	case openapiPath != "":
		regs, err = loadOpenAPIRegistration(openapiPath, target)
	case protoPath != "":
		regs, err = gat.ProtoFile(protoPath, target)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gwag serve: %v\n", err)
		return 1
	}
	for i := range regs {
		if namespace != "" {
			regs[i].Service.Namespace = namespace
		}
		if version != "" {
			regs[i].Service.Version = version
		}
	}

	g, err := gat.New(regs...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gwag serve: build gateway: %v\n", err)
		return 1
	}

	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, prefix); err != nil {
		fmt.Fprintf(os.Stderr, "gwag serve: register http: %v\n", err)
		return 1
	}

	pretty := strings.TrimRight(prefix, "/")
	if pretty == "" {
		pretty = "/"
	}
	log.Printf("gwag serve: %s → %s", regs[0].Service.Namespace, target)
	log.Printf("gwag serve: listening on %s; POST %s/graphql", addr, strings.TrimRight(pretty, "/"))
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "gwag serve: %v\n", err)
		return 1
	}
	return 0
}

// stringListValue implements flag.Value for repeatable string flags.
type stringListValue struct{ values []string }

func (s *stringListValue) String() string     { return strings.Join(s.values, ",") }
func (s *stringListValue) Set(v string) error { s.values = append(s.values, v); return nil }

// loadOpenAPIRegistration reads spec from disk, parses it via
// kin-openapi, ingests into IR, populates schema IDs, and returns one
// ServiceRegistration pointing dispatch at the upstream target. The
// dispatcher path inside gat handles per-call HTTP forwarding.
func loadOpenAPIRegistration(path, target string) ([]gat.ServiceRegistration, error) {
	specBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	doc, err := loader.LoadFromData(specBytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	svc := ir.IngestOpenAPI(doc)
	if svc.Namespace == "" {
		svc.Namespace = openAPINamespaceFromDoc(doc, path)
	}
	if svc.Version == "" {
		svc.Version = "v1"
	}
	ir.PopulateSchemaIDs(svc)
	return []gat.ServiceRegistration{{
		Service: svc,
		BaseURL: target,
	}}, nil
}

// openAPINamespaceFromDoc derives a GraphQL-safe namespace string
// from the spec's Info.Title via the shared ir.SanitizeNamespace rule
// (case preserved), falling back to the spec filename stem and then a
// literal "openapi".
func openAPINamespaceFromDoc(doc *openapi3.T, path string) string {
	if doc != nil && doc.Info != nil {
		if ns := ir.SanitizeNamespace(doc.Info.Title); ns != "" {
			return ns
		}
	}
	base := filepath.Base(path)
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	if ns := ir.SanitizeNamespace(stem); ns != "" {
		return ns
	}
	return "openapi"
}
