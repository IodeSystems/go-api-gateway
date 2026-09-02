// gat-gateway is the gat entry in the competitor sweep.
//
// `gwag serve` fronts one upstream per process. gwag fronts several at
// once, so sweeping gat against it through `gwag serve` would compare a
// one-service gateway to a many-service one. This binary registers the
// same `hello-*` upstreams gwag does, in one gat gateway on one port,
// so the only difference left between the two rows is what the gateway
// itself does per request.
//
// Namespaces are set to match gwag's registered names (`hello_proto`,
// `hello_openapi`), which lets both gateways run the byte-identical
// query from compare/competitors.yaml — no per-gateway query override.
//
// No NATS, no cluster, no admin, no control plane: that is the point of
// gat, and it is what the sweep is pricing.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/iodesystems/gwag/v2/gw/gat"
	"github.com/iodesystems/gwag/v2/gw/ir"
)

func main() {
	addr := flag.String("addr", ":18085", "HTTP listen address")
	prefix := flag.String("prefix", "/api", "mount prefix for /graphql + /schema/*")
	protoFile := flag.String("proto", "protos/hello.proto", "path to the hello-proto .proto")
	protoTarget := flag.String("proto-target", "localhost:50055", "hello-proto gRPC target")
	openapiURL := flag.String("openapi-url", "http://localhost:50053/openapi.json", "hello-openapi spec URL")
	openapiTarget := flag.String("openapi-target", "http://localhost:50053", "hello-openapi base URL")
	flag.Parse()

	var regs []gat.ServiceRegistration

	protoRegs, err := gat.ProtoFile(*protoFile, *protoTarget)
	if err != nil {
		log.Fatalf("gat-gateway: proto %s: %v", *protoFile, err)
	}
	for i := range protoRegs {
		// gwag registers this upstream as `hello_proto` (from the
		// service's advertised name); gat derives `hello` from the
		// proto package. Align so both gateways answer the same query.
		protoRegs[i].Service.Namespace = "hello_proto"
	}
	regs = append(regs, protoRegs...)

	openapiReg, err := openAPIRegistration(*openapiURL, *openapiTarget, "hello_openapi")
	if err != nil {
		log.Fatalf("gat-gateway: openapi %s: %v", *openapiURL, err)
	}
	regs = append(regs, openapiReg)

	g, err := gat.New(regs...)
	if err != nil {
		log.Fatalf("gat-gateway: build: %v", err)
	}

	mux := http.NewServeMux()
	if err := gat.RegisterHTTP(mux, g, *prefix); err != nil {
		log.Fatalf("gat-gateway: mount: %v", err)
	}
	// The sweep's readiness probe and the traffic driver both speak
	// GraphQL, but a plain liveness path makes a failed boot obvious in
	// the log rather than as a probe timeout.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("gat-gateway: %d service(s) on %s; POST %s/graphql",
		len(regs), *addr, strings.TrimRight(*prefix, "/"))
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("gat-gateway: %v", err)
	}
}

// openAPIRegistration fetches the upstream's own OpenAPI document and
// ingests it, the same way an adopter pointing gat at a running service
// would. Fetching rather than reading a checked-in copy keeps the spec
// in step with whatever hello-openapi actually serves.
func openAPIRegistration(specURL, target, namespace string) (gat.ServiceRegistration, error) {
	resp, err := http.Get(specURL)
	if err != nil {
		return gat.ServiceRegistration{}, fmt.Errorf("fetch spec: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return gat.ServiceRegistration{}, fmt.Errorf("fetch spec: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return gat.ServiceRegistration{}, fmt.Errorf("read spec: %w", err)
	}

	doc, err := openapi3.NewLoader().LoadFromData(body)
	if err != nil {
		return gat.ServiceRegistration{}, fmt.Errorf("parse spec: %w", err)
	}
	svc := ir.IngestOpenAPI(doc)
	svc.Namespace = namespace
	svc.Version = "v1"
	ir.PopulateSchemaIDs(svc)

	return gat.ServiceRegistration{Service: svc, BaseURL: target}, nil
}
