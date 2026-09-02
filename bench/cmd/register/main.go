// register — long-running registration sidecar for the bench stack.
//
// Connects to a gateway's control plane, registers an external
// service (OpenAPI / GraphQL / proto-FileDescriptorSet) bound to
// `--baseurl`, then heartbeats until SIGINT/SIGTERM. The gateway
// dispatches to --baseurl directly; this binary only owns the
// registration lifecycle.
//
// Spawned by `bin/bench service add`. Each invocation owns one
// (namespace, version) entry on the gateway. Killing the process
// (or `bench service remove`) deregisters cleanly.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/iodesystems/gwag/v2/gw/controlclient"
)

func main() {
	gateway := flag.String("gateway", "localhost:50090", "Gateway control-plane host:port")
	name := flag.String("name", "", "Namespace to mount the service under (required)")
	version := flag.String("version", "v1", "Service version (vN)")
	baseURL := flag.String("baseurl", "", "Upstream URL the gateway dispatches to. For OpenAPI/proto: HTTP/gRPC base URL of the service. Ignored for --graphql.")
	openapi := flag.String("openapi", "", "Path or URL to an OpenAPI 3.x spec (JSON/YAML)")
	graphql := flag.String("graphql", "", "Endpoint URL of an upstream GraphQL service to introspect + ingest")
	instance := flag.String("instance", "", "Free-form instance label (e.g. \"hostname@pod\")")
	flag.Parse()

	if *name == "" {
		fmt.Fprintln(os.Stderr, "register: --name required")
		os.Exit(2)
	}
	if (*openapi == "") == (*graphql == "") {
		fmt.Fprintln(os.Stderr, "register: exactly one of --openapi / --graphql required")
		os.Exit(2)
	}

	svc := controlclient.Service{
		Namespace: *name,
		Version:   *version,
	}
	switch {
	case *openapi != "":
		spec, err := readSpec(*openapi)
		if err != nil {
			fmt.Fprintf(os.Stderr, "register: read openapi spec: %v\n", err)
			os.Exit(1)
		}
		svc.OpenAPISpec = spec
		if *baseURL == "" {
			fmt.Fprintln(os.Stderr, "register: --baseurl required for openapi services")
			os.Exit(2)
		}
	case *graphql != "":
		svc.GraphQLEndpoint = *graphql
	}

	addr := *baseURL
	if svc.GraphQLEndpoint != "" {
		// ServiceAddr is ignored for GraphQL but the controlclient
		// rejects an empty value. Pass the endpoint host:port shape.
		addr = "graphql:" + *graphql
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reg, err := controlclient.SelfRegister(ctx, controlclient.Options{
		GatewayAddr: *gateway,
		ServiceAddr: addr,
		InstanceID:  *instance,
		Services:    []controlclient.Service{svc},
	})
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered %s/%s → %s (gateway %s)", *name, *version, addr, *gateway)

	<-ctx.Done()
	log.Printf("register: shutting down")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := reg.Close(stopCtx); err != nil {
		log.Printf("deregister: %v", err)
	}
}

// readSpec returns the bytes for `loc` — file path or http(s) URL.
func readSpec(loc string) ([]byte, error) {
	if strings.HasPrefix(loc, "http://") || strings.HasPrefix(loc, "https://") {
		resp, err := http.Get(loc)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("status %s", resp.Status)
		}
		return io.ReadAll(resp.Body)
	}
	return os.ReadFile(loc)
}
