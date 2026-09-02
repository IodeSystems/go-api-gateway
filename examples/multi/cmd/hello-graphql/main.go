// hello-graphql: a tiny native GraphQL service self-registering with
// the gateway via the GraphQL stitching path. Sibling of hello-proto +
// hello-openapi; the gateway introspects this endpoint at register
// time and forwards `hello(name: ...)` queries back to it.
//
// Exists primarily so bench traffic graphql --direct has a format-
// native upstream to compare against. The gateway's `gateway.AddGraphQL`
// path is the same one external GraphQL services take in production;
// this binary is just a tiny stand-in.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/gw/controlclient"
)

func newSchema() (graphql.Schema, error) {
	helloPayload := graphql.NewObject(graphql.ObjectConfig{
		Name:        "HelloPayload",
		Description: "Hello reply object.",
		Fields: graphql.Fields{
			"greeting": &graphql.Field{
				Type:        graphql.NewNonNull(graphql.String),
				Description: "Greeting line, e.g. \"Hello, alice!\".",
			},
		},
	})
	root := graphql.NewObject(graphql.ObjectConfig{
		Name:        "Query",
		Description: "Format-native GraphQL sibling of hello-proto and hello-openapi; the direct-dial baseline for `bench traffic graphql --direct`.",
		Fields: graphql.Fields{
			"hello": &graphql.Field{
				Type:        graphql.NewNonNull(helloPayload),
				Description: "Greet the named recipient.",
				Args: graphql.FieldConfigArgument{
					"name": &graphql.ArgumentConfig{
						Type:        graphql.NewNonNull(graphql.String),
						Description: "Recipient name; echoed back in the greeting.",
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					name, _ := p.Args["name"].(string)
					return map[string]any{"greeting": "Hello, " + name + "!"}, nil
				},
			},
		},
	})
	return graphql.NewSchema(graphql.SchemaConfig{Query: root})
}

func main() {
	addr := flag.String("addr", ":50054", "HTTP listen address")
	gatewayAddr := flag.String("gateway", "localhost:50090", "Gateway control plane address")
	advertise := flag.String("advertise", "", "GraphQL endpoint URL to advertise to the gateway (e.g. http://localhost:50054/graphql). Defaults to http://<addr>/graphql.")
	namespace := flag.String("namespace", "hello_graphql", "Namespace to register under")
	version := flag.String("version", "v1", "Service version (unstable / vN)")
	path := flag.String("path", "/graphql", "HTTP path on which to serve GraphQL")
	register := flag.Bool("register", true, "Self-register with the gateway's control plane. Set false when running against a non-gwag gateway (Apollo Router, graphql-mesh) that introspects backends directly.")
	flag.Parse()

	if *advertise == "" {
		host := *addr
		if strings.HasPrefix(host, ":") {
			host = "localhost" + host
		}
		*advertise = "http://" + host + *path
	}

	schema, err := newSchema()
	if err != nil {
		log.Fatalf("build schema: %v", err)
	}
	// Plan cache + ExecutePlanAppend on the upstream side mirrors the
	// gateway's hot path: parse + validate + plan happens once per
	// distinct query string; repeat requests skip the parser+validator
	// entirely. Without this, every request through the bench burns
	// ~600µs in graphql-go/handler's fresh parse-validate-plan walk,
	// dwarfing the gateway's own per-request cost (~30µs self-time)
	// and pushing the graphql scenario's ceiling 2.5× below proto/openapi
	// on the matrix sweep.
	planCache := graphql.NewPlanCache(graphql.PlanCacheOptions{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveGraphQL(w, r, &schema, planCache)
	})
	mux := http.NewServeMux()
	mux.Handle(*path, h)

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("hello-graphql listening on %s%s", *addr, *path)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	var reg *controlclient.Registration
	if *register {
		var err error
		reg, err = controlclient.SelfRegister(context.Background(), controlclient.Options{
			GatewayAddr: *gatewayAddr,
			// ServiceAddr is ignored for GraphQL bindings — the endpoint URL
			// is the dispatch destination too — but Options requires a non-
			// empty value, so feed it the same URL.
			ServiceAddr: *advertise,
			InstanceID:  fmt.Sprintf("hello-graphql@%s", *addr),
			Services: []controlclient.Service{{
				Namespace:       *namespace,
				Version:         *version,
				GraphQLEndpoint: *advertise,
			}},
		})
		if err != nil {
			log.Fatalf("self-register: %v", err)
		}
		log.Printf("hello-graphql registered with %s as %s:%s (endpoint %s)", *gatewayAddr, *namespace, *version, *advertise)
	} else {
		log.Printf("hello-graphql: --register=false, skipping control-plane registration")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("hello-graphql shutting down")
	if reg != nil {
		_ = reg.Close(context.Background())
	}
	_ = srv.Shutdown(context.Background())
}

// gqlRequest is the GraphQL-over-HTTP request body we accept.
// graphql-multipart-spec / GET-style query params are not supported —
// this binary only exists to be hammered by the bench, which always
// POSTs application/json.
type gqlRequest struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	OperationName string                 `json:"operationName,omitempty"`
}

// responseBufPool keeps the per-request []byte off the GC's hands.
// Cap at 64 KB; one-off megabyte responses would otherwise pin a fat
// allocation in the pool forever.
const responseBufMax = 64 * 1024

var responseBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func serveGraphQL(w http.ResponseWriter, r *http.Request, schema *graphql.Schema, planCache *graphql.PlanCache) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req gqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	pr := planCache.Get(schema, req.Query, req.OperationName)
	if len(pr.Errors) > 0 {
		_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: pr.Errors})
		return
	}
	args := req.Variables
	if len(pr.SynthArgs) > 0 {
		merged := make(map[string]interface{}, len(args)+len(pr.SynthArgs))
		for k, v := range args {
			merged[k] = v
		}
		for k, v := range pr.SynthArgs {
			merged[k] = v
		}
		args = merged
	}
	buf := responseBufPool.Get().(*[]byte)
	body, errs := graphql.ExecutePlanAppend(pr.Plan, graphql.ExecuteParams{
		Schema:        *schema,
		OperationName: req.OperationName,
		Args:          args,
		Context:       r.Context(),
	}, (*buf)[:0])
	if len(errs) > 0 {
		_ = json.NewEncoder(w).Encode(&graphql.Result{Errors: errs})
	} else {
		_, _ = w.Write(body)
	}
	if cap(body) <= responseBufMax {
		*buf = body[:0]
		responseBufPool.Put(buf)
	}
}
