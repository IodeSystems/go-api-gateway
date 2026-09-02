// hello-openapi: an HTTP/JSON service that exposes a single Hello
// operation, self-registers with the gateway via the OpenAPI control
// plane, and exists primarily so bench traffic openapi --direct has
// a format-native upstream to compare against.
//
// Mirrors hello-proto + hello-graphql so the bench can characterise
// each ingress against an upstream that natively speaks the matching
// format. Production OpenAPI services go through gateway.AddOpenAPI /
// AddOpenAPIBytes the same way; this binary is just a tiny stand-in.
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
	"syscall"

	"github.com/iodesystems/gwag/v2/gw/controlclient"
)

const helloSpec = `{
  "openapi": "3.0.0",
  "info": {
    "title": "hello-openapi",
    "version": "1.0.0",
    "description": "Format-native OpenAPI sibling of hello-proto and hello-graphql; the direct-dial baseline for ` + "`bench traffic openapi --direct`" + `."
  },
  "paths": {
    "/Hello": {
      "post": {
        "operationId": "Hello",
        "summary": "Greet the named recipient.",
        "description": "Returns a one-line greeting derived from the request name. Mirrors HelloService.Hello on the proto upstream.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "description": "Hello request body.",
                "properties": {
                  "name": {
                    "type": "string",
                    "description": "Recipient name; echoed back in the greeting."
                  }
                },
                "required": ["name"]
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Greeting returned successfully.",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "description": "Hello reply body.",
                  "properties": {
                    "greeting": {
                      "type": "string",
                      "description": "Greeting line, e.g. \"Hello, alice!\"."
                    }
                  },
                  "required": ["greeting"]
                }
              }
            }
          }
        }
      }
    }
  }
}`

type helloReq struct {
	Name string `json:"name"`
}

type helloResp struct {
	Greeting string `json:"greeting"`
}

func main() {
	addr := flag.String("addr", ":50053", "HTTP listen address")
	gatewayAddr := flag.String("gateway", "localhost:50090", "Gateway control plane address")
	advertise := flag.String("advertise", "", "HTTP base URL to advertise to the gateway (e.g. http://localhost:50053). Defaults to http://<addr>.")
	namespace := flag.String("namespace", "hello_openapi", "Namespace to register under")
	version := flag.String("version", "v1", "Service version (unstable / vN)")
	register := flag.Bool("register", true, "Self-register with the gateway's control plane. Set false when running against a non-gwag gateway (Apollo Router, graphql-mesh) that introspects backends directly.")
	flag.Parse()

	if *advertise == "" {
		host := *addr
		if strings.HasPrefix(host, ":") {
			host = "localhost" + host
		}
		*advertise = "http://" + host
	}

	mux := http.NewServeMux()
	// /openapi.json serves the embedded spec so non-gwag gateways
	// (graphql-mesh, etc.) that introspect via standard discovery can
	// pull the schema directly from us.
	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(helloSpec))
	})
	mux.HandleFunc("/Hello", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var in helloReq
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(helloResp{Greeting: "Hello, " + in.Name + "!"})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("hello-openapi HTTP listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	var reg *controlclient.Registration
	if *register {
		var err error
		reg, err = controlclient.SelfRegister(context.Background(), controlclient.Options{
			GatewayAddr: *gatewayAddr,
			ServiceAddr: *advertise,
			InstanceID:  fmt.Sprintf("hello-openapi@%s", *addr),
			Services: []controlclient.Service{{
				Namespace:   *namespace,
				Version:     *version,
				OpenAPISpec: []byte(helloSpec),
			}},
		})
		if err != nil {
			log.Fatalf("self-register: %v", err)
		}
		log.Printf("hello-openapi registered with %s as %s:%s (advertise %s)", *gatewayAddr, *namespace, *version, *advertise)
	} else {
		log.Printf("hello-openapi: --register=false, skipping control-plane registration")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("hello-openapi shutting down")
	if reg != nil {
		_ = reg.Close(context.Background())
	}
	_ = srv.Shutdown(context.Background())
}
