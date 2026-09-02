// Gateway binary: serves GraphQL on :8080 and a control plane on :50090.
// Boots empty; services register themselves at runtime. Optionally
// embeds a NATS server and forms a cluster with peer gateways.
//
//	# single-node
//	$ go run ./cmd/gateway --nats-data /tmp/gw1
//
//	# 3-node, gateway 1 is the seed
//	$ go run ./cmd/gateway --nats-data /tmp/gw1 \
//	    --nats-listen :14222 --nats-cluster :14248
//	$ go run ./cmd/gateway --nats-data /tmp/gw2 \
//	    --nats-listen :14223 --nats-cluster :14249 \
//	    --http :8081 --control-plane :50091 --nats-peer localhost:14248
//	$ go run ./cmd/gateway --nats-data /tmp/gw3 \
//	    --nats-listen :14224 --nats-cluster :14250 \
//	    --http :8082 --control-plane :50092 --nats-peer localhost:14248
package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http/httptest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	gateway "github.com/iodesystems/gwag/v2/gw"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
	gwui "github.com/iodesystems/gwag/v2/ui"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// apiOrUI is the catch-all for the example mux: unmatched /api/*
// returns JSON 404 (so a typo in a client doesn't render the SPA);
// everything else is the embedded UI bundle, with index.html
// fallback for SPA routes.
func apiOrUI(uiFS http.Handler) http.HandlerFunc {
	ui := uiFS
	if ui == nil {
		ui = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "ui not embedded", http.StatusNotFound)
		})
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			esc := strings.ReplaceAll(r.URL.Path, `\`, `\\`)
			esc = strings.ReplaceAll(esc, `"`, `\"`)
			fmt.Fprintf(w, `{"error":"not found","path":"%s"}`, esc)
			return
		}
		ui.ServeHTTP(w, r)
	}
}

func main() {
	httpAddr := flag.String("http", ":8080", "GraphQL HTTP listen address")
	cpAddr := flag.String("control-plane", ":50090", "Control-plane gRPC listen address")
	natsListen := flag.String("nats-listen", ":14222", "Embedded NATS client listen address")
	natsCluster := flag.String("nats-cluster", ":14248", "Embedded NATS cluster route listen address")
	natsData := flag.String("nats-data", "", "JetStream storage directory (required to enable cluster mode)")
	nodeName := flag.String("node-name", "", "Cluster node name (defaults to control-plane addr)")
	var natsPeers stringList
	flag.Var(&natsPeers, "nats-peer", "Cluster peer route, repeatable (e.g. localhost:14248)")
	tlsCert := flag.String("tls-cert", "", "Server cert (PEM); enables mTLS on cluster routes + outbound gRPC")
	tlsKey := flag.String("tls-key", "", "Server key (PEM); pair with --tls-cert")
	tlsCA := flag.String("tls-ca", "", "CA bundle (PEM) used to verify peer certs")
	clusterName := flag.String("nats-cluster-name", "", "NATS cluster identifier; empty defaults to \"gwag\". Set distinct values across deployments that must not federate.")
	maxInflight := flag.Int("max-inflight", gateway.DefaultBackpressure.MaxInflight, "Per-pool unary dispatch concurrency cap; 0 disables")
	maxStreams := flag.Int("max-streams", gateway.DefaultBackpressure.MaxStreams, "Per-pool subscription stream cap; 0 disables")
	maxStreamsTotal := flag.Int("max-streams-total", gateway.DefaultBackpressure.MaxStreamsTotal, "Gateway-wide subscription stream cap; 0 disables")
	maxWait := flag.Duration("max-wait", gateway.DefaultBackpressure.MaxWaitTime, "Per-dispatch wait budget; exceeded → backoff reject; 0 disables")
	insecureSubscribe := flag.Bool("insecure-subscribe", false, "Disable HMAC verification on subscriptions (dev only)")
	subscribeSecret := flag.String("subscribe-secret", "", "Hex-encoded shared HMAC secret for subscription verification")
	subscribeSkew := flag.Duration("subscribe-skew", 0, "Accepted timestamp drift on subscribe HMACs; 0 → 5min default")
	signerSecret := flag.String("signer-secret", "", "Hex-encoded bearer for the gRPC SignSubscriptionToken RPC; admin token also works as fallback")
	pprofEnable := flag.Bool("pprof", false, "Expose net/http/pprof under /debug/pprof behind AdminMiddleware. Off by default — pprof leaks goroutine and heap state, never make it public.")
	adminDataDir := flag.String("admin-data-dir", "", "Directory under which the gateway persists its boot admin token (empty → in-memory only). Set this to a writable path so a worked example (e.g. cmd/mcp-demo) can read <dir>/admin-token without scraping logs. Implicitly set when --nats-data is.")
	genMode := flag.Bool("gen", false, "Build the static admin GraphQL schema and print SDL to stdout, then exit. No cluster, no listeners — the gateway is constructed in-process, the admin OpenAPI is self-ingested, and SchemaHandler renders the SDL the UI codegen consumes.")
	flag.Parse()

	if *genMode {
		emitSchemaSDL()
		return
	}

	var mtls *tls.Config
	if *tlsCert != "" || *tlsKey != "" || *tlsCA != "" {
		c, err := gateway.LoadMTLSConfig(*tlsCert, *tlsKey, *tlsCA)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
		mtls = c
	}

	var cluster *gateway.Cluster
	if *natsData != "" {
		name := *nodeName
		if name == "" {
			name = "gw" + *cpAddr
		}
		c, err := gateway.StartCluster(gateway.ClusterOptions{
			NodeName:      name,
			ClientListen:  *natsListen,
			ClusterListen: *natsCluster,
			Peers:         natsPeers,
			DataDir:       *natsData,
			ClusterName:   *clusterName,
			TLS:           mtls,
		})
		if err != nil {
			log.Fatalf("start cluster: %v", err)
		}
		cluster = c
		log.Printf("nats client=%s cluster=%s data=%s peers=%v node=%s tls=%v",
			*natsListen, *natsCluster, *natsData, []string(natsPeers), c.NodeID, mtls != nil)
	}

	var gwOpts []gateway.Option
	if cluster != nil {
		gwOpts = append(gwOpts, gateway.WithCluster(cluster))
	}
	if mtls != nil {
		gwOpts = append(gwOpts, gateway.WithTLS(mtls))
	}
	// Persist the admin token under whichever data dir the operator
	// configured. `--admin-data-dir` wins; `--nats-data` is the
	// historical default (one dir for everything cluster-mode needs).
	// Standalone gateways without either get a fresh in-memory token
	// each boot.
	tokenDir := *adminDataDir
	if tokenDir == "" {
		tokenDir = *natsData
	}
	if tokenDir != "" {
		gwOpts = append(gwOpts, gateway.WithAdminDataDir(tokenDir))
	}
	gwOpts = append(gwOpts, gateway.WithBackpressure(gateway.BackpressureOptions{
		MaxInflight:     *maxInflight,
		MaxStreams:      *maxStreams,
		MaxStreamsTotal: *maxStreamsTotal,
		MaxWaitTime:     *maxWait,
	}))

	switch {
	case *insecureSubscribe:
		gwOpts = append(gwOpts, gateway.WithoutSubscriptionAuth())
	case *subscribeSecret != "":
		secret, err := hex.DecodeString(*subscribeSecret)
		if err != nil {
			log.Fatalf("subscribe-secret must be hex: %v", err)
		}
		gwOpts = append(gwOpts, gateway.WithSubscriptionAuth(gateway.SubscriptionAuthOptions{
			Secret:     secret,
			SkewWindow: *subscribeSkew,
		}))
	}
	if *signerSecret != "" {
		secret, err := hex.DecodeString(*signerSecret)
		if err != nil {
			log.Fatalf("signer-secret must be hex: %v", err)
		}
		gwOpts = append(gwOpts, gateway.WithSignerSecret(secret))
	}
	if *pprofEnable {
		gwOpts = append(gwOpts, gateway.WithPprof())
	}
	// Seed the MCP allowlist so /mcp exposes greeter + library + admin
	// out of the box. Operators retune at runtime via
	// /api/admin/mcp/{include,exclude,auto-include}.
	gwOpts = append(gwOpts, gateway.WithMCPInclude(
		"greeter.**",
		"library.**",
		"admin.**",
	))
	gw := gateway.New(gwOpts...)

	// ----------------------------------------------------------------
	// Worked-example injectors (plan §1, "InjectType / InjectPath /
	// InjectHeader"). Two of three flavors land here; the third —
	// hide-and-fill via InjectType[*authpb.Context] — lives in
	// examples/auth so the gateway here stays generic (no
	// service-specific proto imports).
	//
	// 1. Inspect-and-default for greeter.Hello.name. Hide(false) keeps
	//    `name` on the external schema; Nullable(true) flips it to
	//    optional. The resolver returns nil to pass through whatever
	//    the caller sent, or the source IP when they omitted it.
	//
	// 2. Header inject for X-Source-IP. Hide(true) (default) ignores
	//    any inbound value; the gateway stamps the header on every
	//    outbound dispatch (HTTP via OpenAPI; gRPC metadata for
	//    proto). The greeter logs incoming metadata so you can see it
	//    land.
	gw.Use(gateway.InjectPath("greeter.Hello.name", func(ctx context.Context, current any) (any, error) {
		if current != nil {
			return nil, nil // pass through caller's value
		}
		return clientIP(ctx), nil
	}, gateway.Hide(false), gateway.Nullable(true)))

	gw.Use(gateway.InjectHeader("X-Source-IP", func(ctx context.Context, _ *string) (string, error) {
		return clientIP(ctx), nil
	}))

	cpLis, err := net.Listen("tcp", *cpAddr)
	if err != nil {
		log.Fatalf("listen control plane: %v", err)
	}
	var grpcOpts []grpc.ServerOption
	if mtls != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(mtls)))
	}
	// Mount GRPCUnknownHandler on the same gRPC server that hosts
	// ControlPlane — gw doc's recommended pattern. Any registered
	// service (proto / OpenAPI / GraphQL) becomes reachable as gRPC
	// at /<svc.FullName>/<method> via the IR-translation layer.
	grpcOpts = append(grpcOpts, grpc.UnknownServiceHandler(gw.GRPCUnknownHandler()))
	srv := grpc.NewServer(grpcOpts...)
	cpv1.RegisterControlPlaneServer(srv, gw.ControlPlane())

	// Dogfood: define admin routes via huma → emit OpenAPI →
	// self-ingest into the GraphQL surface. Same path any external
	// huma-defined service takes; the UI talks to /graphql only.
	adminMux, adminSpec, err := gw.AdminHumaRouter()
	if err != nil {
		log.Fatalf("admin huma router: %v", err)
	}
	go func() {
		log.Printf("control plane listening on %s", *cpAddr)
		if err := srv.Serve(cpLis); err != nil {
			log.Fatalf("control plane serve: %v", err)
		}
	}()

	// API routes live under /api/. The catch-all on / serves the
	// embedded UI (and falls back to its index.html for unknown paths
	// so client-side routing works), with an explicit JSON 404 for
	// unmatched /api/* so misroutes don't masquerade as the SPA. This
	// is the zdx-go convention.
	mux := http.NewServeMux()
	mux.Handle("/api/graphql", gw.Handler())
	// REST-shape ingress: any registered service becomes reachable as
	// HTTP/JSON. Proto services land at POST /<pkg>.<Service>/<method>;
	// OpenAPI services at their declared path/method. Mounted under
	// /api/ingress/ so it doesn't collide with the SPA catch-all on /.
	mux.Handle("/api/ingress/", http.StripPrefix("/api/ingress", gw.IngressHandler()))
	mux.Handle("/api/schema", gw.SchemaHandler()) // back-compat alias
	mux.Handle("/api/schema/graphql", gw.SchemaHandler())
	mux.Handle("/api/schema/proto", gw.SchemaProtoHandler())
	mux.Handle("/api/schema/openapi", gw.SchemaOpenAPIHandler())
	mux.Handle("/api/metrics", gw.MetricsHandler())
	mux.Handle("/api/health", gw.HealthHandler())
	// Bearer-gated: writes require the boot token; reads stay public so
	// the UI's services-list/peer views work unauthenticated.
	mux.Handle("/api/admin/", http.StripPrefix("/api", gw.AdminMiddleware(adminMux)))
	// MCP Streamable HTTP transport at /mcp. MountMCP wraps the
	// handler in AdminMiddleware (bearer-gated) and registers at the
	// conventional path; the allowlist is seeded above via
	// WithMCPInclude and tunable at runtime via /api/admin/mcp/*.
	// See examples/multi/cmd/mcp-demo for a worked client.
	gw.MountMCP(mux)
	if pmux := gw.PprofMux(); pmux != nil {
		// Bearer-gated. pprof leaks goroutine + heap state; never serve it
		// without auth.
		mux.Handle("/debug/pprof/", gw.AdminMiddleware(pmux))
		log.Printf("pprof enabled at /debug/pprof (admin bearer required)")
	}
	mux.Handle("/", apiOrUI(gateway.UIHandler(gwui.FS())))
	if path := gw.AdminTokenPath(); path != "" {
		log.Printf("admin token = %s  (persisted to %s)", gw.AdminTokenHex(), path)
	} else {
		log.Printf("admin token = %s  (in-memory, regenerated on restart)", gw.AdminTokenHex())
	}

	// Self-ingest the huma admin OpenAPI so its operations become
	// GraphQL fields under namespace "admin". Note the base URL points
	// at /api so the OpenAPI dispatch path resolves to the gated mount.
	if err := gw.AddOpenAPIBytes(adminSpec,
		gateway.As("admin"),
		gateway.To("http://localhost"+*httpAddr+"/api")); err != nil {
		log.Fatalf("self-ingest admin openapi: %v", err)
	}

	// Surface admin_events_watchServices in the Subscription type.
	// The gateway publishes ServiceChange events to NATS whenever its
	// registry mutates; UI clients (and any subscriber) see them in
	// real time. Cluster-less invocations still register the schema
	// fragment — runtime subscriptions return a clean error if no
	// broker resolves.
	if err := gw.AddAdminEvents(); err != nil {
		log.Fatalf("AddAdminEvents: %v", err)
	}
	go func() {
		log.Printf("graphql listening on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, mux); err != nil {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	// Graceful drain: /health flips to 503 immediately so the LB pulls
	// us out, active subscriptions get cancelled, then we wait up to
	// 30s for streams to finish. After that, gRPC and NATS shut down.
	log.Printf("draining...")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := gw.Drain(drainCtx); err != nil {
		log.Printf("drain returned: %v (proceeding with shutdown)", err)
	} else {
		log.Printf("drained cleanly")
	}
	drainCancel()

	srv.GracefulStop()
	cluster.Close()
}

// clientIP returns the caller's IP for the worked-example injectors.
// Prefers the first hop in X-Forwarded-For (set by an upstream proxy
// you trust), then Forwarded.for=, then the raw RemoteAddr. Returns
// "" off the HTTP path. Trust the XFF chain only if your edge actually
// strips it from untrusted callers — this example reads it
// unconditionally because the demo runs locally.
func clientIP(ctx context.Context) string {
	r := gateway.HTTPRequestFromContext(ctx)
	if r == nil {
		return ""
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if v := r.Header.Get("X-Real-IP"); v != "" {
		return v
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// emitSchemaSDL is the --gen entry point. Constructs a fresh gateway
// in-process (no cluster, no listeners), self-ingests the admin
// huma-OpenAPI surface — same path the runtime gateway takes — and
// renders the resulting SDL via the existing SchemaHandler. Output
// goes to stdout so `pnpm run schema` can redirect it into
// schema.graphql without standing up a live process.
//
// Logs are routed to stderr so stdout stays clean for the SDL bytes
// the consumer will write to a file. The base URL passed to
// AddOpenAPIBytes is "http://localhost/api" — purely a placeholder
// since no dispatch happens in --gen mode, but a valid URL is
// required by the OpenAPI ingest.
func emitSchemaSDL() {
	log.SetOutput(os.Stderr)
	gw := gateway.New()
	defer gw.Close()
	_, adminSpec, err := gw.AdminHumaRouter()
	if err != nil {
		log.Fatalf("admin huma router: %v", err)
	}
	if err := gw.AddOpenAPIBytes(adminSpec,
		gateway.As("admin"),
		gateway.To("http://localhost/api")); err != nil {
		log.Fatalf("self-ingest admin openapi: %v", err)
	}
	if err := gw.AddAdminEvents(); err != nil {
		log.Fatalf("AddAdminEvents: %v", err)
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/schema/graphql", nil)
	gw.SchemaHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		log.Fatalf("schema render: status=%d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stdout.Write(rr.Body.Bytes()); err != nil {
		log.Fatalf("write stdout: %v", err)
	}
}
