// gwag up: zero-config full-featured gateway boot. Embedded NATS
// (subscriptions + cluster-ready), persistent admin token, admin
// huma routes self-ingested as admin_* GraphQL fields, /metrics +
// /health + /schema/* endpoints, and the UI bundle on /. Auto-
// registers a primary login in .gw/credentials.json so subsequent
// gwag subcommands resolve without flags.
//
// State for the running gateway lives under .gw/contexts/<name>/data/
// (admin-token, JetStream). Each --context name gets its own subdir,
// so multiple gwag-up invocations from one project (e.g. local +
// staging-mirror) don't share state.
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	gateway "github.com/iodesystems/gwag/v2/gw"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
	gwui "github.com/iodesystems/gwag/v2/ui"
)

type peerList []string

func (p *peerList) String() string     { return strings.Join(*p, ",") }
func (p *peerList) Set(v string) error { *p = append(*p, v); return nil }

func upCmd(args []string) int {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	httpAddr := fs.String("addr", ":8080", "HTTP listen address")
	cpAddr := fs.String("control-plane", ":50090", "Control-plane gRPC listen address")
	natsListen := fs.String("nats-listen", ":14222", "Embedded NATS client listen address")
	natsCluster := fs.String("nats-cluster", ":14248", "Embedded NATS cluster route address")
	var natsPeers peerList
	fs.Var(&natsPeers, "nats-peer", "Cluster peer route, repeatable")
	natsClusterName := fs.String("nats-cluster-name", "", "NATS cluster identifier; empty = default")
	nodeName := fs.String("node-name", "", "Cluster node name (defaults to control-plane addr)")
	dataDir := fs.String("data-dir", "", "Persistent state directory (default: .gw/contexts/<context>/data)")
	ctxName := fs.String("context", "", "Login name in .gw/credentials.json to register and use (default: primary, or 'default' if .gw is empty)")
	noContext := fs.Bool("no-context-write", false, "Skip auto-writing the credential entry to .gw/credentials.json")
	allowTier := fs.String("allow-tier", "unstable,stable,vN", "Comma-separated tiers accepted by this gateway")
	insecureSubscribe := fs.Bool("insecure-subscribe", false, "Disable HMAC verification on subscriptions (dev only)")
	signerSecret := fs.String("signer-secret", "", "Hex-encoded bearer for SignSubscriptionToken (admin token also works)")
	pprofEnable := fs.Bool("pprof", false, "Mount net/http/pprof under /debug/pprof behind AdminMiddleware")
	mcpUpstreams := &stringListValue{}
	fs.Var(mcpUpstreams, "mcp-upstream", "Ingest a downstream MCP server's tools — NS:TRANSPORT:TARGET (transport = stdio|http|sse; repeatable)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: gwag up [flags]")
		fmt.Fprintln(fs.Output(), "  Boots a standalone gateway: embedded NATS, admin endpoints, UI,")
		fmt.Fprintln(fs.Output(), "  metrics, health. Defaults are zero-config; pass --nats-peer to")
		fmt.Fprintln(fs.Output(), "  join an existing cluster.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}

	tiers, err := parseAllowTier(*allowTier)
	if err != nil {
		fmt.Fprintf(os.Stderr, "--allow-tier: %v\n", err)
		return 2
	}

	// Resolve which credential entry this invocation owns. Default
	// is always "default" — never silently overwrite an existing
	// remote primary (e.g. operator has --name staging set, runs
	// gwag up; the local gateway should land in a distinct slot).
	resolvedCtx := *ctxName
	if resolvedCtx == "" {
		resolvedCtx = "default"
	}

	dir := *dataDir
	if dir == "" {
		dir = contextDataDir(resolvedCtx)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Fatalf("data-dir %s: %v", dir, err)
	}

	clusterNodeName := *nodeName
	if clusterNodeName == "" {
		clusterNodeName = "gwag" + *cpAddr
	}
	cluster, err := gateway.StartCluster(gateway.ClusterOptions{
		NodeName:      clusterNodeName,
		ClientListen:  *natsListen,
		ClusterListen: *natsCluster,
		Peers:         natsPeers,
		DataDir:       dir,
		ClusterName:   *natsClusterName,
	})
	if err != nil {
		log.Fatalf("start cluster: %v", err)
	}
	log.Printf("nats client=%s cluster=%s data=%s peers=%v node=%s",
		*natsListen, *natsCluster, dir, []string(natsPeers), cluster.NodeID)

	gwOpts := []gateway.Option{
		gateway.WithAllowTier(tiers...),
		gateway.WithCluster(cluster),
		gateway.WithAdminDataDir(dir),
	}
	if *insecureSubscribe {
		gwOpts = append(gwOpts, gateway.WithoutSubscriptionAuth())
	}
	if *signerSecret != "" {
		secret, err := hex.DecodeString(*signerSecret)
		if err != nil {
			log.Fatalf("--signer-secret must be hex: %v", err)
		}
		gwOpts = append(gwOpts, gateway.WithSignerSecret(secret))
	}
	if *pprofEnable {
		gwOpts = append(gwOpts, gateway.WithPprof())
	}
	gw := gateway.New(gwOpts...)

	cpLis, err := net.Listen("tcp", *cpAddr)
	if err != nil {
		log.Fatalf("listen control plane: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(gw.GRPCUnknownHandler()))
	cpv1.RegisterControlPlaneServer(srv, gw.ControlPlane())
	go func() {
		log.Printf("control plane listening on %s", *cpAddr)
		if err := srv.Serve(cpLis); err != nil {
			log.Fatalf("control plane serve: %v", err)
		}
	}()

	adminMux, adminSpec, err := gw.AdminHumaRouter()
	if err != nil {
		log.Fatalf("admin huma router: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/graphql", gw.Handler())
	mux.Handle("/api/ingress/", http.StripPrefix("/api/ingress", gw.IngressHandler()))
	mux.Handle("/api/schema", gw.SchemaHandler())
	mux.Handle("/api/schema/graphql", gw.SchemaHandler())
	mux.Handle("/api/schema/proto", gw.SchemaProtoHandler())
	mux.Handle("/api/schema/openapi", gw.SchemaOpenAPIHandler())
	mux.Handle("/api/metrics", gw.MetricsHandler())
	mux.Handle("/api/health", gw.HealthHandler())
	mux.Handle("/api/admin/", http.StripPrefix("/api", gw.AdminMiddleware(adminMux)))
	gw.MountMCP(mux)
	if pmux := gw.PprofMux(); pmux != nil {
		mux.Handle("/debug/pprof/", gw.AdminMiddleware(pmux))
		log.Printf("pprof enabled at /debug/pprof (admin bearer required)")
	}
	mux.Handle("/", apiOrUIHandler(gateway.UIHandler(gwui.FS())))

	for _, v := range mcpUpstreams.values {
		ns, transport, target, err := parseMCPUpstream(v)
		if err != nil {
			log.Fatalf("--mcp-upstream: %v", err)
		}
		if err := gw.AddMCP(transport, target, gateway.As(ns)); err != nil {
			log.Fatalf("register mcp upstream %q: %v", v, err)
		}
		log.Printf("registered mcp upstream %s (%s) → namespace %s", target, transport, ns)
	}

	if err := gw.AddOpenAPIBytes(adminSpec,
		gateway.As("admin"),
		gateway.To("http://localhost"+*httpAddr+"/api")); err != nil {
		log.Fatalf("self-ingest admin openapi: %v", err)
	}

	if err := gw.AddAdminEvents(); err != nil {
		log.Fatalf("AddAdminEvents: %v", err)
	}

	token := gw.AdminTokenHex()
	if path := gw.AdminTokenPath(); path != "" {
		log.Printf("admin token = %s  (persisted to %s)", token, path)
	} else {
		log.Printf("admin token = %s  (in-memory)", token)
	}

	// Auto-login: write the credential entry and promote it to
	// primary so subsequent `gwag` subcommands resolve here without
	// flags. Demotes any other primary; operator switches back with
	// `gwag use NAME`. Skip with --no-context-write.
	if !*noContext {
		// gwag up manages the project-local context; load local-only so a
		// global fallback isn't copied into a fresh .gw/.
		creds, _ := loadCredentialsAt(credentialsFile)
		entry := loginEntry{
			Name:     resolvedCtx,
			Primary:  true,
			Gateway:  endpointHost(*cpAddr),
			Endpoint: "http://" + endpointHost(*httpAddr),
			Bearer:   token,
		}
		for i := range creds.Logins {
			if creds.Logins[i].Name != resolvedCtx {
				creds.Logins[i].Primary = false
			}
		}
		creds.upsert(entry)
		if err := saveCredentials(creds); err != nil {
			log.Printf("warn: write credentials: %v", err)
		} else {
			abs, _ := filepath.Abs(credentialsFile)
			log.Printf("registered context %q (primary) in %s", resolvedCtx, abs)
		}
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

	log.Printf("draining...")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := gw.Drain(drainCtx); err != nil {
		log.Printf("drain returned: %v", err)
	} else {
		log.Printf("drained cleanly")
	}
	drainCancel()
	srv.GracefulStop()
	cluster.Close()
	return 0
}

// endpointHost converts a listen-style addr (":8080" or "host:8080")
// into a host:port suitable for a credential entry. Bare ":PORT"
// becomes "localhost:PORT" so the saved context dials through
// localhost regardless of the bind address.
func endpointHost(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

// apiOrUIHandler is the catch-all for the gwag up mux: unmatched
// /api/* returns JSON 404 (so a typo in a client doesn't render the
// SPA); everything else is the embedded UI bundle, with index.html
// fallback for SPA routes. Mirrors examples/multi/cmd/gateway's
// apiOrUI; copied here so gwag has no dependency on the example.
func apiOrUIHandler(uiFS http.Handler) http.HandlerFunc {
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
