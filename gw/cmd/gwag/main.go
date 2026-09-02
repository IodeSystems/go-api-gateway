// Command gwag runs a GraphQL gateway over a list of .proto files
// and gRPC destinations supplied on the command line, or talks to a
// running gateway via subcommands.
//
// Usage:
//
//	# Zero-config local gateway: NATS + admin + UI + auto-login.
//	gwag up
//
//	# Single-upstream embedded translator (no NATS, no admin, no cluster):
//	gwag serve --openapi spec.yaml --to http://localhost:8081
//	gwag serve --proto greeter.proto --to localhost:50051
//
//	# Static configuration from .proto / OpenAPI / GraphQL sources
//	# (no NATS, no admin):
//	gwag --proto path/to/foo.proto=foo-svc:50051 \
//	     --proto path/to/bar.proto=billing@bar-svc:50051 \
//	     --openapi petstore.json=pets@http://pets-svc:8080 \
//	     --graphql reviews@https://reviews-svc/graphql \
//	     --addr :8080
//
//	# Static + runtime registration (no NATS):
//	gwag --control-plane :50090 --addr :8080
//
//	# Talk to a running gateway:
//	gwag peer list   --gateway localhost:50090
//	gwag peer forget --gateway localhost:50090 NODE_ID
//
//	# Persist named gateway logins so subcommands don't need --gateway:
//	gwag login --name local --gateway localhost:50090 --token DEADBEEF...
//	gwag login --name staging --gateway staging.gw:50090 --token ...
//	gwag context                # list logins; primary is marked
//	gwag use staging            # switch primary
//	gwag peer list              # uses primary login
//	gwag peer list --context local
//	gwag logout staging         # remove one login; --all wipes .gw/
//
// Each --proto flag takes PATH=[NAMESPACE@]ADDR. Without a namespace,
// the proto's filename stem is used. Without TLS configuration, dialing
// is insecure — fine for inside a service mesh, not the public network.
//
// --openapi takes SPEC=[NAMESPACE@]URL: SPEC is a local spec file or an
// http(s) URL, URL is the upstream base the gateway forwards to.
// --graphql takes [NAMESPACE@]URL: a downstream GraphQL endpoint
// introspected once at boot. All three static flags are repeatable and
// compose with --control-plane runtime registration.
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	gateway "github.com/iodesystems/gwag/v2/gw"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

type protoSpec struct {
	path string
	ns   string
	addr string
}

// parseAllowTier validates a --allow-tier comma-list against the
// canonical set ("unstable", "stable", "vN"). Empty is rejected — at
// least one tier must be allowed for a meaningful gateway. Whitespace
// around tokens is tolerated so the flag value can be quoted with
// spaces in shell scripts. Plan §4 boot gate.
func parseAllowTier(s string) ([]string, error) {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		switch t {
		case "unstable", "stable", "vN":
			out = append(out, t)
		default:
			return nil, fmt.Errorf("unknown tier %q (want unstable, stable, or vN)", t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one tier required")
	}
	return out, nil
}

type protoFlag []protoSpec

func (p *protoFlag) String() string { return fmt.Sprint(*p) }

func (p *protoFlag) Set(v string) error {
	eq := strings.Index(v, "=")
	if eq < 0 {
		return fmt.Errorf("expected --proto PATH=[NS@]ADDR, got %q", v)
	}
	spec := protoSpec{path: v[:eq]}
	rest := v[eq+1:]
	if at := strings.Index(rest, "@"); at >= 0 {
		spec.ns = rest[:at]
		spec.addr = rest[at+1:]
	} else {
		spec.addr = rest
	}
	if spec.path == "" || spec.addr == "" {
		return fmt.Errorf("--proto: both path and addr are required, got %q", v)
	}
	*p = append(*p, spec)
	return nil
}

type openapiSpec struct {
	spec string // local file path or http(s):// URL to the OpenAPI document
	ns   string
	url  string // upstream base URL the gateway forwards to
}

type openapiFlag []openapiSpec

func (o *openapiFlag) String() string { return fmt.Sprint(*o) }

func (o *openapiFlag) Set(v string) error {
	eq := strings.Index(v, "=")
	if eq < 0 {
		return fmt.Errorf("expected --openapi SPEC=[NS@]URL, got %q", v)
	}
	spec := v[:eq]
	ns, url := splitNSURL(v[eq+1:])
	if spec == "" || url == "" {
		return fmt.Errorf("--openapi: both spec and upstream url are required, got %q", v)
	}
	*o = append(*o, openapiSpec{spec: spec, ns: ns, url: url})
	return nil
}

type graphqlSpec struct {
	ns  string
	url string // GraphQL endpoint, introspected at boot and dispatched to
}

type graphqlFlag []graphqlSpec

func (q *graphqlFlag) String() string { return fmt.Sprint(*q) }

func (q *graphqlFlag) Set(v string) error {
	ns, url := splitNSURL(v)
	if url == "" {
		return fmt.Errorf("--graphql: endpoint URL is required, got %q", v)
	}
	*q = append(*q, graphqlSpec{ns: ns, url: url})
	return nil
}

// splitNSURL peels an optional "ns@" prefix off a URL. The '@' is a
// namespace separator only when it precedes the scheme's "://", so a URL
// carrying userinfo (http://user@host) stays intact.
func splitNSURL(s string) (ns, url string) {
	at := strings.Index(s, "@")
	if at < 0 {
		return "", s
	}
	if scheme := strings.Index(s, "://"); scheme >= 0 && at > scheme {
		return "", s
	}
	return s[:at], s[at+1:]
}

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "up":
			os.Exit(upCmd(os.Args[2:]))
		case "serve":
			os.Exit(serveCmd(os.Args[2:]))
		case "login":
			os.Exit(loginCmd(os.Args[2:]))
		case "logout":
			os.Exit(logoutCmd(os.Args[2:]))
		case "use":
			os.Exit(useCmd(os.Args[2:]))
		case "context":
			os.Exit(contextCmd(os.Args[2:]))
		case "peer":
			os.Exit(peerCmd(os.Args[2:]))
		case "services":
			os.Exit(servicesCmd(os.Args[2:]))
		case "schema":
			os.Exit(schemaCmd(os.Args[2:]))
		case "sign":
			os.Exit(signCmd(os.Args[2:]))
		}
	}
	runGateway()
}

func runGateway() {
	var protos protoFlag
	flag.Var(&protos, "proto", "PATH=[NS@]ADDR (repeatable)")
	var openapis openapiFlag
	flag.Var(&openapis, "openapi", "SPEC=[NS@]URL — SPEC is a spec file or http(s) URL, URL is the upstream base (repeatable)")
	var graphqls graphqlFlag
	flag.Var(&graphqls, "graphql", "[NS@]URL — downstream GraphQL endpoint, introspected at boot (repeatable)")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	cpAddr := flag.String("control-plane", "", "Control-plane gRPC listen address (e.g. :50090); empty = no runtime registration")
	allowTier := flag.String("allow-tier", "unstable,stable,vN", "Comma-separated tiers accepted by this gateway (subset of unstable,stable,vN); production deployments restrict to \"stable,vN\" or \"vN\"")
	adminDataDir := flag.String("admin-data-dir", "", "Directory under which the gateway persists (and reloads) its boot admin token; empty = in-memory only. Set a writable path so 'gwag sign --gateway --bearer $(cat <dir>/admin-token)' works against this gateway instead of local --secret signing.")
	mcpEnable := flag.Bool("mcp", false, "Mount the outbound MCP server at /mcp (exposes the gateway surface as MCP tools, admin-bearer gated)")
	mcpUpstreams := &stringListValue{}
	flag.Var(mcpUpstreams, "mcp-upstream", "Ingest a downstream MCP server's tools — NS:TRANSPORT:TARGET (transport = stdio|http|sse; repeatable)")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: gwag [--addr :8080] [--control-plane :50090] [--proto PATH=[NS@]ADDR ...]")
		fmt.Fprintln(flag.CommandLine.Output(), "            [--openapi SPEC=[NS@]URL ...] [--graphql [NS@]URL ...]")
		fmt.Fprintln(flag.CommandLine.Output(), "            [--admin-data-dir DIR] [--mcp] [--mcp-upstream NS:TRANSPORT:TARGET ...]")
		fmt.Fprintln(flag.CommandLine.Output(), "       gwag peer (list|forget) ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(protos) == 0 && len(openapis) == 0 && len(graphqls) == 0 && *cpAddr == "" && len(mcpUpstreams.values) == 0 {
		fmt.Fprintln(os.Stderr, "gwag: nothing to serve — pass --proto / --openapi / --graphql for static registration, --control-plane for runtime registration, --mcp-upstream to ingest an MCP server, or a combination")
		flag.Usage()
		os.Exit(2)
	}

	tiers, err := parseAllowTier(*allowTier)
	if err != nil {
		fmt.Fprintf(os.Stderr, "--allow-tier: %v\n", err)
		os.Exit(2)
	}
	gwOpts := []gateway.Option{gateway.WithAllowTier(tiers...)}
	if *adminDataDir != "" {
		gwOpts = append(gwOpts, gateway.WithAdminDataDir(*adminDataDir))
	}
	gw := gateway.New(gwOpts...)
	for _, p := range protos {
		opts := []gateway.ServiceOption{gateway.To(p.addr)}
		if p.ns != "" {
			opts = append(opts, gateway.As(p.ns))
		}
		if err := gw.AddProto(p.path, opts...); err != nil {
			log.Fatalf("register %s: %v", p.path, err)
		}
		log.Printf("registered %s → %s", p.path, p.addr)
	}
	for _, o := range openapis {
		opts := []gateway.ServiceOption{gateway.To(o.url)}
		if o.ns != "" {
			opts = append(opts, gateway.As(o.ns))
		}
		if err := gw.AddOpenAPI(o.spec, opts...); err != nil {
			log.Fatalf("register openapi %s: %v", o.spec, err)
		}
		log.Printf("registered openapi %s → %s", o.spec, o.url)
	}
	for _, q := range graphqls {
		var opts []gateway.ServiceOption
		if q.ns != "" {
			opts = append(opts, gateway.As(q.ns))
		}
		if err := gw.AddGraphQL(q.url, opts...); err != nil {
			log.Fatalf("register graphql %s: %v", q.url, err)
		}
		log.Printf("registered graphql %s", q.url)
	}
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

	if *cpAddr != "" {
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
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", gw.MetricsHandler())
	mux.Handle("/health", gw.HealthHandler())
	if *mcpEnable {
		gw.MountMCP(mux)
		log.Printf("MCP server at /mcp (admin-bearer gated)")
	}
	mux.Handle("/", gw.Handler())
	log.Printf("listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

// parseMCPUpstream parses a --mcp-upstream value of the form
// NS:TRANSPORT:TARGET. TARGET may itself contain colons (URLs,
// command lines), so the split is bounded to three fields.
func parseMCPUpstream(v string) (ns string, transport gateway.MCPTransport, target string, err error) {
	parts := strings.SplitN(v, ":", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("expected NS:TRANSPORT:TARGET, got %q", v)
	}
	ns, t, target := parts[0], parts[1], parts[2]
	if ns == "" || target == "" {
		return "", "", "", fmt.Errorf("namespace and target are both required, got %q", v)
	}
	switch gateway.MCPTransport(t) {
	case gateway.MCPStdio, gateway.MCPHTTP, gateway.MCPSSE:
		return ns, gateway.MCPTransport(t), target, nil
	default:
		return "", "", "", fmt.Errorf("unknown transport %q in %q (want stdio, http, or sse)", t, v)
	}
}

func servicesCmd(args []string) int {
	if len(args) == 0 || args[0] != "list" {
		fmt.Fprintln(os.Stderr, "Usage: gwag services list [--gateway HOST:PORT] [--context NAME]")
		return 2
	}
	flags, _ := splitFlagsAndPositionals(args[1:])
	fs := flag.NewFlagSet("services list", flag.ContinueOnError)
	gwAddr := fs.String("gateway", "", "Gateway control-plane address (default: from .gw primary or localhost:50090)")
	ctxName := fs.String("context", "", "Login name in .gw/credentials.json (default: primary)")
	if err := fs.Parse(flags); err != nil {
		return 2
	}
	login := resolveLogin(*ctxName)
	addr := resolveCtx(*gwAddr, login.Gateway, "localhost:50090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		return 1
	}
	defer conn.Close()
	client := cpv1.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ListServices(ctx, &cpv1.ListServicesRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ListServices: %v\n", err)
		return 1
	}
	if len(resp.GetServices()) == 0 {
		fmt.Println("(no services registered)")
		return 0
	}
	fmt.Printf("%-20s %-6s %-66s %s\n", "NAMESPACE", "VER", "HASH", "REPLICAS")
	for _, s := range resp.GetServices() {
		fmt.Printf("%-20s %-6s %-66s %d\n",
			s.GetNamespace(), s.GetVersion(), s.GetHashHex(), s.GetReplicaCount())
	}
	return 0
}

func schemaCmd(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: gwag schema (fetch|diff) ...")
		return 2
	}
	switch args[0] {
	case "fetch":
		return schemaFetchCmd(args[1:])
	case "diff":
		return schemaDiffCmd(args[1:])
	default:
		fmt.Fprintln(os.Stderr, "Usage: gwag schema (fetch|diff) ...")
		return 2
	}
}

func schemaFetchCmd(args []string) int {
	flags, _ := splitFlagsAndPositionals(args)
	fs := flag.NewFlagSet("schema fetch", flag.ContinueOnError)
	endpoint := fs.String("endpoint", "", "Gateway /schema endpoint URL (default: from .gw primary + /schema or http://localhost:8080/schema)")
	ctxName := fs.String("context", "", "Login name in .gw/credentials.json (default: primary)")
	jsonOut := fs.Bool("json", false, "Fetch introspection JSON instead of SDL")
	if err := fs.Parse(flags); err != nil {
		return 2
	}
	login := resolveLogin(*ctxName)
	ctxEndpoint := ""
	if login.Endpoint != "" {
		ctxEndpoint = strings.TrimRight(login.Endpoint, "/") + "/schema"
	}
	url := resolveCtx(*endpoint, ctxEndpoint, "http://localhost:8080/schema")
	if *jsonOut {
		if strings.Contains(url, "?") {
			url += "&format=json"
		} else {
			url += "?format=json"
		}
	}
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GET %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "GET %s: %s\n", url, resp.Status)
		return 1
	}
	_, _ = io.Copy(os.Stdout, resp.Body)
	return 0
}

func schemaDiffCmd(args []string) int {
	flags, _ := splitFlagsAndPositionals(args)
	fs := flag.NewFlagSet("schema diff", flag.ContinueOnError)
	from := fs.String("from", "", "Old schema source (URL or file path; required)")
	to := fs.String("to", "", "New schema source (URL or file path; required)")
	strict := fs.Bool("strict", false, "Exit non-zero if any breaking changes are reported")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: gwag schema diff --from OLD --to NEW [--strict]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(flags); err != nil {
		return 2
	}
	if *from == "" || *to == "" {
		fs.Usage()
		return 2
	}
	oldSDL, err := loadSchemaSource(*from)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load --from %s: %v\n", *from, err)
		return 1
	}
	newSDL, err := loadSchemaSource(*to)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load --to %s: %v\n", *to, err)
		return 1
	}
	oldM, err := parseSchemaModel(oldSDL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse --from: %v\n", err)
		return 1
	}
	newM, err := parseSchemaModel(newSDL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse --to: %v\n", err)
		return 1
	}
	changes := diffModels(oldM, newM)
	breaking := 0
	for _, c := range changes {
		fmt.Printf("[%s] %s\n", strings.ToUpper(c.severity), c.msg)
		if c.severity == "breaking" {
			breaking++
		}
	}
	if len(changes) == 0 {
		fmt.Println("(no differences)")
	} else {
		fmt.Printf("\n%d change(s); %d breaking\n", len(changes), breaking)
	}
	if *strict && breaking > 0 {
		return 1
	}
	return 0
}

// loadSchemaSource fetches an SDL from either an HTTP(S) URL or a
// local file path. URLs are detected by the http:// or https:// prefix.
func loadSchemaSource(src string) (string, error) {
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		resp, err := http.Get(src)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("status %s", resp.Status)
		}
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}
	b, err := os.ReadFile(src)
	return string(b), err
}

// signCmd dispatches to the gateway's SignSubscriptionToken RPC. Two
// modes: --gateway HOST:PORT goes over the wire (now bearer-gated,
// see plan §2.1 — signer-secret OR admin token); --secret HEX signs
// locally (pure crypto, no gateway involvement).
func signCmd(args []string) int {
	flags, _ := splitFlagsAndPositionals(args)
	fs := flag.NewFlagSet("sign", flag.ContinueOnError)
	gwAddr := fs.String("gateway", "", "Gateway control-plane address (e.g. localhost:50090); empty = sign locally with --secret. Defaults from .gw primary if set there.")
	ctxName := fs.String("context", "", "Login name in .gw/credentials.json (default: primary)")
	channel := fs.String("channel", "", "Resolved subject the token will sign")
	ttl := fs.Int64("ttl", 60, "Token TTL in seconds (informational)")
	secretHex := fs.String("secret", "", "Hex-encoded shared secret (local sign mode)")
	kid := fs.String("kid", "", "Optional rotation key id; empty = legacy default secret")
	bearerHex := fs.String("bearer", "", "Hex-encoded bearer for the gRPC sign endpoint (signer-secret or admin token); required with --gateway. Defaults from .gw primary if set there.")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: gwag sign --channel SUBJECT [--ttl 60] [--kid KID]")
		fmt.Fprintln(fs.Output(), "  Remote: --gateway HOST:PORT --bearer HEX")
		fmt.Fprintln(fs.Output(), "  Local:  --secret HEX")
		fs.PrintDefaults()
	}
	if err := fs.Parse(flags); err != nil {
		return 2
	}
	if *channel == "" {
		fs.Usage()
		return 2
	}
	login := resolveLogin(*ctxName)
	addr := resolveCtx(*gwAddr, login.Gateway, "")
	bearer := resolveCtx(*bearerHex, login.Bearer, "")
	if addr != "" {
		if bearer == "" {
			fmt.Fprintln(os.Stderr, "--bearer is required with --gateway (the sign endpoint is bearer-gated; pass it on the command line or set bearer in .gw via 'gwag login --token HEX')")
			return 2
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer conn.Close()
		client := cpv1.NewControlPlaneClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer)
		resp, err := client.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
			Channel:    *channel,
			TtlSeconds: *ttl,
			Kid:        *kid,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "SignSubscriptionToken: %v\n", err)
			return 1
		}
		if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
			fmt.Fprintf(os.Stderr, "code=%s reason=%s\n", resp.GetCode(), resp.GetReason())
			return 1
		}
		fmt.Printf("hmac=%s\nts=%d\nkid=%s\n", resp.GetHmac(), resp.GetTimestampUnix(), resp.GetKid())
		return 0
	}
	if *secretHex == "" {
		fmt.Fprintln(os.Stderr, "either --gateway or --secret is required")
		return 2
	}
	secret, err := hex.DecodeString(*secretHex)
	if err != nil {
		fmt.Fprintln(os.Stderr, "decode --secret:", err)
		return 1
	}
	mac, kidOut, ts := gateway.SignSubscribeTokenWithKid(secret, *kid, *channel, *ttl)
	fmt.Printf("hmac=%s\nts=%d\nkid=%s\n", mac, ts, kidOut)
	return 0
}

// splitFlagsAndPositionals walks a free-form argv tail and returns
// (flags, positionals) so the flags can be parsed even when they're
// interleaved with positional args. Recognises --flag, --flag=val,
// -f, -f=val, and -f val (the last requires foreknowledge that the
// flag takes a value, which we approximate by always consuming the
// next token unless it starts with '-').
func splitFlagsAndPositionals(args []string) (flags, positionals []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" || a == "--" {
			positionals = append(positionals, a)
			continue
		}
		flags = append(flags, a)
		if strings.Contains(a, "=") {
			continue
		}
		// flag in --foo / -foo form; take next token as its value if
		// it doesn't itself look like a flag.
		if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			flags = append(flags, args[i+1])
			i++
		}
	}
	return flags, positionals
}

func peerCmd(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: gwag peer (list|forget NODE_ID) [--gateway HOST:PORT]")
		return 2
	}
	verb := args[0]
	rest := args[1:]
	// Reorder so all flags precede positionals — Go's flag.Parse stops
	// at the first non-flag, which makes `forget NODE_ID --gateway X`
	// otherwise drop the flag.
	flags, positionals := splitFlagsAndPositionals(rest)
	fs := flag.NewFlagSet("peer "+verb, flag.ContinueOnError)
	gwAddr := fs.String("gateway", "", "Gateway control-plane address (default: from .gw primary or localhost:50090)")
	ctxName := fs.String("context", "", "Login name in .gw/credentials.json (default: primary)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: gwag peer "+verb+" [--gateway HOST:PORT] [--context NAME] [args...]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(flags); err != nil {
		return 2
	}
	rest = positionals
	login := resolveLogin(*ctxName)
	addr := resolveCtx(*gwAddr, login.Gateway, "localhost:50090")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		return 1
	}
	defer conn.Close()
	client := cpv1.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch verb {
	case "list":
		resp, err := client.ListPeers(ctx, &cpv1.ListPeersRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "ListPeers: %v\n", err)
			return 1
		}
		if len(resp.GetPeers()) == 0 {
			fmt.Println("(no peers)")
			return 0
		}
		fmt.Printf("%-58s %-10s %s\n", "NODE_ID", "NAME", "JOINED")
		for _, p := range resp.GetPeers() {
			joined := time.UnixMilli(p.GetJoinedUnixMs()).Format(time.RFC3339)
			fmt.Printf("%-58s %-10s %s\n", p.GetNodeId(), p.GetName(), joined)
		}
		return 0

	case "forget":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "usage: gwag peer forget NODE_ID [--gateway HOST:PORT]")
			return 2
		}
		resp, err := client.ForgetPeer(ctx, &cpv1.ForgetPeerRequest{NodeId: rest[0]})
		if err != nil {
			fmt.Fprintf(os.Stderr, "ForgetPeer: %v\n", err)
			return 1
		}
		fmt.Printf("removed=%v new_replicas=%d\n", resp.GetRemoved(), resp.GetNewReplicas())
		return 0

	default:
		fs.Usage()
		return 2
	}
}
