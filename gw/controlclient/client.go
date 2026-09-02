// Package controlclient is the convenience layer subservices use to
// self-register with a gwag control plane: one Register call,
// a background heartbeat goroutine, and graceful Deregister on Close.
//
//	//go:embed greeter.proto
//	var greeterProto []byte
//
//	reg, err := controlclient.SelfRegister(ctx, controlclient.Options{
//	    GatewayAddr: "gateway:50090",
//	    ServiceAddr: "greeter:50051",
//	    Services: []controlclient.Service{
//	        {Namespace: "greeter", ProtoSource: greeterProto},
//	    },
//	})
//	if err != nil { log.Fatal(err) }
//	defer reg.Close(context.Background())
//
// Holds an open *grpc.ClientConn to the gateway for the lifetime of
// the registration. Re-Register is automatic on heartbeat-not-ok
// (gateway evicted us, e.g. after a restart) — the goroutine fires a
// fresh Register and resumes.
package controlclient

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// Service describes one service binding to register with the gateway.
// At least one of ProtoSource/ProtoFS, OpenAPISpec, or GraphQLEndpoint
// must be set. Multiple Services can be passed in a single SelfRegister
// call to bind multiple namespaces in one go.
//
// Stability: stable
type Service struct {
	// Namespace under which to mount this service in the gateway's
	// GraphQL surface. Empty falls back to the proto's filename stem
	// (proto bindings) or the OpenAPI spec's Info.Title (OpenAPI
	// bindings).
	Namespace string

	// Version, e.g. "v1", "v2". Multiple versions of the same namespace
	// coexist on the gateway; latest surfaces flat under the namespace,
	// older versions appear as `vN` sub-objects with @deprecated.
	// Empty defaults to "v1". Ignored for OpenAPI bindings (single
	// version per ns in v1).
	Version string

	// ProtoSource is the raw bytes of the entrypoint .proto file —
	// exactly what the operator wrote on disk. The receiving gateway
	// compiles via protocompile (SourceInfoStandard) so leading /
	// trailing comments survive into the GraphQL SDL and MCP search
	// corpus. Most callers `//go:embed greeter.proto` and pass the
	// embedded bytes here.
	//
	// Mutually exclusive with ProtoFS, OpenAPISpec, GraphQLEndpoint.
	ProtoSource []byte

	// ProtoImports passes transitive .proto imports keyed by their
	// import path (e.g. "auth.proto"). Required only when ProtoSource
	// has `import "..."` statements; well-known imports
	// (google/protobuf/*) resolve automatically. Single-file .protos
	// leave this nil.
	ProtoImports map[string][]byte

	// ProtoFS is the multi-file ergonomic shape: pass any fs.FS
	// (embed.FS, os.DirFS(...), tar/zip wrappers, anything that
	// satisfies the interface) and the controlclient walks it to
	// build ProtoSource (= bytes of ProtoEntry) and ProtoImports
	// (= every other .proto under the FS, keyed by relative path).
	// Use this when the service has many .proto files; use
	// ProtoSource directly for single-file services.
	//
	// Mutually exclusive with ProtoSource / ProtoImports.
	ProtoFS fs.FS

	// ProtoEntry is the entrypoint filename within ProtoFS (e.g.
	// "user.proto"). Required when ProtoFS is set.
	ProtoEntry string

	// OpenAPISpec is the raw bytes of an OpenAPI 3.x document (JSON
	// or YAML; kin-openapi parses either). When set, ServiceAddr in
	// the parent Options is the HTTP base URL the gateway dispatches
	// to (e.g. "https://billing.internal").
	OpenAPISpec []byte

	// GraphQLEndpoint is the URL of an upstream GraphQL service to
	// ingest under this Namespace. Mutually exclusive with the proto
	// source fields and OpenAPISpec. The receiving gateway runs the
	// canonical introspection query against this endpoint at Register
	// time and forwards dispatches back to it. ServiceAddr is ignored
	// for GraphQL bindings — the endpoint URL carries both the
	// schema source and the dispatch destination.
	GraphQLEndpoint string

	// MaxConcurrency caps simultaneous unary dispatches against this
	// (namespace, version) on the receiving gateway. Overrides the
	// gateway-wide BackpressureOptions.MaxInflight default for this
	// one binding. 0 → fall back to the gateway default.
	MaxConcurrency uint32

	// MaxConcurrencyPerInstance caps simultaneous unary dispatches
	// against any single replica behind this binding. New axis: the
	// service-level cap above bounds the pool, this bounds each
	// replica individually. 0 → unbounded per replica (only the
	// service-level cap applies).
	MaxConcurrencyPerInstance uint32
}

// Options configures a SelfRegister call. GatewayAddr, ServiceAddr,
// and at least one Service entry are required.
//
// Stability: stable
type Options struct {
	// GatewayAddr is the gRPC host:port where the gateway's control
	// plane is listening (the WithControlPlane addr).
	GatewayAddr string

	// ServiceAddr is the host:port where this service's gRPC server
	// is reachable. Passed to the gateway verbatim.
	ServiceAddr string

	// Services to register. All bind to ServiceAddr. Empty rejected.
	Services []Service

	// InstanceID is echoed in ListRegistrations for operator debugging.
	// Free-form ("greeter@pod-abc"). Optional.
	InstanceID string

	// TTL is how long the gateway should retain the registration
	// without seeing a heartbeat. 0 → server default (30s).
	TTL time.Duration

	// HeartbeatInterval defaults to TTL/3 (or 10s if TTL is 0).
	HeartbeatInterval time.Duration

	// DialOptions are appended to the gateway-conn dial. Insecure is
	// the default if none are supplied.
	DialOptions []grpc.DialOption

	// Logger is called with non-fatal events (eviction recovery,
	// heartbeat errors). Defaults to log.Printf-equivalent.
	Logger func(format string, args ...any)

	// BuildTag, when non-empty, marks the calling binary as a release
	// build. SelfRegister refuses any Service whose Version is
	// "unstable" — release artifacts belong to numbered cuts (vN), not
	// trunk's mutable slot. Plan §4 forcing function: a redeployed
	// v3-era pod can't accidentally overwrite `unstable` because its
	// release binary still carries v3's tag.
	//
	// Recommended pattern: stamp via -ldflags "-X 'main.buildTag=v1.2.3'"
	// (or whatever the project's release machinery emits) and pass it
	// through to controlclient.Options. Trunk CI omits it; release CI
	// sets it. Empty defeats the lint, so don't paper over a CI bug
	// by clearing the field — unset means "trunk", set means "release".
	BuildTag string
}

// Registration is the live handle returned by SelfRegister. Close
// stops the heartbeat goroutine and gracefully deregisters.
//
// Stability: stable
type Registration struct {
	conn   *grpc.ClientConn
	client cpv1.ControlPlaneClient

	mu     sync.Mutex
	id     string
	ttl    time.Duration
	stop   chan struct{}
	done   chan struct{}
	logger func(string, ...any)
	opts   Options
}

// SelfRegister registers with the gateway control plane and starts a
// background heartbeat goroutine. Returns a Registration; call
// Close when the service shuts down to deregister and stop the goroutine.
// Blocks until the initial Register RPC succeeds or ctx is cancelled.
//
// Stability: stable
func SelfRegister(ctx context.Context, opts Options) (*Registration, error) {
	if opts.GatewayAddr == "" {
		return nil, errors.New("controlclient: GatewayAddr required")
	}
	if opts.ServiceAddr == "" {
		return nil, errors.New("controlclient: ServiceAddr required")
	}
	if len(opts.Services) == 0 {
		return nil, errors.New("controlclient: at least one Service required")
	}
	if opts.BuildTag != "" {
		for _, s := range opts.Services {
			if s.Version == "unstable" {
				return nil, fmt.Errorf("controlclient: BuildTag=%q set; refusing to register %s/unstable (release builds must claim a numbered vN — see plan §4)", opts.BuildTag, s.Namespace)
			}
		}
	}
	if opts.Logger == nil {
		opts.Logger = log.Printf
	}

	dialOpts := opts.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(opts.GatewayAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial gateway %s: %w", opts.GatewayAddr, err)
	}

	r := &Registration{
		conn:   conn,
		client: cpv1.NewControlPlaneClient(conn),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		logger: opts.Logger,
		opts:   opts,
	}

	if err := r.register(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	hi := opts.HeartbeatInterval
	if hi == 0 {
		if r.ttl > 0 {
			hi = r.ttl / 3
		} else {
			hi = 10 * time.Second
		}
	}
	go r.heartbeatLoop(hi)

	return r, nil
}

func (r *Registration) register(ctx context.Context) error {
	req := &cpv1.RegisterRequest{
		Addr:       r.opts.ServiceAddr,
		InstanceId: r.opts.InstanceID,
		TtlSeconds: uint32(r.opts.TTL / time.Second),
	}
	for _, s := range r.opts.Services {
		protoSource := s.ProtoSource
		protoImports := s.ProtoImports
		if s.ProtoFS != nil {
			if s.ProtoEntry == "" {
				return fmt.Errorf("service %q: ProtoFS set without ProtoEntry", s.Namespace)
			}
			if len(s.ProtoSource) > 0 || len(s.ProtoImports) > 0 {
				return fmt.Errorf("service %q: ProtoFS is mutually exclusive with ProtoSource / ProtoImports", s.Namespace)
			}
			src, imports, err := resolveProtoFS(s.ProtoFS, s.ProtoEntry)
			if err != nil {
				return fmt.Errorf("service %q: ProtoFS: %w", s.Namespace, err)
			}
			protoSource = src
			protoImports = imports
		}

		hasProto := len(protoSource) > 0
		hasOpenAPI := len(s.OpenAPISpec) > 0
		hasGraphQL := s.GraphQLEndpoint != ""
		set := 0
		if hasProto {
			set++
		}
		if hasOpenAPI {
			set++
		}
		if hasGraphQL {
			set++
		}
		if set > 1 {
			return fmt.Errorf("service %q: may set only one of ProtoSource (or ProtoFS), OpenAPISpec, GraphQLEndpoint", s.Namespace)
		}
		if set == 0 {
			return fmt.Errorf("service %q: must set ProtoSource (or ProtoFS), OpenAPISpec, or GraphQLEndpoint", s.Namespace)
		}
		if hasGraphQL {
			req.Services = append(req.Services, &cpv1.ServiceBinding{
				Namespace:                 s.Namespace,
				Version:                   s.Version,
				GraphqlEndpoint:           s.GraphQLEndpoint,
				MaxConcurrency:            s.MaxConcurrency,
				MaxConcurrencyPerInstance: s.MaxConcurrencyPerInstance,
			})
			continue
		}
		if hasOpenAPI {
			req.Services = append(req.Services, &cpv1.ServiceBinding{
				Namespace:                 s.Namespace,
				Version:                   s.Version,
				OpenapiSpec:               s.OpenAPISpec,
				MaxConcurrency:            s.MaxConcurrency,
				MaxConcurrencyPerInstance: s.MaxConcurrencyPerInstance,
			})
			continue
		}
		req.Services = append(req.Services, &cpv1.ServiceBinding{
			Namespace:                 s.Namespace,
			Version:                   s.Version,
			ProtoSource:               protoSource,
			ProtoImports:              protoImports,
			MaxConcurrency:            s.MaxConcurrency,
			MaxConcurrencyPerInstance: s.MaxConcurrencyPerInstance,
		})
	}

	resp, err := r.client.Register(ctx, req)
	if err != nil {
		return fmt.Errorf("Register: %w", err)
	}
	r.mu.Lock()
	r.id = resp.GetRegistrationId()
	r.ttl = time.Duration(resp.GetTtlSeconds()) * time.Second
	r.mu.Unlock()
	return nil
}

func (r *Registration) heartbeatLoop(interval time.Duration) {
	defer close(r.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.mu.Lock()
			id := r.id
			r.mu.Unlock()
			if id == "" {
				continue
			}
			resp, err := r.client.Heartbeat(context.Background(), &cpv1.HeartbeatRequest{RegistrationId: id})
			if err != nil {
				r.logger("controlclient: heartbeat error: %v", err)
				continue
			}
			if !resp.GetOk() {
				r.logger("controlclient: gateway evicted us, re-registering")
				if err := r.register(context.Background()); err != nil {
					r.logger("controlclient: re-register failed: %v", err)
				}
			}
		}
	}
}

// Close stops the heartbeat and gracefully deregisters. Safe to call
// once. ctx is used only for the Deregister call.
//
// Stability: stable
func (r *Registration) Close(ctx context.Context) error {
	select {
	case <-r.stop:
		return nil
	default:
	}
	close(r.stop)
	<-r.done

	r.mu.Lock()
	id := r.id
	r.mu.Unlock()
	if id != "" {
		_, _ = r.client.Deregister(ctx, &cpv1.DeregisterRequest{RegistrationId: id})
	}
	return r.conn.Close()
}

// resolveProtoFS walks fsys, returns the bytes of `entry` as the
// proto_source plus a map of every other .proto under the FS as
// proto_imports (keyed by their path within fsys). Used by
// Service.ProtoFS to flatten a multi-file .proto layout into the
// (source, imports-map) shape the wire carries.
//
// Filenames in the imports map use the same path the proto's
// `import "..."` statements use — protocompile resolves both
// against the same string. This means imports must be referenced
// the same way they're keyed in fsys (typically just the basename,
// e.g. "auth.proto", since `import "auth.proto";` is the convention
// for co-located files). Caller responsibility to align fsys layout
// with the import paths in the entrypoint .proto.
func resolveProtoFS(fsys fs.FS, entry string) ([]byte, map[string][]byte, error) {
	src, err := fs.ReadFile(fsys, entry)
	if err != nil {
		return nil, nil, fmt.Errorf("read entry %q: %w", entry, err)
	}
	imports := map[string][]byte{}
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".proto") {
			return nil
		}
		// Strip leading "./" if present and normalise; entry may be
		// either basename ("greeter.proto") or path ("foo/x.proto") —
		// match the WalkDir-emitted shape.
		key := path.Clean(p)
		if key == path.Clean(entry) {
			return nil
		}
		body, err := fs.ReadFile(fsys, p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		imports[key] = body
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(imports) == 0 {
		imports = nil
	}
	return src, imports, nil
}
