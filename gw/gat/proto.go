package gat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// ProtoFile compiles the .proto file at path, dials target as the
// upstream gRPC server, and returns one ServiceRegistration per
// service declared in the file. Pass the result to gat.New(...).
//
// Imports resolve from the directory containing path; well-known
// google/protobuf/* imports resolve automatically. Comments survive
// into the rendered GraphQL SDL via SourceInfoStandard, matching the
// full gwag ingest path.
//
// The dialled connection uses insecure credentials (no TLS). For
// mTLS or other transport credentials, dial yourself and register
// via ServiceRegistration.Dispatchers with custom dispatchers — see
// the gat.New godoc for the BYO-IR pattern.
//
// One ProtoFile call = one gRPC connection. The connection lives as
// long as the gat.Gateway it backs; gat does not currently expose a
// Close() hook (the connection is closed when the process exits).
//
// Stability: experimental
func ProtoFile(path string, target string) ([]ServiceRegistration, error) {
	fd, err := compileProtoFile(path)
	if err != nil {
		return nil, err
	}
	return registrationsForProto(fd, target)
}

// ProtoSource compiles raw .proto entrypoint bytes (typed under entry
// for error messages) with an imports map for any referenced files,
// dials target, and returns ServiceRegistrations. Suitable for
// embedded specs (//go:embed) where ProtoFile can't reach the disk.
//
// imports is keyed by the same path string used in the .proto's
// `import "..."` declarations. Well-known google/protobuf/* imports
// resolve automatically.
//
// Stability: experimental
func ProtoSource(entry string, body []byte, imports map[string][]byte, target string) ([]ServiceRegistration, error) {
	fd, err := compileProtoSource(entry, body, imports)
	if err != nil {
		return nil, err
	}
	return registrationsForProto(fd, target)
}

// registrationsForProto turns a compiled FileDescriptor into
// ServiceRegistration values with proto dispatchers wired to a single
// shared *grpc.ClientConn. Caller passes the result to gat.New.
func registrationsForProto(fd protoreflect.FileDescriptor, target string) ([]ServiceRegistration, error) {
	services := ir.IngestProto(fd)
	if len(services) == 0 {
		return nil, fmt.Errorf("gat: %s declares no services", fd.Path())
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("gat: dial %s: %w", target, err)
	}

	defaultNS, defaultVer := splitProtoPackage(string(fd.Package()))
	regs := make([]ServiceRegistration, 0, len(services))
	for _, svc := range services {
		if svc.Namespace == "" {
			svc.Namespace = defaultNS
		}
		if svc.Version == "" {
			svc.Version = defaultVer
		}
		ir.PopulateSchemaIDs(svc)

		sd := fd.Services().ByName(protoreflect.Name(svc.ServiceName))
		if sd == nil {
			return nil, fmt.Errorf("gat: service %q not found in compiled descriptor", svc.ServiceName)
		}

		dispatchers := map[ir.SchemaID]ir.Dispatcher{}
		for _, op := range svc.FlatOperations() {
			md := sd.Methods().ByName(protoreflect.Name(op.Name))
			if md == nil {
				continue
			}
			dispatchers[op.SchemaID] = &protoDispatcher{
				conn:       conn,
				fullMethod: fmt.Sprintf("/%s/%s", sd.FullName(), md.Name()),
				inputDesc:  md.Input(),
				outputDesc: md.Output(),
			}
		}
		regs = append(regs, ServiceRegistration{
			Service:     svc,
			Dispatchers: dispatchers,
		})
	}
	return regs, nil
}

// protoDispatcher implements ir.Dispatcher for one proto unary
// method. It owns the dynamicpb marshal / unmarshal bridge and runs
// grpc.ClientConn.Invoke per dispatch — no pool, no replicas, no
// backpressure (gat is the embedded, simple-start variant).
type protoDispatcher struct {
	conn       *grpc.ClientConn
	fullMethod string
	inputDesc  protoreflect.MessageDescriptor
	outputDesc protoreflect.MessageDescriptor
}

// Dispatch satisfies ir.Dispatcher.
func (d *protoDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	req := dynamicpb.NewMessage(d.inputDesc)
	if err := protoArgsToMessage(args, req); err != nil {
		return nil, fmt.Errorf("gat: encode %s: %w", d.fullMethod, err)
	}
	resp := dynamicpb.NewMessage(d.outputDesc)
	if err := d.conn.Invoke(ctx, d.fullMethod, req, resp); err != nil {
		return nil, fmt.Errorf("gat: invoke %s: %w", d.fullMethod, err)
	}
	return protoMessageToMap(resp), nil
}

// compileProtoFile compiles a .proto from disk with imports anchored
// at the file's directory. SourceInfoStandard preserves comments.
func compileProtoFile(path string) (protoreflect.FileDescriptor, error) {
	dir, file := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{dir},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := c.Compile(context.Background(), file)
	if err != nil {
		return nil, fmt.Errorf("gat: compile %s: %w", path, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("gat: compile %s: empty result", path)
	}
	return files[0], nil
}

// compileProtoSource compiles raw .proto bytes with an in-memory
// imports map. Mirrors the full gwag bytes-ingest path.
func compileProtoSource(entry string, body []byte, imports map[string][]byte) (protoreflect.FileDescriptor, error) {
	if entry == "" {
		entry = "entry.proto"
	}
	files := map[string][]byte{entry: body}
	maps.Copy(files, imports)
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(p string) (io.ReadCloser, error) {
				if b, ok := files[p]; ok {
					return io.NopCloser(bytes.NewReader(b)), nil
				}
				return nil, fmt.Errorf("proto import not found: %s", p)
			},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	out, err := c.Compile(context.Background(), entry)
	if err != nil {
		return nil, fmt.Errorf("gat: compile %s: %w", entry, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("gat: compile %s: empty result", entry)
	}
	return out[0], nil
}

// Compile-time assertion: protoDispatcher implements ir.Dispatcher.
var _ ir.Dispatcher = (*protoDispatcher)(nil)

// protoPackageVersionRE matches a trailing "vN" segment so a proto
// package like "greeter.v1" splits into namespace "greeter" + version
// "v1".
var protoPackageVersionRE = regexp.MustCompile(`^v\d+$`)

// splitProtoPackage derives a GraphQL-safe (namespace, version) pair
// from a proto package string. GraphQL forbids dots in type / field
// names, so the proto's dotted package can't be used verbatim.
//
// Rules:
//   - "" → ("default", "v1")
//   - "greeter.v1" → ("greeter", "v1")     (trailing vN is the version)
//   - "greeter" → ("greeter", "v1")
//   - "a.b.c.v2" → ("a_b_c", "v2")
//   - "a.b.c" → ("a_b_c", "v1")
//
// Adopters who need a different mapping override Service.Namespace /
// Service.Version on the returned ServiceRegistration before passing
// to gat.New.
func splitProtoPackage(pkg string) (namespace, version string) {
	if pkg == "" {
		return "default", "v1"
	}
	parts := strings.Split(pkg, ".")
	if len(parts) > 1 && protoPackageVersionRE.MatchString(parts[len(parts)-1]) {
		version = parts[len(parts)-1]
		parts = parts[:len(parts)-1]
	} else {
		version = "v1"
	}
	return strings.Join(parts, "_"), version
}
