package gateway

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/iodesystems/gwag/v2/gw/ir"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// SchemaProtoHandler returns an http.Handler for GET /schema/proto.
// It emits a FileDescriptorSet for selected services with the
// gateway's transformations applied — hidden fields stripped,
// internal namespaces skipped. NOT the raw registered protos:
// clients that codegen from this output get the contract surface as
// the gateway actually exposes it.
//
// Query parameters:
//   - service=ns[:ver][,ns[:ver]...]  — selector, comma separated.
//     Missing version → all versions
//     of that namespace. Missing
//     service param → all services.
//
// Response: application/protobuf, the marshalled FileDescriptorSet.
//
// Caveats:
//   - Hidden fields are stripped from message descriptors. Field
//     numbers of remaining fields are preserved (wire-compatible with
//     non-hidden fields), but the resulting messages cannot be
//     wire-compatibly produced by callers using the *original* proto.
//   - Cross-file imports are emitted only for files that themselves
//     have at least one selected/non-internal service. If a hidden
//     namespace was the source of a referenced type, the reference
//     remains in the descriptor — callers may see dangling type
//     references. (Acceptable for v1; refine when concrete cases
//     appear.)
//
// Stability: stable
func (g *Gateway) SchemaProtoHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		selectors, err := parseProtoSelectors(r.URL.Query().Get("service"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Pull every source through IR — proto, OpenAPI, GraphQL —
		// then render each to a FileDescriptorProto. Same-kind
		// (proto-origin) services emit verbatim via the Origin
		// shortcut; cross-kind services (OpenAPI/GraphQL) emit
		// synthesized FileDescriptorProtos so /schema/proto's
		// filter-by-namespace still has content even when the
		// matching service was never registered as proto.
		svcs, err := g.gatewayServicesAsIR(irSelectorsFromSchema(selectors))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fds, err := ir.RenderProtoFiles(svcs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Same-kind shortcut returns the registered descriptor —
		// fold in transitive imports + apply Hides via the existing
		// helpers so wire compatibility tracks the gateway's old
		// path. (For cross-kind synthesized files there are no
		// imports yet; v1 doesn't synthesize external well-known-
		// type imports.)
		g.mu.Lock()
		hides := map[protoreflect.FullName]bool{}
		for _, name := range g.hiddenTypeNames() {
			hides[protoreflect.FullName(name)] = true
		}
		g.mu.Unlock()
		if len(hides) > 0 {
			for _, fp := range fds.File {
				for _, m := range fp.MessageType {
					stripHiddenFields(m, hides)
				}
			}
		}
		// Stable ordering for deterministic bytes.
		sort.Slice(fds.File, func(i, j int) bool {
			return fds.File[i].GetName() < fds.File[j].GetName()
		})

		switch r.URL.Query().Get("format") {
		case "sdl":
			emitProtoSDL(w, fds)
		default:
			out, err := proto.MarshalOptions{Deterministic: true}.Marshal(fds)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/protobuf")
			w.Header().Set("Content-Disposition", `attachment; filename="services.fds"`)
			_, _ = w.Write(out)
		}
	})
}

// emitProtoSDL renders each FileDescriptorProto in `fds` as `.proto`
// SDL text via jhump/protoreflect's protoprint package, packs them
// as a JSON array of {name, sdl} entries, and writes the result.
//
// One JSON envelope keeps the response self-describing for browser
// consumers (the /schema viewer can render per-file blocks without
// hand-parsing a custom delimiter format), and the array preserves
// the deterministic file ordering above.
func emitProtoSDL(w http.ResponseWriter, fds *descriptorpb.FileDescriptorSet) {
	type fileSDL struct {
		Name string `json:"name"`
		SDL  string `json:"sdl"`
	}

	// CreateFileDescriptorsFromSet honours the import graph: every
	// dependency must be in the set so transitive lookups resolve.
	// collectTransformedFiles already includes imports, so we're
	// guaranteed a closed set here.
	descs, err := desc.CreateFileDescriptorsFromSet(fds)
	if err != nil {
		http.Error(w, fmt.Sprintf("desc: %v", err), http.StatusInternalServerError)
		return
	}
	printer := &protoprint.Printer{
		Compact:                  false,
		SortElements:             true,
		ForceFullyQualifiedNames: false,
	}
	out := make([]fileSDL, 0, len(fds.File))
	for _, fp := range fds.File {
		fd, ok := descs[fp.GetName()]
		if !ok {
			continue
		}
		var sb strings.Builder
		if err := printer.PrintProtoFile(fd, &sb); err != nil {
			http.Error(w, fmt.Sprintf("print %s: %v", fp.GetName(), err), http.StatusInternalServerError)
			return
		}
		out = append(out, fileSDL{Name: fp.GetName(), SDL: sb.String()})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := ir.WriteJSON(w, out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// serviceSelector picks a (namespace, optional version) for export.
type serviceSelector struct {
	namespace string
	version   string // "" = any version
}

// schemaFilter wraps a parsed selector list and exposes the per-source
// match shapes the schema-build helpers need. An empty selector list
// matches everything (no filter active). Used by /schema/graphql to
// build a per-request filtered schema; the cached g.schema is built
// with an empty filter (= everything).
type schemaFilter struct {
	selectors []serviceSelector
}

func (f schemaFilter) matchPool(k poolKey) bool { return matchSelectors(k, f.selectors) }

func parseProtoSelectors(raw string) ([]serviceSelector, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]serviceSelector, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		ns, ver, _ := strings.Cut(p, ":")
		ns = strings.TrimSpace(ns)
		ver = strings.TrimSpace(ver)
		if err := validateNS(ns); err != nil {
			return nil, fmt.Errorf("service selector %q: %w", p, err)
		}
		if ver != "" {
			if _, _, err := parseVersion(ver); err != nil {
				return nil, fmt.Errorf("service selector %q: %w", p, err)
			}
		}
		out = append(out, serviceSelector{namespace: ns, version: ver})
	}
	return out, nil
}

func matchSelectors(k poolKey, sels []serviceSelector) bool {
	if len(sels) == 0 {
		return true
	}
	for _, s := range sels {
		if s.namespace != k.namespace {
			continue
		}
		if s.version == "" || s.version == k.version {
			return true
		}
	}
	return false
}

// collectTransformedFiles walks fd plus its transitive imports and
// adds (transformed) FileDescriptorProtos into fds, deduped by path.
func collectTransformedFiles(
	fd protoreflect.FileDescriptor,
	hides map[protoreflect.FullName]bool,
	fds *descriptorpb.FileDescriptorSet,
	seen map[string]bool,
) {
	if seen[string(fd.Path())] {
		return
	}
	seen[string(fd.Path())] = true
	for i := 0; i < fd.Imports().Len(); i++ {
		collectTransformedFiles(fd.Imports().Get(i).FileDescriptor, hides, fds, seen)
	}
	fds.File = append(fds.File, transformFileDescriptor(fd, hides))
}

// transformFileDescriptor returns a FileDescriptorProto with hidden
// field types stripped from every message. Field numbers are
// preserved on the remaining fields. Method definitions are kept
// intact (the gateway's GraphQL filtering applies only at the
// GraphQL surface — direct gRPC callers still see them).
func transformFileDescriptor(
	fd protoreflect.FileDescriptor,
	hides map[protoreflect.FullName]bool,
) *descriptorpb.FileDescriptorProto {
	src := protodesc.ToFileDescriptorProto(fd)
	if len(hides) == 0 {
		return src
	}
	for _, m := range src.MessageType {
		stripHiddenFields(m, hides)
	}
	return src
}

// SchemaOpenAPIHandler returns an http.Handler for GET /schema/openapi.
// It re-emits each ingested OpenAPI spec keyed by namespace + version,
// optionally filtered by the selector.
//
// Query parameters:
//   - service=ns[:ver][,...]  — comma-separated namespaces, each
//     optionally pinned to a version. Empty
//     selector → all.
//
// Response: application/json, an object
// `{"<ns>": {"<vN>": <spec>, ...}, ...}`. Single-version namespaces
// surface as `{"<ns>": {"v1": <spec>}}`.
//
// Stability: stable
func (g *Gateway) SchemaOpenAPIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		selectors, err := parseProtoSelectors(r.URL.Query().Get("service"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		filter := schemaFilter{selectors: selectors}

		// Build per-source IR + render OpenAPI per service. The IR
		// pipeline picks up proto pools too — a proto-only namespace
		// surfaces as `{<ns>: {<vN>: <synth-openapi-spec>}}` so the
		// /schema/openapi tab in the UI is no longer empty when the
		// filter narrows to a proto service.
		_ = filter
		svcs, err := g.gatewayServicesAsIR(irSelectorsFromSchema(selectors))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := map[string]map[string]any{}
		for _, svc := range svcs {
			doc, err := ir.RenderOpenAPI(svc)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			byVer, ok := out[svc.Namespace]
			if !ok {
				byVer = map[string]any{}
				out[svc.Namespace] = byVer
			}
			byVer[svc.Version] = doc
		}

		w.Header().Set("Content-Type", "application/json")
		if err := ir.WriteJSON(w, out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// stripHiddenFields removes fields whose type_name resolves to a
// hidden FullName. Recurses into nested types.
func stripHiddenFields(
	m *descriptorpb.DescriptorProto,
	hides map[protoreflect.FullName]bool,
) {
	out := m.Field[:0]
	for _, f := range m.Field {
		if f.TypeName != nil {
			tn := strings.TrimPrefix(*f.TypeName, ".")
			if hides[protoreflect.FullName(tn)] {
				continue
			}
		}
		out = append(out, f)
	}
	// Re-slice to drop the trailing now-unused entries.
	for i := len(out); i < len(m.Field); i++ {
		m.Field[i] = nil
	}
	m.Field = out
	for _, nested := range m.NestedType {
		stripHiddenFields(nested, hides)
	}
}
