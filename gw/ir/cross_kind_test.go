package ir

import (
	"strings"
	"testing"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// TestProtoIngest_RenderOpenAPI is the headline cross-kind test
// that motivated the IR refactor: feed the gateway's greeter proto
// through ingest, then render OpenAPI. The resulting spec carries
// one path per non-streaming RPC + a Pet-shaped components section
// for the request/response messages.
func TestProtoIngest_RenderOpenAPI(t *testing.T) {
	svcs := IngestProto(greeterv1.File_greeter_proto)
	svc := svcs[0]
	svc.Namespace = "greeter"
	svc.Version = "v1"

	doc, err := RenderOpenAPI(svc)
	if err != nil {
		t.Fatalf("RenderOpenAPI: %v", err)
	}
	if doc.Paths == nil {
		t.Fatal("rendered doc has no paths")
	}

	// Hello is the only non-streaming RPC; expect exactly one POST
	// operation under /greeter.v1.GreeterService/Hello (OpenAPI
	// renderer's synthetic path for proto-origin operations).
	pi := doc.Paths.Value("/greeter.v1.GreeterService/Hello")
	if pi == nil {
		keys := []string{}
		for k := range doc.Paths.Map() {
			keys = append(keys, k)
		}
		t.Fatalf("missing POST /greeter.v1.GreeterService/Hello; have %v", keys)
	}
	if pi.Post == nil || pi.Post.OperationID != "Hello" {
		t.Errorf("Hello operation missing or wrong OperationID: %#v", pi.Post)
	}

	// Streaming methods (Greetings, Echo) should NOT show up as
	// OpenAPI paths — OpenAPI has no native streaming shape, and
	// our renderer drops Subscription/StreamingClient operations.
	if doc.Paths.Value("/greeter.v1.GreeterService/Greetings") != nil {
		t.Error("Greetings (server-streaming) shouldn't surface as an OpenAPI path")
	}

	// HelloRequest + HelloResponse messages → components/schemas.
	for _, want := range []string{"greeter.v1.HelloRequest", "greeter.v1.HelloResponse"} {
		if _, ok := doc.Components.Schemas[want]; !ok {
			t.Errorf("components/schemas missing %q", want)
		}
	}

	// Proto unary args must surface as a JSON requestBody (not
	// parameters[in=query]) so codegen clients send the body the
	// gateway's HTTP ingress actually decodes — IngressHandler's
	// ingressShapeProtoPost path reads canonical args from the body
	// verbatim. Hello takes one arg "name"; expect it inside the
	// body schema's properties, with no query parameters.
	if pi.Post.RequestBody == nil || pi.Post.RequestBody.Value == nil {
		t.Fatalf("Hello: missing requestBody — proto unary args must render as body")
	}
	mt := pi.Post.RequestBody.Value.Content["application/json"]
	if mt == nil || mt.Schema == nil || mt.Schema.Value == nil {
		t.Fatalf("Hello: requestBody has no application/json schema")
	}
	if _, ok := mt.Schema.Value.Properties["name"]; !ok {
		t.Errorf("Hello: requestBody schema missing 'name' property; got %v", mt.Schema.Value.Properties)
	}
	for _, p := range pi.Post.Parameters {
		if p != nil && p.Value != nil && p.Value.In == "query" {
			t.Errorf("Hello: unexpected query parameter %q (proto unary should put args in body)", p.Value.Name)
		}
	}
}

// TestOpenAPIIngest_RenderProto is the inverse: an OpenAPI spec
// distilled into IR re-renders to a proto FileDescriptorSet. The
// canonical path doesn't synthesise full input/output messages
// from non-proto origins yet — we just confirm the file exists
// with one Service and one method per OpenAPI operation.
func TestOpenAPIIngest_RenderProto(t *testing.T) {
	doc := loadOpenAPI(t)
	svc := IngestOpenAPI(doc)
	svc.Namespace = "petstore"
	svc.Version = "v1"
	svc.ServiceName = "PetstoreService"

	fds, err := RenderProtoFiles([]*Service{svc})
	if err != nil {
		t.Fatalf("RenderProtoFiles: %v", err)
	}
	if got := len(fds.File); got != 1 {
		t.Fatalf("FileDescriptorSet has %d files, want 1", got)
	}
	fp := fds.File[0]
	if fp.GetPackage() != "petstore.v1" {
		t.Errorf("package = %q, want petstore.v1", fp.GetPackage())
	}
	// Three operations from the test spec → three methods on the
	// synthesized service.
	if got := len(fp.GetService()); got != 1 {
		t.Fatalf("service count = %d, want 1", got)
	}
	if got := len(fp.GetService()[0].GetMethod()); got != 3 {
		t.Errorf("method count = %d, want 3", got)
	}
}

// TestProtoIngest_RenderGraphQL completes the matrix: proto in,
// GraphQL SDL out (the served runtime schema, via PrintSchemaSDL).
// Cross-kind so non-streaming RPCs surface as Query operations under
// the namespace fold and streaming ones flatten onto Subscription.
func TestProtoIngest_RenderGraphQL(t *testing.T) {
	svcs := IngestProto(greeterv1.File_greeter_proto)
	svcs[0].Namespace = "greeter"
	svcs[0].Version = "v1"
	sdl := runtimeSDL(t, svcs...)

	for _, want := range []string{
		"type Query",
		"greeter: GreeterQueryNamespace!",
		"hello(name: String)",
		"type Subscription",
		"greeter_greetings",
	} {
		if !strings.Contains(sdl, want) {
			t.Errorf("SDL missing %q\n--- SDL ---\n%s", want, sdl)
		}
	}
}

