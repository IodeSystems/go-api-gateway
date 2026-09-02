package gat_test

// Cost-of-abstraction benchmarks for gat's ingresses.
//
// One huma handler, four call paths into it:
//
//	Handler      direct Go call            — the floor; business logic only
//	REST         huma's own router         — huma's binding + JSON, no gat
//	GraphQL      gat GraphQL ingress       — parse/plan-cache/exec + reflect bind
//	ConnectJSON  gat connect/gRPC ingress  — protojson round-trip + reflect bind
//
// Everything runs through http.Handler.ServeHTTP against an
// httptest.ResponseRecorder, so the numbers exclude kernel sockets and
// TLS. Loopback HTTP costs ~300-400µs on its own (see docs/perf.md);
// including it would drown the deltas these benchmarks exist to show.
// REST is the baseline to subtract, not Handler — huma is present on
// every path, so REST isolates what gat itself adds.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

// benchGetProject is the single handler every path below dispatches
// to. Deliberately trivial: the point is to measure the ingress, so
// the handler's own cost should be near zero and identical across
// paths.
func benchGetProject(ctx context.Context, in *getProjectInput) (*getProjectOutput, error) {
	return &getProjectOutput{Body: project{ID: in.ID, Name: "Project " + in.ID}}, nil
}

// benchListProjects returns a 25-element list — the fan-out shape, where
// per-field executor cost shows up instead of per-request fixed cost.
func benchListProjects(ctx context.Context, in *listProjectsInput) (*listProjectsOutput, error) {
	out := &listProjectsOutput{}
	n := in.Limit
	if n <= 0 || n > 25 {
		n = 25
	}
	out.Body.Projects = make([]project, n)
	for i := range out.Body.Projects {
		out.Body.Projects[i] = project{ID: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("Project %d", i)}
	}
	return out, nil
}

// benchCreateInput carries a 25-element body — the shape where request
// binding actually costs something. The list benchmarks above send a
// one-scalar request, so they say nothing about how the binder scales
// with body size.
type benchCreateInput struct {
	Body struct {
		Projects []project `json:"projects"`
	}
}

type benchCreateOutput struct {
	Body struct {
		Created int `json:"created"`
	}
}

func benchCreateProjects(ctx context.Context, in *benchCreateInput) (*benchCreateOutput, error) {
	out := &benchCreateOutput{}
	out.Body.Created = len(in.Body.Projects)
	return out, nil
}

// benchGateway builds the mux/gat pair the ingress benchmarks share.
// Both operations are registered on every path so the schema shape is
// identical regardless of which ingress a given benchmark drives.
func benchGateway(tb testing.TB) *http.ServeMux {
	tb.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Bench", "1.0.0"))
	g, err := gat.New()
	if err != nil {
		tb.Fatalf("gat.New: %v", err)
	}
	gat.Register(api, g, huma.Operation{
		OperationID: "getProject",
		Method:      http.MethodGet,
		Path:        "/projects/{id}",
	}, benchGetProject)
	gat.Register(api, g, huma.Operation{
		OperationID: "listProjects",
		Method:      http.MethodGet,
		Path:        "/projects",
	}, benchListProjects)
	gat.Register(api, g, huma.Operation{
		OperationID: "createProjects",
		Method:      http.MethodPost,
		Path:        "/projects",
	}, benchCreateProjects)

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		tb.Fatalf("RegisterHuma: %v", err)
	}
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		tb.Fatalf("RegisterGRPC: %v", err)
	}
	return mux
}

// serveOK runs one request through the mux and fails the benchmark on
// a non-2xx or a GraphQL `errors` key — a silently-erroring path would
// otherwise benchmark the error branch and look fast.
func serveOK(tb testing.TB, mux *http.ServeMux, req *http.Request) {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code < 200 || rec.Code >= 300 {
		tb.Fatalf("%s %s: %d %s", req.Method, req.URL.Path, rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"errors"`) {
		tb.Fatalf("%s %s: graphql errors: %s", req.Method, req.URL.Path, rec.Body.String())
	}
}

func newGraphQLReq(query string) *http.Request {
	payload, _ := json.Marshal(map[string]any{"query": query})
	req := httptest.NewRequest(http.MethodPost, "/api/graphql", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// connectProcedure pulls the FDS off /api/schema/proto and returns the
// canonical `/{Service.FullName}/{Method}` path for an operation, so
// the connect benchmark doesn't hardcode gat's naming rules.
func connectProcedure(tb testing.TB, mux *http.ServeMux, operationID string) string {
	tb.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/schema/proto", nil))
	if rec.Code != http.StatusOK {
		tb.Fatalf("GET /api/schema/proto: %d %s", rec.Code, rec.Body.String())
	}
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(rec.Body.Bytes(), fds); err != nil {
		tb.Fatalf("unmarshal FDS: %v", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		tb.Fatalf("NewFiles: %v", err)
	}
	var found string
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		svcs := fd.Services()
		for i := 0; i < svcs.Len(); i++ {
			svc := svcs.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				if strings.EqualFold(string(m.Name()), operationID) {
					found = "/" + string(svc.FullName()) + "/" + string(m.Name())
					return false
				}
			}
		}
		return true
	})
	if found == "" {
		tb.Fatalf("no proto method for operation %q", operationID)
	}
	return found
}

// newConnectReq builds a Connect unary POST. Connect's JSON codec over
// HTTP/1.1 is a plain POST with Content-Type: application/json and the
// bare message as the body — no h2c server needed to exercise the same
// handler a grpc-go client would reach.
func newConnectReq(procedure, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/grpc"+procedure, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	return req
}

// --- point lookup: getProject ------------------------------------------

func BenchmarkGatIngress_Handler(b *testing.B) {
	ctx := context.Background()
	in := &getProjectInput{ID: "p1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := benchGetProject(ctx, in)
		if err != nil || out.Body.ID != "p1" {
			b.Fatalf("handler: %v", err)
		}
	}
}

func BenchmarkGatIngress_REST(b *testing.B) {
	mux := benchGateway(b)
	serveOK(b, mux, httptest.NewRequest(http.MethodGet, "/projects/p1", nil))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, httptest.NewRequest(http.MethodGet, "/projects/p1", nil))
	}
}

func BenchmarkGatIngress_GraphQL(b *testing.B) {
	mux := benchGateway(b)
	const q = `{ Bench { getProject(id: "p1") { id name } } }`
	serveOK(b, mux, newGraphQLReq(q))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newGraphQLReq(q))
	}
}

func BenchmarkGatIngress_ConnectJSON(b *testing.B) {
	mux := benchGateway(b)
	proc := connectProcedure(b, mux, "getProject")
	const body = `{"id":"p1"}`
	serveOK(b, mux, newConnectReq(proc, body))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newConnectReq(proc, body))
	}
}

// --- fan-out: listProjects (25 rows) -----------------------------------

func BenchmarkGatIngressList_Handler(b *testing.B) {
	ctx := context.Background()
	in := &listProjectsInput{Limit: 25}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := benchListProjects(ctx, in)
		if err != nil || len(out.Body.Projects) != 25 {
			b.Fatalf("handler: %v", err)
		}
	}
}

func BenchmarkGatIngressList_REST(b *testing.B) {
	mux := benchGateway(b)
	serveOK(b, mux, httptest.NewRequest(http.MethodGet, "/projects?limit=25", nil))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, httptest.NewRequest(http.MethodGet, "/projects?limit=25", nil))
	}
}

func BenchmarkGatIngressList_GraphQL(b *testing.B) {
	mux := benchGateway(b)
	const q = `{ Bench { listProjects(limit: 25) { projects { id name } } } }`
	serveOK(b, mux, newGraphQLReq(q))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newGraphQLReq(q))
	}
}

func BenchmarkGatIngressList_ConnectJSON(b *testing.B) {
	mux := benchGateway(b)
	proc := connectProcedure(b, mux, "listProjects")
	const body = `{"limit":25}`
	serveOK(b, mux, newConnectReq(proc, body))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newConnectReq(proc, body))
	}
}

// --- body-heavy request: createProjects (25 rows in) --------------------
//
// The point-lookup and list benchmarks both send a tiny request, so they
// measure response cost almost exclusively. This one inverts that: 25
// rows go IN and a single count comes back, which is where the Connect
// ingress's request binding is actually on the hook.

func benchCreateBody() string {
	var b strings.Builder
	b.WriteString(`{"body":{"projects":[`)
	for i := 0; i < 25; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"p%d","name":"Project %d"}`, i, i)
	}
	b.WriteString(`]}}`)
	return b.String()
}

func BenchmarkGatIngressCreate_REST(b *testing.B) {
	mux := benchGateway(b)
	body := `{"projects":[` + strings.TrimSuffix(strings.Repeat(`{"id":"p","name":"n"},`, 25), ",") + `]}`
	newReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/projects", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		return r
	}
	serveOK(b, mux, newReq())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newReq())
	}
}

func BenchmarkGatIngressCreate_ConnectJSON(b *testing.B) {
	mux := benchGateway(b)
	proc := connectProcedure(b, mux, "createProjects")
	body := benchCreateBody()
	serveOK(b, mux, newConnectReq(proc, body))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serveOK(b, mux, newConnectReq(proc, body))
	}
}
