package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestCrossFormatIngressMatrix asserts the (ingress × source) matrix
// promised by the IR-as-equalizer architecture: every registered
// service should be reachable through every ingress, regardless of
// the source's native format. Sources: proto (greeter), openapi
// (test), graphql (pets). Ingresses: GraphQL (gw.Handler), HTTP
// (gw.IngressHandler), gRPC (gw.GRPCUnknownHandler).
func TestCrossFormatIngressMatrix(t *testing.T) {
	f := newCrossFormatFixture(t)

	t.Run("graphql_x_proto", func(t *testing.T) {
		body := f.postGraphQL(t, `{"query":"{ greeter { hello(name:\"world\") { greeting } } }"}`)
		if !strings.Contains(body, "hello world") {
			t.Fatalf("body=%s", body)
		}
	})
	t.Run("graphql_x_openapi", func(t *testing.T) {
		body := f.postGraphQL(t, `{"query":"{ test { getThing(id:\"42\") { id name } } }"}`)
		if !strings.Contains(body, `"abc"`) || !strings.Contains(body, `"thing"`) {
			t.Fatalf("body=%s", body)
		}
	})
	t.Run("graphql_x_graphql", func(t *testing.T) {
		body := f.postGraphQL(t, `{"query":"{ pets { users { id name role } } }"}`)
		if !strings.Contains(body, "alice") || !strings.Contains(body, "MEMBER") {
			t.Fatalf("body=%s", body)
		}
	})

	t.Run("http_x_proto", func(t *testing.T) {
		resp, err := http.Post(f.httpServer.URL+"/greeter.v1.GreeterService/Hello",
			"application/json", strings.NewReader(`{"name":"world"}`))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d body=%s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "hello world") {
			t.Fatalf("body=%s", body)
		}
	})
	t.Run("http_x_openapi", func(t *testing.T) {
		resp, err := http.Get(f.httpServer.URL + "/things/42")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d body=%s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), `"id":"abc"`) {
			t.Fatalf("body=%s", body)
		}
	})
	t.Run("http_x_graphql", func(t *testing.T) {
		// Synthesized REST route per ir.RenderOpenAPI(svc) for a
		// graphql-origin service: /<ns>.<ver>.Service/<op>, GET for
		// OpQuery. Canonical-args dispatch synthesizes the upstream
		// query at construction.
		resp, err := http.Get(f.httpServer.URL + "/pets.v1.Service/users")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d body=%s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "alice") || !strings.Contains(string(body), "MEMBER") {
			t.Fatalf("body=%s", body)
		}
	})

	t.Run("grpc_x_proto", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := f.grpcClient.Hello(ctx, &greeterv1.HelloRequest{Name: "world"})
		if err != nil {
			t.Fatalf("Hello: %v", err)
		}
		if resp.GetGreeting() != "hello world" {
			t.Fatalf("greeting=%q", resp.GetGreeting())
		}
	})
	t.Run("grpc_x_openapi", func(t *testing.T) {
		// Synthesised gRPC route per ir.RenderProtoFiles(svc) for an
		// openapi-origin service. Method output is the openapi inline
		// response type directly (not the synthRequest wrapper) since
		// the renderer points the method at the existing svc.Types
		// entry when op.Output is a non-repeated Named ref.
		inputDesc, outputDesc := f.synthMethodDescriptors(t, "test", "v1", "getThing")
		input := dynamicpb.NewMessage(inputDesc)
		idFd := inputDesc.Fields().ByName("id")
		if idFd == nil {
			t.Fatalf("input descriptor missing id field")
		}
		input.Set(idFd, protoreflect.ValueOfString("42"))
		output := dynamicpb.NewMessage(outputDesc)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.grpcConn.Invoke(ctx, "/test.v1.Service/getThing", input, output); err != nil {
			t.Fatalf("Invoke: %v", err)
		}

		gotID := output.Get(outputDesc.Fields().ByName("id")).String()
		gotName := output.Get(outputDesc.Fields().ByName("name")).String()
		if gotID != "abc" {
			t.Fatalf("id=%q want abc", gotID)
		}
		if gotName != "thing" {
			t.Fatalf("name=%q want thing", gotName)
		}
	})
	t.Run("grpc_x_graphql", func(t *testing.T) {
		// Stitched-graphql op `users` returns `[User]`. The renderer
		// emits a `usersResponse` wrapper with `repeated value` since
		// proto methods can't return a top-level repeated message.
		inputDesc, outputDesc := f.synthMethodDescriptors(t, "pets", "v1", "users")
		input := dynamicpb.NewMessage(inputDesc)
		output := dynamicpb.NewMessage(outputDesc)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.grpcConn.Invoke(ctx, "/pets.v1.Service/users", input, output); err != nil {
			t.Fatalf("Invoke: %v", err)
		}

		valueFd := outputDesc.Fields().ByName("value")
		if valueFd == nil {
			t.Fatalf("output descriptor missing value field")
		}
		if !valueFd.IsList() {
			t.Fatalf("value field is not a list")
		}
		list := output.Get(valueFd).List()
		if list.Len() != 2 {
			t.Fatalf("users len=%d want 2", list.Len())
		}
		userDesc := valueFd.Message()
		idFd := userDesc.Fields().ByName("id")
		nameFd := userDesc.Fields().ByName("name")
		roleFd := userDesc.Fields().ByName("role")
		first := list.Get(0).Message()
		if got := first.Get(idFd).String(); got != "1" {
			t.Fatalf("user[0].id=%q want 1", got)
		}
		if got := first.Get(nameFd).String(); got != "alice" {
			t.Fatalf("user[0].name=%q want alice", got)
		}
		// role is a graphql enum; protoreflect.Value.String() on an
		// enum field returns the number, so resolve the name via the
		// field's enum descriptor.
		roleNum := first.Get(roleFd).Enum()
		roleEV := roleFd.Enum().Values().ByNumber(roleNum)
		if roleEV == nil {
			t.Fatalf("unknown role number %d", roleNum)
		}
		// The descriptor spelling is prefixed with the enum type
		// (`Role_ADMIN`), deliberately: proto scopes enum VALUE names to
		// the package, so two enums sharing a value would collide at
		// files-registry build. Every non-proto surface speaks the bare
		// name, and ir.EnumValueByExternalName is the documented bridge.
		if got, want := string(roleEV.Name()), ir.ProtoEnumValueName("Role", "ADMIN"); got != want {
			t.Fatalf("user[0].role descriptor name=%q want %q", got, want)
		}
		if ev := ir.EnumValueByExternalName(roleFd.Enum(), "ADMIN"); ev == nil {
			t.Fatal(`external name "ADMIN" must resolve against the rendered descriptor`)
		} else if ev.Number() != roleNum {
			t.Fatalf("external lookup gave number %d, field holds %d", ev.Number(), roleNum)
		}
	})
}

// synthMethodDescriptors returns the synthesised input/output
// MessageDescriptors for one method on a non-proto slot. Mirrors
// what rebuildGRPCIngressLocked does in its cross-kind second pass
// — render the slot's IR via ir.RenderProtoFiles, hand the
// FileDescriptorSet to protodesc.NewFiles, then walk to the named
// method. Used by the gRPC×{openapi,graphql} matrix cells so the
// test can build dynamicpb input/output messages without hard-
// coding the generated proto.
func (f *crossFormatFixture) synthMethodDescriptors(t *testing.T, ns, ver, opName string) (input, output protoreflect.MessageDescriptor) {
	t.Helper()
	f.gw.mu.Lock()
	slot := f.gw.slots[poolKey{namespace: ns, version: ver}]
	f.gw.mu.Unlock()
	if slot == nil {
		t.Fatalf("no slot for %s/%s", ns, ver)
	}
	fds, err := ir.RenderProtoFiles(slot.ir)
	if err != nil {
		t.Fatalf("RenderProtoFiles: %v", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("protodesc.NewFiles: %v", err)
	}
	var found protoreflect.MethodDescriptor
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		ss := fd.Services()
		for i := 0; i < ss.Len(); i++ {
			sd := ss.Get(i)
			ms := sd.Methods()
			for j := 0; j < ms.Len(); j++ {
				if string(ms.Get(j).Name()) == opName {
					found = ms.Get(j)
					return false
				}
			}
		}
		return true
	})
	if found == nil {
		t.Fatalf("synthesised method %s not found in %s/%s", opName, ns, ver)
	}
	return found.Input(), found.Output()
}

// crossFormatFixture is a single Gateway that has registered one of
// each source kind (proto / openapi / graphql) plus all three ingress
// servers — so the same gateway state is exercised across every cell.
// Single-fixture is deliberate: a per-cell fixture wouldn't catch
// cross-kind interference (e.g. ingress route table collisions).
type crossFormatFixture struct {
	gw         *Gateway
	httpServer *httptest.Server
	gqlServer  *httptest.Server
	grpcServer *grpc.Server
	grpcConn   *grpc.ClientConn
	grpcClient greeterv1.GreeterServiceClient
}

func (f *crossFormatFixture) postGraphQL(t *testing.T, query string) string {
	t.Helper()
	resp, err := http.Post(f.gqlServer.URL+"/graphql", "application/json", strings.NewReader(query))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var env struct {
		Errors []json.RawMessage `json:"errors"`
	}
	_ = json.Unmarshal(body, &env)
	if len(env.Errors) > 0 {
		t.Fatalf("graphql errors: %s", body)
	}
	return string(body)
}

func newCrossFormatFixture(t *testing.T) *crossFormatFixture {
	t.Helper()

	// proto backend: in-process gRPC greeter on ephemeral port.
	protoLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proto listen: %v", err)
	}
	protoSrv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(protoSrv, &fakeGreeterServer{})
	go func() { _ = protoSrv.Serve(protoLis) }()
	t.Cleanup(protoSrv.Stop)

	// openapi backend: httptest server returning a fixed thing.
	openAPIBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"abc","name":"thing"}`))
	}))
	t.Cleanup(openAPIBackend.Close)

	// graphql backend: pets fixture from graphql_ingest_test (canned
	// introspection + scripted users response).
	rf := newRemoteFixture(t)
	rf.queryHandler = func(string, map[string]any) any {
		return map[string]any{
			"users": []map[string]any{
				{"id": "1", "name": "alice", "role": "ADMIN"},
				{"id": "2", "name": "bob", "role": "MEMBER"},
			},
		}
	}

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)

	if err := gw.AddProtoBytes("greeter.proto", testProtoBytes(t, "greeter.proto"), To(protoLis.Addr().String()), As("greeter")); err != nil {
		t.Fatalf("AddProtoDescriptor: %v", err)
	}
	if err := gw.AddOpenAPIBytes([]byte(minimalOpenAPISpec), To(openAPIBackend.URL), As("test")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}
	if err := gw.AddGraphQL(rf.server.URL, As("pets")); err != nil {
		t.Fatalf("AddGraphQL: %v", err)
	}

	gqlServer := httptest.NewServer(gw.Handler())
	t.Cleanup(gqlServer.Close)
	httpServer := httptest.NewServer(gw.IngressHandler())
	t.Cleanup(httpServer.Close)

	// Frontend gRPC server hosting the gateway's unknown handler.
	feLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("frontend listen: %v", err)
	}
	feSrv := grpc.NewServer(grpc.UnknownServiceHandler(gw.GRPCUnknownHandler()))
	go func() { _ = feSrv.Serve(feLis) }()
	t.Cleanup(feSrv.Stop)

	conn, err := grpc.NewClient(feLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial frontend: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &crossFormatFixture{
		gw:         gw,
		httpServer: httpServer,
		gqlServer:  gqlServer,
		grpcServer: feSrv,
		grpcConn:   conn,
		grpcClient: greeterv1.NewGreeterServiceClient(conn),
	}
}
