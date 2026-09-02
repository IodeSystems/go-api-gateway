package gat_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/proto"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

func TestGRPCIngress_ConnectClient(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)

	gat.Register(api, g, huma.Operation{
		OperationID: "getProject",
		Method:      http.MethodGet,
		Path:        "/projects/{id}",
	}, func(ctx context.Context, in *getProjectInput) (*getProjectOutput, error) {
		return &getProjectOutput{Body: project{ID: in.ID, Name: "Project " + in.ID}}, nil
	})

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}

	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.EnableHTTP2 = true
	srv.Start()
	defer srv.Close()

	// Pull the FDS off /api/schema/proto so we can build a dynamic
	// client without depending on protoc.
	fdsBytes := mustGET(t, srv.URL+"/api/schema/proto")
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(fdsBytes, fds); err != nil {
		t.Fatalf("unmarshal FDS: %v", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("NewFiles: %v", err)
	}

	md := findMethod(t, files, "getProject")
	procedure := "/" + string(md.Parent().(protoreflect.ServiceDescriptor).FullName()) + "/" + string(md.Name())

	// Build a connect client over a dynamic message type and call it.
	httpClient := srv.Client()
	client := connect.NewClient[dynamicpb.Message, dynamicpb.Message](
		httpClient,
		srv.URL+"/api/grpc"+procedure,
		connect.WithSchema(md),
		connect.WithResponseInitializer(func(spec connect.Spec, msg any) error {
			dyn, ok := msg.(*dynamicpb.Message)
			if !ok {
				return nil
			}
			methodDesc, ok := spec.Schema.(protoreflect.MethodDescriptor)
			if !ok {
				return nil
			}
			*dyn = *dynamicpb.NewMessage(methodDesc.Output())
			return nil
		}),
	)

	reqMsg := dynamicpb.NewMessage(md.Input())
	idField := md.Input().Fields().ByName("id")
	if idField == nil {
		t.Fatalf("input message has no 'id' field; got fields: %s", listFields(md.Input()))
	}
	reqMsg.Set(idField, protoreflect.ValueOfString("p1"))

	resp, err := client.CallUnary(context.Background(), connect.NewRequest(reqMsg))
	if err != nil {
		t.Fatalf("CallUnary: %v", err)
	}

	gotJSON, err := json.Marshal(dynamicAsJSON(resp.Msg))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(gotJSON), "Project p1") {
		t.Errorf("response missing expected payload: %s", gotJSON)
	}
}

func findMethod(t *testing.T, files *protoregistry.Files, opName string) protoreflect.MethodDescriptor {
	t.Helper()
	var found protoreflect.MethodDescriptor
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			for j := 0; j < sd.Methods().Len(); j++ {
				md := sd.Methods().Get(j)
				if string(md.Name()) == opName {
					found = md
					return false
				}
			}
		}
		return true
	})
	if found == nil {
		t.Fatalf("method %q not found in FDS", opName)
	}
	return found
}

func listFields(md protoreflect.MessageDescriptor) string {
	var names []string
	for i := 0; i < md.Fields().Len(); i++ {
		names = append(names, string(md.Fields().Get(i).Name()))
	}
	return strings.Join(names, ",")
}

func dynamicAsJSON(msg *dynamicpb.Message) map[string]any {
	out := map[string]any{}
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		out[string(fd.Name())] = v.Interface()
		return true
	})
	return out
}
