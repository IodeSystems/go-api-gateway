package ir

import (
	"testing"

	authv1 "github.com/iodesystems/gwag/v2/examples/auth/gen/auth/v1"
	userv1 "github.com/iodesystems/gwag/v2/examples/auth/gen/user/v1"
	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

// TestProtoRoundtrip verifies that ingest → IR → render via the
// same-kind Origin shortcut reproduces the source FileDescriptor.
// Cross-kind paths are tested separately; this is the "the IR
// preserves enough to re-emit proto verbatim" guarantee.
func TestProtoRoundtrip(t *testing.T) {
	svcs := IngestProto(greeterv1.File_greeter_proto)
	if len(svcs) == 0 {
		t.Fatal("IngestProto returned no services")
	}
	svc := svcs[0]
	if svc.ServiceName != "GreeterService" {
		t.Errorf("ServiceName = %q, want GreeterService", svc.ServiceName)
	}
	// Methods: Hello (unary), Greetings (server-streaming),
	// Echo (bidi). All three should land as Operations with
	// matching streaming flags.
	if got := len(svc.Operations); got != 3 {
		t.Fatalf("Operations = %d, want 3", got)
	}
	wantOps := map[string]struct {
		kind            OpKind
		streamingClient bool
	}{
		"Hello":     {OpQuery, false},
		"Greetings": {OpSubscription, false},
		"Echo":      {OpSubscription, true}, // bidi: server-streaming + client-streaming
	}
	for _, op := range svc.Operations {
		want, ok := wantOps[op.Name]
		if !ok {
			t.Errorf("unexpected op %q", op.Name)
			continue
		}
		if op.Kind != want.kind {
			t.Errorf("op %s Kind = %v, want %v", op.Name, op.Kind, want.kind)
		}
		if op.StreamingClient != want.streamingClient {
			t.Errorf("op %s StreamingClient = %v, want %v", op.Name, op.StreamingClient, want.streamingClient)
		}
		if op.OriginKind != KindProto {
			t.Errorf("op %s OriginKind = %v, want KindProto", op.Name, op.OriginKind)
		}
	}

	// Types: HelloRequest, HelloResponse, GreetingsFilter, Greeting.
	// All four messages should land in svc.Types keyed by FullName.
	for _, fn := range []string{
		"greeter.v1.HelloRequest",
		"greeter.v1.HelloResponse",
		"greeter.v1.GreetingsFilter",
		"greeter.v1.Greeting",
	} {
		if _, ok := svc.Types[fn]; !ok {
			t.Errorf("Types missing %q", fn)
		}
	}

	// Render back to FDS via the Origin shortcut and verify the file
	// path lands in the output. The shortcut returns the source
	// FileDescriptorProto — so the file count + name match the
	// source.
	fds, err := RenderProtoFiles([]*Service{svc})
	if err != nil {
		t.Fatalf("RenderProtoFiles: %v", err)
	}
	if got := len(fds.File); got != 1 {
		t.Fatalf("FileDescriptorSet has %d files, want 1", got)
	}
	if got := fds.File[0].GetName(); got != "greeter.proto" {
		t.Errorf("file name = %q, want greeter.proto", got)
	}
	if got := fds.File[0].GetPackage(); got != "greeter.v1" {
		t.Errorf("package = %q, want greeter.v1", got)
	}
	// Three methods on the original GreeterService.
	gotMethods := map[string]bool{}
	for _, sp := range fds.File[0].GetService() {
		for _, mp := range sp.GetMethod() {
			gotMethods[mp.GetName()] = true
		}
	}
	for _, want := range []string{"Hello", "Greetings", "Echo"} {
		if !gotMethods[want] {
			t.Errorf("rendered FDS missing method %q", want)
		}
	}
}

// TestProtoIngestFieldDetails sanity-checks one message's fields
// since the field-level info (numbers, json_name, repeated, etc.)
// is what cross-kind renderers depend on later.
func TestProtoIngestFieldDetails(t *testing.T) {
	svcs := IngestProto(greeterv1.File_greeter_proto)
	svc := svcs[0]
	greeting, ok := svc.Types["greeter.v1.Greeting"]
	if !ok {
		t.Fatal("greeter.v1.Greeting missing from Types")
	}
	// Greeting { string greeting = 1; string for_name = 2; }
	if got := len(greeting.Fields); got != 2 {
		t.Fatalf("Greeting has %d fields, want 2", got)
	}
	for _, f := range greeting.Fields {
		switch f.Name {
		case "greeting":
			if f.ProtoNumber != 1 {
				t.Errorf("greeting.greeting ProtoNumber = %d, want 1", f.ProtoNumber)
			}
			if f.Type.Builtin != ScalarString {
				t.Errorf("greeting.greeting Type.Builtin = %v, want ScalarString", f.Type.Builtin)
			}
		case "for_name":
			if f.ProtoNumber != 2 {
				t.Errorf("greeting.for_name ProtoNumber = %d, want 2", f.ProtoNumber)
			}
			if f.JSONName != "forName" {
				t.Errorf("greeting.for_name JSONName = %q, want forName", f.JSONName)
			}
		default:
			t.Errorf("unexpected field %q", f.Name)
		}
	}
}

// TestProtoIngest_TransitiveImports covers the cross-file path:
// user.proto imports auth.proto's Context message; ingesting only
// user.proto should still register auth.v1.Context in the IR Types
// map, so an IR-driven type-builder can resolve cross-file refs
// without falling back to the descriptor graph.
func TestProtoIngest_TransitiveImports(t *testing.T) {
	svcs := IngestProto(userv1.File_user_proto)
	if len(svcs) == 0 {
		t.Fatal("IngestProto(user) returned no services")
	}
	svc := svcs[0]

	// User's own message must still be there.
	if _, ok := svc.Types["user.v1.GetMeRequest"]; !ok {
		keys := []string{}
		for k := range svc.Types {
			keys = append(keys, k)
		}
		t.Errorf("user.v1.GetMeRequest missing from Types; have %v", keys)
	}
	// And the imported auth.v1.Context message lands too.
	if _, ok := svc.Types["auth.v1.Context"]; !ok {
		keys := []string{}
		for k := range svc.Types {
			keys = append(keys, k)
		}
		t.Errorf("imported auth.v1.Context missing from Types; have %v", keys)
	}

	// Sanity: the package-level auth-only ingest still works on its own.
	authSvcs := IngestProto(authv1.File_auth_proto)
	if len(authSvcs) == 0 {
		t.Fatal("IngestProto(auth) returned no services")
	}
	if _, ok := authSvcs[0].Types["auth.v1.Context"]; !ok {
		t.Errorf("auth.v1.Context missing from auth-only ingest")
	}
}
