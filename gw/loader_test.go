package gateway

import (
	"strings"
	"testing"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestLoadProto_PreservesComments pins that the path-based proto load
// (used by AddProto) preserves leading comments through to IR
// Description fields. protoc-gen-go's embedded raw descriptor strips
// SourceCodeInfo unconditionally, so AddProtoDescriptor /
// SelfRegister flows lose comments by design — path-based loading via
// protocompile is the only ingest path that carries doc strings, and
// that's what the MCP search corpus depends on for proto-source
// services.
func TestLoadProto_PreservesComments(t *testing.T) {
	fd, err := loadProto("../examples/multi/protos/greeter.proto")
	if err != nil {
		t.Fatalf("loadProto: %v", err)
	}
	svcs := ir.IngestProto(fd)
	if len(svcs) == 0 {
		t.Fatal("IngestProto returned no services")
	}
	svc := svcs[0]

	var greetings *ir.Operation
	for _, op := range svc.Operations {
		if op.Name == "Greetings" {
			greetings = op
			break
		}
	}
	if greetings == nil {
		t.Fatal("Greetings op missing")
	}
	if !strings.Contains(greetings.Description, "Server-streaming") {
		t.Errorf("Greetings.Description = %q, want contains 'Server-streaming'", greetings.Description)
	}

	gf, ok := svc.Types["greeter.v1.GreetingsFilter"]
	if !ok {
		t.Fatal("GreetingsFilter type missing")
	}
	var nameField *ir.Field
	for _, f := range gf.Fields {
		if f.Name == "name" {
			nameField = f
			break
		}
	}
	if nameField == nil {
		t.Fatal("GreetingsFilter.name field missing")
	}
	if !strings.Contains(nameField.Description, "Filter by recipient") {
		t.Errorf("GreetingsFilter.name.Description = %q, want contains 'Filter by recipient'", nameField.Description)
	}
}
