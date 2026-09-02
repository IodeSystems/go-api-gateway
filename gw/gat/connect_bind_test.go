package gat

// bindPlan replaced messageToArgs + bindInput on the Connect request
// path. Same contract as the other two walkers: it is only safe if it
// leaves the input struct in exactly the state the old path did.
//
// bindInput skips an argument it cannot map instead of erroring, so a
// divergence would surface as a handler quietly reading a zero value —
// the failure mode behind both embedded-parameter bugs. These tests
// bind the same message both ways and require reflect.DeepEqual, and
// pin the cases where the plan must refuse to form at all.

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// --- input types under test ---------------------------------------------

type bindScope struct {
	Dir string `query:"dir"`
}

type bindListInput struct {
	bindScope         // unexported embed: huma cannot see Dir, and neither should we
	Limit     int64   `query:"limit"`
	Cursor    string  `query:"cursor"`
	Verbose   bool    `query:"verbose"`
	Ratio     float64 `query:"ratio"`
}

// BindScope is the exported counterpart — huma promotes its parameters,
// and so must the plan.
type BindScope struct {
	Dir string `query:"dir"`
}

type bindEmbedInput struct {
	BindScope
	Limit int64 `query:"limit"`
}

type bindPtrInput struct {
	Limit *int64 `query:"limit"`
	Name  string `path:"name"`
}

type bindBodyChild struct {
	Note string `json:"note"`
	Size int32  `json:"size"`
}

type bindBodyPayload struct {
	ID       string          `json:"id"`
	Tags     []string        `json:"tags"`
	Children []bindBodyChild `json:"children"`
	Nested   bindBodyChild   `json:"nested"`
}

type bindBodyInput struct {
	Limit int64 `query:"limit"`
	Body  bindBodyPayload
}

// bindTimeInput carries a time.Time, a json.Unmarshaler. The plan must
// refuse: the map path routes it through a JSON round-trip that honours
// UnmarshalJSON, and a field walk would not.
type bindTimeInput struct {
	Since time.Time `query:"since"`
}

type bindEnumInput struct {
	Color string `query:"color"`
}

// --- descriptor fixtures -------------------------------------------------

func bindDesc(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto, extra ...*descriptorpb.DescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	msgs := append([]*descriptorpb.DescriptorProto{{Name: proto.String(name), Field: fields}}, extra...)
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("bind_" + strings.ToLower(name) + ".proto"),
		Package:     proto.String("bindtest." + strings.ToLower(name)),
		Syntax:      proto.String("proto3"),
		MessageType: msgs,
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Color"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("COLOR_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("COLOR_RED"), Number: proto.Int32(1)},
			},
		}},
	}
	files, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fdp},
	})
	if err != nil {
		t.Fatalf("NewFiles: %v", err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName("bindtest." + strings.ToLower(name) + "." + name))
	if err != nil {
		t.Fatalf("find %s: %v", name, err)
	}
	return d.(protoreflect.MessageDescriptor)
}

func irOp(args ...*ir.Arg) *ir.Operation {
	return &ir.Operation{Name: "op", Args: args}
}

func qArg(name string) *ir.Arg { return &ir.Arg{Name: name, OpenAPILocation: "query"} }
func pArg(name string) *ir.Arg { return &ir.Arg{Name: name, OpenAPILocation: "path"} }
func bArg(name string) *ir.Arg { return &ir.Arg{Name: name, OpenAPILocation: "body"} }

// bothPaths binds msg through the plan and through
// messageToArgs+bindInput, returning both results for comparison.
func bothPaths(t *testing.T, typ reflect.Type, op *ir.Operation, bodyArg string, md protoreflect.MessageDescriptor, msg *dynamicpb.Message) (planned, viaMap reflect.Value) {
	t.Helper()
	plan := buildBindPlan(typ, op, bodyArg, md)
	if plan == nil {
		t.Fatalf("expected a plan for %s", typ)
	}
	p := reflect.New(typ)
	if err := plan.bind(p.Elem(), msg); err != nil {
		t.Fatalf("plan bind: %v", err)
	}
	m := reflect.New(typ)
	if err := bindInput(m.Elem(), op, messageToArgs(msg), bodyArg); err != nil {
		t.Fatalf("bindInput: %v", err)
	}
	return p, m
}

// --- equality tests ------------------------------------------------------

func TestBindPlan_MatchesBindInput_Scalars(t *testing.T) {
	md := bindDesc(t, "ListReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
		field("cursor", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
		field("verbose", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, false),
		field("ratio", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, false),
	})
	op := irOp(qArg("limit"), qArg("cursor"), qArg("verbose"), qArg("ratio"))

	msg := dynamicpb.NewMessage(md)
	set(msg, "limit", protoreflect.ValueOfInt64(9007199254740993)) // past 2^53
	set(msg, "cursor", protoreflect.ValueOfString("abc"))
	set(msg, "verbose", protoreflect.ValueOfBool(true))
	set(msg, "ratio", protoreflect.ValueOfFloat64(0.25))

	planned, viaMap := bothPaths(t, reflect.TypeFor[bindListInput](), op, "", md, msg)
	if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
		t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
	}
	// The int64 past 2^53 is the whole reason the map path needed a
	// string-to-number repair; assert the value actually survived.
	if got := planned.Elem().Interface().(bindListInput).Limit; got != 9007199254740993 {
		t.Errorf("Limit = %d; want 9007199254740993", got)
	}
}

func TestBindPlan_MatchesBindInput_ExportedEmbed(t *testing.T) {
	md := bindDesc(t, "EmbedReq", []*descriptorpb.FieldDescriptorProto{
		field("dir", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
		field("limit", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
	})
	op := irOp(qArg("dir"), qArg("limit"))

	msg := dynamicpb.NewMessage(md)
	set(msg, "dir", protoreflect.ValueOfString("/tmp"))
	set(msg, "limit", protoreflect.ValueOfInt64(7))

	planned, viaMap := bothPaths(t, reflect.TypeFor[bindEmbedInput](), op, "", md, msg)
	if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
		t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
	}
	// The v1.3.2 bug was a parameter promoted out of an exported embed
	// arriving and being dropped. Assert it landed.
	if got := planned.Elem().Interface().(bindEmbedInput).Dir; got != "/tmp" {
		t.Errorf("promoted Dir = %q; want /tmp", got)
	}
}

func TestBindPlan_MatchesBindInput_Pointers(t *testing.T) {
	md := bindDesc(t, "PtrReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
	})
	op := irOp(qArg("limit"), pArg("name"))

	t.Run("set", func(t *testing.T) {
		msg := dynamicpb.NewMessage(md)
		set(msg, "limit", protoreflect.ValueOfInt64(12))
		set(msg, "name", protoreflect.ValueOfString("x"))
		planned, viaMap := bothPaths(t, reflect.TypeFor[bindPtrInput](), op, "", md, msg)
		if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
			t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
		}
	})

	t.Run("absent stays nil", func(t *testing.T) {
		msg := dynamicpb.NewMessage(md)
		set(msg, "name", protoreflect.ValueOfString("x"))
		planned, viaMap := bothPaths(t, reflect.TypeFor[bindPtrInput](), op, "", md, msg)
		if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
			t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
		}
		if planned.Elem().Interface().(bindPtrInput).Limit != nil {
			t.Errorf("absent optional should stay nil")
		}
	})
}

func TestBindPlan_MatchesBindInput_Body(t *testing.T) {
	child := &descriptorpb.DescriptorProto{
		Name: proto.String("Child"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("note", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
			field("size", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32, false),
		},
	}
	payload := &descriptorpb.DescriptorProto{
		Name: proto.String("Payload"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
			field("tags", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, true),
			msgField("children", 3, ".bindtest.bodyreq.Child", true),
			msgField("nested", 4, ".bindtest.bodyreq.Child", false),
		},
	}
	md := bindDesc(t, "BodyReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
		msgField("body", 2, ".bindtest.bodyreq.Payload", false),
	}, child, payload)
	op := irOp(qArg("limit"), bArg("body"))

	msg := dynamicpb.NewMessage(md)
	set(msg, "limit", protoreflect.ValueOfInt64(5))
	body := msg.Mutable(md.Fields().ByName("body")).Message()
	bd := body.Descriptor()
	body.Set(bd.Fields().ByName("id"), protoreflect.ValueOfString("p1"))
	tags := body.Mutable(bd.Fields().ByName("tags")).List()
	tags.Append(protoreflect.ValueOfString("core"))
	tags.Append(protoreflect.ValueOfString("beta"))
	kids := body.Mutable(bd.Fields().ByName("children")).List()
	k := kids.NewElement()
	k.Message().Set(k.Message().Descriptor().Fields().ByName("note"), protoreflect.ValueOfString("kid"))
	k.Message().Set(k.Message().Descriptor().Fields().ByName("size"), protoreflect.ValueOfInt32(3))
	kids.Append(k)
	nested := body.Mutable(bd.Fields().ByName("nested")).Message()
	nested.Set(nested.Descriptor().Fields().ByName("note"), protoreflect.ValueOfString("inner"))

	planned, viaMap := bothPaths(t, reflect.TypeFor[bindBodyInput](), op, "body", md, msg)
	if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
		t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
	}
	got := planned.Elem().Interface().(bindBodyInput)
	if got.Body.ID != "p1" || len(got.Body.Tags) != 2 || len(got.Body.Children) != 1 ||
		got.Body.Children[0].Size != 3 || got.Body.Nested.Note != "inner" {
		t.Errorf("body not fully bound: %+v", got.Body)
	}
}

func TestBindPlan_MatchesBindInput_AllAbsent(t *testing.T) {
	md := bindDesc(t, "ListReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
		field("cursor", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
		field("verbose", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, false),
		field("ratio", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, false),
	})
	op := irOp(qArg("limit"), qArg("cursor"), qArg("verbose"), qArg("ratio"))

	planned, viaMap := bothPaths(t, reflect.TypeFor[bindListInput](), op, "", md, dynamicpb.NewMessage(md))
	if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
		t.Errorf("diverges on empty message\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
	}
}

// The unexported embed is invisible to huma, so its parameter must stay
// unbound on both paths — binding it would be worse than the bug it
// looks like.
func TestBindPlan_LeavesUnexportedEmbedAlone(t *testing.T) {
	md := bindDesc(t, "ListReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
		field("dir", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
	})
	op := irOp(qArg("limit"), qArg("dir"))

	msg := dynamicpb.NewMessage(md)
	set(msg, "limit", protoreflect.ValueOfInt64(1))
	set(msg, "dir", protoreflect.ValueOfString("/should-not-land"))

	planned, viaMap := bothPaths(t, reflect.TypeFor[bindListInput](), op, "", md, msg)
	if !reflect.DeepEqual(planned.Interface(), viaMap.Interface()) {
		t.Errorf("diverges\nplanned: %+v\nviaMap:  %+v", planned.Elem(), viaMap.Elem())
	}
	if got := planned.Elem().Interface().(bindListInput).bindScope.Dir; got != "" {
		t.Errorf("unexported embed was written (%q); huma cannot see it, so neither should the plan", got)
	}
}

// --- refusal tests -------------------------------------------------------

func TestBindPlan_RefusesCustomUnmarshaler(t *testing.T) {
	md := bindDesc(t, "TimeReq", []*descriptorpb.FieldDescriptorProto{
		field("since", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
	})
	op := irOp(qArg("since"))
	if plan := buildBindPlan(reflect.TypeFor[bindTimeInput](), op, "", md); plan != nil {
		t.Error("expected no plan: time.Time decodes itself, so the map path's JSON round-trip must be kept")
	}
}

func TestBindPlan_RefusesEnum(t *testing.T) {
	md := bindDesc(t, "EnumReq", []*descriptorpb.FieldDescriptorProto{
		enumField("color", 1, ".bindtest.enumreq.Color"),
	})
	op := irOp(qArg("color"))
	if plan := buildBindPlan(reflect.TypeFor[bindEnumInput](), op, "", md); plan != nil {
		t.Error("expected no plan: enum rendering is the map path's business")
	}
}

func TestBindPlan_RefusesTypeMismatch(t *testing.T) {
	// Proto says string, the Go field is an int64.
	md := bindDesc(t, "MismatchReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
	})
	op := irOp(qArg("limit"))
	if plan := buildBindPlan(reflect.TypeFor[bindListInput](), op, "", md); plan != nil {
		t.Error("expected no plan for a string proto field feeding an int64 Go field")
	}
}

func TestBindPlan_RefusesNothingMapped(t *testing.T) {
	md := bindDesc(t, "OtherReq", []*descriptorpb.FieldDescriptorProto{
		field("unrelated", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, false),
	})
	op := irOp(qArg("unrelated"))
	if plan := buildBindPlan(reflect.TypeFor[bindListInput](), op, "", md); plan != nil {
		t.Error("expected no plan when no proto field pairs with a Go field")
	}
}

// The gatbench shapes are what the published numbers are measured on.
// If the plan silently stopped forming for them the benchmark would
// still pass while quietly measuring the map path.
func TestBindPlan_FormsForBenchmarkShapes(t *testing.T) {
	md := bindDesc(t, "ListReq", []*descriptorpb.FieldDescriptorProto{
		field("limit", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
	})
	if buildBindPlan(reflect.TypeFor[bindEmbedInput](), irOp(qArg("limit")), "", md) == nil {
		t.Error("no plan for a plain scalar input — the fast path is not being exercised")
	}
}

// The synthetic descriptors above are hand-built. This one goes through
// the real path — huma ops, gat ingest, gat's own proto synthesis — and
// asserts a plan forms for each. gat's naming and message synthesis are
// exactly what the plan has to agree with, so a change there that
// quietly drops every operation onto the map path would show up here
// and nowhere else.
func TestBindPlan_FormsForRealGatOperations(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Planned", "1.0.0"))
	g, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	Register(api, g, huma.Operation{
		OperationID: "getThing",
		Method:      http.MethodGet,
		Path:        "/things/{id}",
	}, func(ctx context.Context, in *struct {
		ID string `path:"id"`
	}) (*struct {
		Body struct {
			Name string `json:"name"`
		}
	}, error) {
		return nil, nil
	})

	Register(api, g, huma.Operation{
		OperationID: "listThings",
		Method:      http.MethodGet,
		Path:        "/things",
	}, func(ctx context.Context, in *struct {
		Limit int `query:"limit"`
	}) (*struct {
		Body struct {
			Names []string `json:"names"`
		}
	}, error) {
		return nil, nil
	})

	if err := RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}

	fds, err := ir.RenderProtoFiles(g.services)
	if err != nil {
		t.Fatalf("RenderProtoFiles: %v", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("NewFiles: %v", err)
	}

	byName := map[string]*capturedOp{}
	for _, c := range g.captured {
		byName[c.op.OperationID] = c
	}

	planned := map[string]bool{}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			for j := 0; j < sd.Methods().Len(); j++ {
				md := sd.Methods().Get(j)
				c, ok := byName[string(md.Name())]
				if !ok {
					continue
				}
				planned[string(md.Name())] = buildBindPlan(c.inputType, c.irOp, "", md.Input()) != nil
			}
		}
		return true
	})

	for _, op := range []string{"getThing", "listThings"} {
		got, seen := planned[op]
		if !seen {
			t.Errorf("%s: no proto method found", op)
			continue
		}
		if !got {
			t.Errorf("%s: no bind plan formed — the operation silently fell back to the map path", op)
		}
	}
}
