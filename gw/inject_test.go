package gateway

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	authv1 "github.com/iodesystems/gwag/v2/examples/auth/gen/auth/v1"
	userv1 "github.com/iodesystems/gwag/v2/examples/auth/gen/user/v1"
	"github.com/iodesystems/gwag/v2/gw/ir"
)

// TestInjectType_PointerProtoTRewrite confirms that the canonical
// proto-pointer T (e.g. *authv1.Context) produces a HideTypeRewrite
// keyed on the proto FullName and a non-nil Runtime.
func TestInjectType_PointerProtoTRewrite(t *testing.T) {
	tx := InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return &authv1.Context{}, nil
	})

	if got, want := len(tx.Schema), 1; got != want {
		t.Fatalf("len(Schema)=%d, want %d", got, want)
	}
	h, ok := tx.Schema[0].(hideTypeRewrite)
	if !ok {
		t.Fatalf("Schema[0] is %T, want HideTypeRewrite", tx.Schema[0])
	}
	if h.Name != "auth.v1.Context" {
		t.Fatalf("HideTypeRewrite.Name=%q, want %q", h.Name, "auth.v1.Context")
	}
	if tx.Runtime == nil {
		t.Fatalf("Runtime=nil; want non-nil for proto-message T")
	}
}

// TestInjectType_ValueProtoTRewrite confirms the value-shape T
// (authv1.Context, no pointer) resolves to the same proto FullName.
func TestInjectType_ValueProtoTRewrite(t *testing.T) {
	tx := InjectType[authv1.Context](func(_ context.Context, _ *authv1.Context) (authv1.Context, error) {
		return authv1.Context{}, nil
	})

	if got, want := len(tx.Schema), 1; got != want {
		t.Fatalf("len(Schema)=%d, want %d", got, want)
	}
	h := tx.Schema[0].(hideTypeRewrite)
	if h.Name != "auth.v1.Context" {
		t.Fatalf("HideTypeRewrite.Name=%q, want %q", h.Name, "auth.v1.Context")
	}
}

// TestInjectType_HideFalseSkipsSchemaRewrite confirms Hide(false)
// keeps the arg in the schema (no HideTypeRewrite emitted) but still
// wires runtime injection so the resolver runs and can inspect what
// the caller sent.
func TestInjectType_HideFalseSkipsSchemaRewrite(t *testing.T) {
	tx := InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return &authv1.Context{}, nil
	}, Hide(false))

	if got := len(tx.Schema); got != 0 {
		t.Fatalf("len(Schema)=%d, want 0 under Hide(false)", got)
	}
	if tx.Runtime == nil {
		t.Fatalf("Runtime=nil; want non-nil for proto-message T")
	}
}

// TestInjectType_NonProtoTSkipsRuntime confirms a Go-native T that
// isn't a proto message produces a schema rewrite (so the field is
// stripped from the SDL if it ever existed) and a nil Runtime
// (runtime injection is proto-shaped today).
func TestInjectType_NonProtoTSkipsRuntime(t *testing.T) {
	tx := InjectType[string](func(_ context.Context, _ *string) (string, error) {
		return "filled", nil
	})

	if got := len(tx.Schema); got != 1 {
		t.Fatalf("len(Schema)=%d, want 1", got)
	}
	if tx.Runtime != nil {
		t.Fatalf("Runtime non-nil; want nil for non-proto T")
	}
}

// TestInjectPath_SchemaRewriteHideTrue confirms InjectPath emits a
// HidePathRewrite under the default Hide(true).
func TestInjectPath_SchemaRewriteHideTrue(t *testing.T) {
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	})
	if got := len(tx.Schema); got != 1 {
		t.Fatalf("len(Schema)=%d, want 1", got)
	}
	h, ok := tx.Schema[0].(hidePathRewrite)
	if !ok {
		t.Fatalf("Schema[0] is %T, want HidePathRewrite", tx.Schema[0])
	}
	if h.Path != "user.GetMe.auth" {
		t.Fatalf("HidePathRewrite.Path=%q, want %q", h.Path, "user.GetMe.auth")
	}
	if tx.Runtime == nil {
		t.Fatal("Runtime=nil; want non-nil")
	}
}

// TestInjectPath_SchemaRewriteHideFalse confirms Hide(false) skips
// the schema rewrite entirely (the arg stays in the SDL).
func TestInjectPath_SchemaRewriteHideFalse(t *testing.T) {
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}, Hide(false))
	if got := len(tx.Schema); got != 0 {
		t.Fatalf("len(Schema)=%d, want 0 under Hide(false)", got)
	}
	if tx.Runtime == nil {
		t.Fatal("Runtime=nil; want non-nil")
	}
}

// TestInjectPath_MalformedPathYieldsNoRuntime confirms a path with
// the wrong number of segments produces a no-op Runtime (and a no-op
// HidePathRewrite — apply ignores it).
func TestInjectPath_MalformedPathYieldsNoRuntime(t *testing.T) {
	tx := InjectPath("user.GetMe", func(_ context.Context, _ any) (any, error) {
		return "x", nil
	})
	if tx.Runtime != nil {
		t.Fatal("Runtime non-nil for malformed path; want nil")
	}
}

// TestInjectPath_RuntimeFillsHiddenArg drives the runtime middleware
// against a synthesized GetMeRequest dynamic message under
// Hide(true). The auth field's name "auth" matches the path's arg
// segment; the resolver fills in a Context. Map keys are camelCase
// because the canonical-args bridge (argsToMessage / messageToMap)
// uses lowerCamel proto names.
func TestInjectPath_RuntimeFillsHiddenArg(t *testing.T) {
	called := 0
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, current any) (any, error) {
		called++
		if current != nil {
			t.Fatalf("Hide(true) resolver got non-nil current=%v", current)
		}
		return map[string]any{
			"userId":   "u_path",
			"tenantId": "t_path",
		}, nil
	})
	if tx.Runtime == nil {
		t.Fatal("Runtime is nil")
	}

	getMeDesc := (&userv1.GetMeRequest{}).ProtoReflect().Descriptor()
	dyn := dynamicpb.NewMessage(getMeDesc)

	ctx := withInjectCache(context.Background())
	ctx = withDispatchOpInfo(ctx, "user", "v1", "GetMe")

	chain := tx.Runtime(func(_ context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		return req, nil
	})
	out, err := chain(ctx, dyn)
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	if called != 1 {
		t.Fatalf("resolver called %d times, want 1", called)
	}
	outDyn := out.(*dynamicpb.Message)
	authFD := getMeDesc.Fields().ByName("auth")
	if !outDyn.Has(authFD) {
		t.Fatal("auth field not set after middleware ran")
	}
	subDyn := outDyn.Get(authFD).Message().Interface().(*dynamicpb.Message)
	authMD := subDyn.Descriptor()
	uid := subDyn.Get(authMD.Fields().ByName("user_id")).String()
	tid := subDyn.Get(authMD.Fields().ByName("tenant_id")).String()
	if uid != "u_path" {
		t.Fatalf("user_id=%q, want %q", uid, "u_path")
	}
	if tid != "t_path" {
		t.Fatalf("tenant_id=%q, want %q", tid, "t_path")
	}
}

// TestInjectPath_RuntimeRespectsCurrentHideFalse drives the middleware
// under Hide(false) with the field already populated and confirms the
// resolver receives the canonical-IR-typed current value and can
// transform it.
func TestInjectPath_RuntimeRespectsCurrentHideFalse(t *testing.T) {
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, current any) (any, error) {
		m, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("current is %T, want map[string]any", current)
		}
		uid, _ := m["userId"].(string)
		return map[string]any{
			"userId":   "rewrote_" + uid,
			"tenantId": m["tenantId"],
		}, nil
	}, Hide(false))

	getMeDesc := (&userv1.GetMeRequest{}).ProtoReflect().Descriptor()
	authFD := getMeDesc.Fields().ByName("auth")
	dyn := dynamicpb.NewMessage(getMeDesc)
	authSub := dynamicpb.NewMessage(authFD.Message())
	authSub.Set(authFD.Message().Fields().ByName("user_id"), protoreflect.ValueOfString("alice"))
	authSub.Set(authFD.Message().Fields().ByName("tenant_id"), protoreflect.ValueOfString("acme"))
	dyn.Set(authFD, protoreflect.ValueOfMessage(authSub))

	ctx := withInjectCache(context.Background())
	ctx = withDispatchOpInfo(ctx, "user", "v1", "GetMe")

	chain := tx.Runtime(func(_ context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		return req, nil
	})
	out, err := chain(ctx, dyn)
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	outDyn := out.(*dynamicpb.Message)
	subDyn := outDyn.Get(authFD).Message().Interface().(*dynamicpb.Message)
	uid := subDyn.Get(authFD.Message().Fields().ByName("user_id")).String()
	if uid != "rewrote_alice" {
		t.Fatalf("user_id=%q, want %q", uid, "rewrote_alice")
	}
}

// TestHidePathRewrite_StripsTargetedArg confirms the schema-half
// rewrite walks ir.Services and strips just the named arg from the
// matching op, leaving siblings (other args, other ops, other
// namespaces) untouched.
func TestHidePathRewrite_StripsTargetedArg(t *testing.T) {
	svcs := []*ir.Service{
		{
			Namespace: "user",
			Version:   "v1",
			Operations: []*ir.Operation{
				{
					Name: "GetMe",
					Args: []*ir.Arg{
						{Name: "auth"},
						{Name: "request_id"},
					},
				},
				{
					Name: "GetOther",
					Args: []*ir.Arg{
						{Name: "auth"},
					},
				},
			},
		},
		{
			Namespace: "billing",
			Version:   "v1",
			Operations: []*ir.Operation{
				{
					Name: "GetMe",
					Args: []*ir.Arg{
						{Name: "auth"},
					},
				},
			},
		},
	}

	hidePathRewrite{Path: "user.GetMe.auth"}.apply(svcs)

	if got := len(svcs[0].Operations[0].Args); got != 1 {
		t.Fatalf("user.GetMe.Args len=%d, want 1 after strip", got)
	}
	if svcs[0].Operations[0].Args[0].Name != "request_id" {
		t.Fatalf("user.GetMe sibling arg=%q, want request_id", svcs[0].Operations[0].Args[0].Name)
	}
	if got := len(svcs[0].Operations[1].Args); got != 1 {
		t.Fatalf("user.GetOther.Args len=%d; want untouched", got)
	}
	if got := len(svcs[1].Operations[0].Args); got != 1 {
		t.Fatalf("billing.GetMe.Args len=%d; cross-namespace strip is wrong", got)
	}
}

// TestInjectPath_NonMatchingOpSkipped confirms the middleware passes
// through cleanly when the dispatch op-info doesn't match the path's
// (namespace, op).
func TestInjectPath_NonMatchingOpSkipped(t *testing.T) {
	called := 0
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		called++
		return map[string]any{}, nil
	})

	getMeDesc := (&userv1.GetMeRequest{}).ProtoReflect().Descriptor()
	dyn := dynamicpb.NewMessage(getMeDesc)

	ctx := withInjectCache(context.Background())
	ctx = withDispatchOpInfo(ctx, "billing", "v1", "ListInvoices")

	chain := tx.Runtime(func(_ context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		return req, nil
	})
	if _, err := chain(ctx, dyn); err != nil {
		t.Fatalf("middleware: %v", err)
	}
	if called != 0 {
		t.Fatalf("resolver called %d times for non-matching op; want 0", called)
	}
}

// TestNullable_TypeRewriteFlipsRequired confirms NullableTypeRewrite
// walks ir.Services and clears Required on every arg/field whose
// named type matches.
func TestNullable_TypeRewriteFlipsRequired(t *testing.T) {
	svcs := []*ir.Service{
		{
			Namespace: "user",
			Version:   "v1",
			Types: map[string]*ir.Type{
				"user.v1.GetMeRequest": {
					Name:     "user.v1.GetMeRequest",
					TypeKind: ir.TypeInput,
					Fields: []*ir.Field{
						{Name: "auth", Type: ir.TypeRef{Named: "auth.v1.Context"}, Required: true},
						{Name: "trace_id", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true},
					},
				},
			},
			Operations: []*ir.Operation{
				{
					Name: "GetMe",
					Args: []*ir.Arg{
						{Name: "auth", Type: ir.TypeRef{Named: "auth.v1.Context"}, Required: true},
						{Name: "name", Type: ir.TypeRef{Builtin: ir.ScalarString}, Required: true},
					},
				},
			},
		},
	}
	nullableTypeRewrite{Name: "auth.v1.Context"}.apply(svcs)

	t0 := svcs[0].Types["user.v1.GetMeRequest"]
	if t0.Fields[0].Required {
		t.Fatal("expected auth field Required=false after Nullable rewrite")
	}
	if !t0.Fields[1].Required {
		t.Fatal("trace_id (non-matching type) should remain Required")
	}
	op0 := svcs[0].Operations[0]
	if op0.Args[0].Required {
		t.Fatal("expected auth arg Required=false after Nullable rewrite")
	}
	if !op0.Args[1].Required {
		t.Fatal("name (non-matching type) should remain Required")
	}
}

// TestNullable_PathRewriteFlipsRequired confirms NullablePathRewrite
// targets just one (ns, op, arg) tuple.
func TestNullable_PathRewriteFlipsRequired(t *testing.T) {
	svcs := []*ir.Service{
		{
			Namespace: "user",
			Version:   "v1",
			Operations: []*ir.Operation{
				{
					Name: "GetMe",
					Args: []*ir.Arg{
						{Name: "auth", Required: true},
						{Name: "name", Required: true},
					},
				},
				{
					Name: "GetOther",
					Args: []*ir.Arg{
						{Name: "auth", Required: true},
					},
				},
			},
		},
	}
	nullablePathRewrite{Path: "user.GetMe.auth"}.apply(svcs)

	if svcs[0].Operations[0].Args[0].Required {
		t.Fatal("user.GetMe.auth should be Required=false")
	}
	if !svcs[0].Operations[0].Args[1].Required {
		t.Fatal("user.GetMe.name should remain Required")
	}
	if !svcs[0].Operations[1].Args[0].Required {
		t.Fatal("user.GetOther.auth should remain Required (different op)")
	}
}

// TestInjectType_NullableHideFalseEmitsBothRewrites confirms the
// composition: Hide(false) skips HideTypeRewrite, Nullable(true)
// adds NullableTypeRewrite. Schema half is exactly the Nullable.
func TestInjectType_NullableHideFalseEmitsBothRewrites(t *testing.T) {
	tx := InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return &authv1.Context{}, nil
	}, Hide(false), Nullable(true))
	if got := len(tx.Schema); got != 1 {
		t.Fatalf("len(Schema)=%d, want 1 (just the Nullable)", got)
	}
	n, ok := tx.Schema[0].(nullableTypeRewrite)
	if !ok {
		t.Fatalf("Schema[0] is %T, want NullableTypeRewrite", tx.Schema[0])
	}
	if n.Name != "auth.v1.Context" {
		t.Fatalf("NullableTypeRewrite.Name=%q", n.Name)
	}
}

// TestInjectType_NullableHideTruePanic asserts the registration
// rejects the bad combo at the user's call site.
func TestInjectType_NullableHideTruePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for Hide(true) + Nullable(true)")
		}
	}()
	_ = InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return nil, nil
	}, Nullable(true))
}

// TestInjectPath_NullableHideTruePanic mirrors the InjectType case.
func TestInjectPath_NullableHideTruePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for Hide(true) + Nullable(true)")
		}
	}()
	_ = InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}, Nullable(true))
}

// TestInjectPath_NullableHideFalseEmitsBothRewrites — composition
// surface mirroring the type case.
func TestInjectPath_NullableHideFalseEmitsBothRewrites(t *testing.T) {
	tx := InjectPath("user.GetMe.auth", func(_ context.Context, _ any) (any, error) {
		return nil, nil
	}, Hide(false), Nullable(true))
	if got := len(tx.Schema); got != 1 {
		t.Fatalf("len(Schema)=%d, want 1", got)
	}
	n, ok := tx.Schema[0].(nullablePathRewrite)
	if !ok {
		t.Fatalf("Schema[0] is %T, want NullablePathRewrite", tx.Schema[0])
	}
	if n.Path != "user.GetMe.auth" {
		t.Fatalf("NullablePathRewrite.Path=%q", n.Path)
	}
}

// TestInjectType_RuntimeFillsHiddenField runs the InjectType runtime
// middleware against a synthesized GetMeRequest dynamic message and
// confirms the auth field gets populated by the resolver.
func TestInjectType_RuntimeFillsHiddenField(t *testing.T) {
	tx := InjectType[*authv1.Context](func(_ context.Context, _ **authv1.Context) (*authv1.Context, error) {
		return &authv1.Context{UserId: "u_resolved", TenantId: "t_resolved"}, nil
	})
	if tx.Runtime == nil {
		t.Fatal("Runtime is nil")
	}

	getMeDesc := (&userv1.GetMeRequest{}).ProtoReflect().Descriptor()
	dyn := dynamicpb.NewMessage(getMeDesc)

	ctx := withInjectCache(context.Background())

	called := false
	chain := tx.Runtime(func(_ context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		called = true
		return req, nil
	})
	out, err := chain(ctx, dyn)
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	if !called {
		t.Fatal("next handler not called")
	}
	outDyn := out.(*dynamicpb.Message)

	authFD := getMeDesc.Fields().ByName("auth")
	if authFD == nil {
		t.Fatal("auth field not found on GetMeRequest descriptor")
	}
	if !outDyn.Has(authFD) {
		t.Fatal("auth field not set after middleware ran")
	}

	subDyn := outDyn.Get(authFD).Message().Interface()
	b, err := proto.Marshal(subDyn)
	if err != nil {
		t.Fatalf("marshal sub: %v", err)
	}
	var got authv1.Context
	if err := proto.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal sub: %v", err)
	}
	if got.GetUserId() != "u_resolved" {
		t.Fatalf("UserId=%q, want %q", got.GetUserId(), "u_resolved")
	}
	if got.GetTenantId() != "t_resolved" {
		t.Fatalf("TenantId=%q, want %q", got.GetTenantId(), "t_resolved")
	}
}

// TestInjectHeader_DefaultsToHideTrue confirms the constructor stamps
// Hide=true by default and surfaces only the Headers slot — no
// Schema or Runtime halves.
func TestInjectHeader_DefaultsToHideTrue(t *testing.T) {
	tx := InjectHeader("X-Source-IP", func(_ context.Context, _ *string) (string, error) {
		return "1.2.3.4", nil
	})
	if len(tx.Schema) != 0 {
		t.Fatalf("len(Schema)=%d, want 0 (headers aren't in the GraphQL schema)", len(tx.Schema))
	}
	if tx.Runtime != nil {
		t.Fatal("Runtime non-nil; want nil for InjectHeader")
	}
	if got := len(tx.headers); got != 1 {
		t.Fatalf("len(Headers)=%d, want 1", got)
	}
	h := tx.headers[0]
	if h.Name != "X-Source-IP" {
		t.Fatalf("Name=%q, want X-Source-IP", h.Name)
	}
	if !h.Hide {
		t.Fatal("Hide=false; want true (default)")
	}
	if h.Fn == nil {
		t.Fatal("Fn=nil")
	}
}

// TestInjectHeader_HideFalsePropagates confirms Hide(false) opt-in
// reaches the HeaderInjector unchanged.
func TestInjectHeader_HideFalsePropagates(t *testing.T) {
	tx := InjectHeader("X-Caller", func(_ context.Context, _ *string) (string, error) {
		return "", nil
	}, Hide(false))
	if tx.headers[0].Hide {
		t.Fatal("Hide=true; want false")
	}
}

// TestInjectHeader_EmptyNamePanics — the name is the routing key for
// both HTTP and gRPC metadata; empty is meaningless and should fail
// loudly at registration.
func TestInjectHeader_EmptyNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("InjectHeader(\"\") did not panic")
		}
	}()
	_ = InjectHeader("", func(_ context.Context, _ *string) (string, error) { return "", nil })
}

// TestInjectHeader_NullableRejected — Nullable applies to schema
// rewrites; headers aren't in the schema, so this is a misuse worth
// surfacing at the call site.
func TestInjectHeader_NullableRejected(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("InjectHeader+Nullable(true) did not panic")
		}
	}()
	_ = InjectHeader("X-Foo", func(_ context.Context, _ *string) (string, error) { return "", nil }, Nullable(true))
}

// TestApplyHeaderInjectors_SkipsEmptyResolverResults — returning ""
// from the resolver opts out of writing the header, mirroring the
// "skip" intent (HTTP gives empty headers no useful semantics).
func TestApplyHeaderInjectors_SkipsEmptyResolverResults(t *testing.T) {
	injectors := []headerInjector{
		{Name: "X-Keep", Hide: true, Fn: func(_ context.Context, _ *string) (string, error) { return "yes", nil }},
		{Name: "X-Skip", Hide: true, Fn: func(_ context.Context, _ *string) (string, error) { return "", nil }},
	}
	out, err := applyHeaderInjectors(withInjectCache(context.Background()), injectors)
	if err != nil {
		t.Fatalf("applyHeaderInjectors: %v", err)
	}
	if out["X-Keep"] != "yes" {
		t.Fatalf("X-Keep=%q, want yes", out["X-Keep"])
	}
	if _, ok := out["X-Skip"]; ok {
		t.Fatalf("X-Skip should be omitted, got %q", out["X-Skip"])
	}
}

// TestApplyHeaderInjectors_HideFalseSeesInboundHeader — under
// Hide(false), the resolver receives a *string pointing at the
// inbound HTTP request's header value (when present).
func TestApplyHeaderInjectors_HideFalseSeesInboundHeader(t *testing.T) {
	var seen *string
	injectors := []headerInjector{{
		Name: "X-Caller",
		Hide: false,
		Fn: func(_ context.Context, current *string) (string, error) {
			seen = current
			if current != nil {
				return "echo:" + *current, nil
			}
			return "", nil
		},
	}}

	r, _ := http.NewRequest("POST", "/", nil)
	r.Header.Set("X-Caller", "alice")
	ctx := withHTTPRequest(withInjectCache(context.Background()), r)

	out, err := applyHeaderInjectors(ctx, injectors)
	if err != nil {
		t.Fatalf("applyHeaderInjectors: %v", err)
	}
	if seen == nil {
		t.Fatal("Hide(false) resolver did not see inbound header")
	}
	if out["X-Caller"] != "echo:alice" {
		t.Fatalf("X-Caller=%q", out["X-Caller"])
	}
}
