package gat_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

// huma skips UNEXPORTED fields when it discovers request parameters, and an
// embedded field takes its name from its type — so embedding an unexported type
// hides every parameter inside it. Embedding is fine; exportedness is the rule.
// The hidden parameter is absent from the OpenAPI document and never bound, so
// gat refuses to mount rather than let that reach production.

// unexported type name ⇒ unexported embedded field ⇒ invisible to huma
type embeddedScope struct {
	Dir string `query:"dir"`
	Now string `query:"now"`
}

// ExportedScope embeds cleanly: huma walks exported embedded structs.
type ExportedScope struct {
	Dir string `query:"dir"`
	Now string `query:"now"`
}

type exportedEmbedInput struct {
	ExportedScope
	Spec string `query:"spec"`
}

// An exported embed nested inside another exported embed is still reachable.
type OuterExported struct{ ExportedScope }

type deepExportedInput struct {
	OuterExported
	Spec string `query:"spec"`
}

// An exported embed underneath an UNexported one is still unreachable.
type hiddenOuter struct{ ExportedScope }

type hiddenOuterInput struct {
	hiddenOuter
	Spec string `query:"spec"`
}

type embeddedParamInput struct {
	embeddedScope
	Spec string `query:"spec"`
}

type flatParamInput struct {
	Dir  string `query:"dir"`
	Now  string `query:"now"`
	Spec string `query:"spec"`
}

type deepEmbedOuter struct{ embeddedScope }

type deepEmbedInput struct {
	deepEmbedOuter
	Spec string `query:"spec"`
}

type headerEmbed struct {
	Trace string `header:"X-Trace"`
}

type headerEmbedInput struct {
	headerEmbed
}

// bodyOnlyEmbed carries no parameter tags, so embedding it is fine — the check
// must not fire on ordinary struct reuse.
type bodyOnlyEmbed struct {
	Ignored string `json:"ignored"`
}

type bodyOnlyEmbedInput struct {
	bodyOnlyEmbed
	Spec string `query:"spec"`
}

type okOutput struct {
	Body struct {
		OK bool `json:"ok"`
	}
}

func mountWith[I any](t *testing.T, tweak func(*gat.Gateway)) error {
	t.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)
	if tweak != nil {
		tweak(g)
	}
	gat.Register(api, g, huma.Operation{
		OperationID: "probe", Method: http.MethodGet, Path: "/probe",
	}, func(ctx context.Context, _ *I) (*okOutput, error) { return &okOutput{}, nil })
	return gat.RegisterHuma(api, g, "/api")
}

func TestUnexportedEmbedRefusesToMount(t *testing.T) {
	err := mountWith[embeddedParamInput](t, nil)
	if err == nil {
		t.Fatal("a parameter inside an unexported embed must not mount silently")
	}
	for _, want := range []string{"embeddedScope", "dir", "now", "export the type or field"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), `"spec"`) {
		t.Errorf("spec is declared directly and must not be reported:\n%s", err)
	}
}

func TestFlatParamsMountCleanly(t *testing.T) {
	if err := mountWith[flatParamInput](t, nil); err != nil {
		t.Fatalf("directly-declared params must mount: %v", err)
	}
}

// An embed inside an embed is just as invisible to huma.
func TestNestedUnexportedEmbedIsFound(t *testing.T) {
	err := mountWith[deepEmbedInput](t, nil)
	if err == nil {
		t.Fatal("a parameter two unexported embeds deep must still be reported")
	}
	if !strings.Contains(err.Error(), "deepEmbedOuter.embeddedScope") {
		t.Errorf("error should name the embed path:\n%s", err)
	}
}

// The rule is exportedness, not embedding: huma walks exported embeds, so gat
// must not reject them. Rejecting working code is worse than the original trap.
func TestExportedEmbedMountsCleanly(t *testing.T) {
	if err := mountWith[exportedEmbedInput](t, nil); err != nil {
		t.Fatalf("an exported embed is handled by huma and must mount: %v", err)
	}
	if err := mountWith[deepExportedInput](t, nil); err != nil {
		t.Fatalf("nested exported embeds must mount: %v", err)
	}
}

// Exportedness does not recover once the walk passes through an unexported embed.
func TestExportedEmbedBeneathUnexportedIsStillHidden(t *testing.T) {
	err := mountWith[hiddenOuterInput](t, nil)
	if err == nil {
		t.Fatal("an exported embed under an unexported one is still unreachable")
	}
	if !strings.Contains(err.Error(), "unexported embedded struct") {
		t.Errorf("error should say why it is unreachable:\n%s", err)
	}
}

func TestEmbeddedHeaderParamIsFound(t *testing.T) {
	err := mountWith[headerEmbedInput](t, nil)
	if err == nil || !strings.Contains(err.Error(), "X-Trace") {
		t.Fatalf("header params count too, got %v", err)
	}
}

// Embedding a struct that carries no parameter tags is ordinary Go reuse and
// must not trip the check.
func TestEmbeddedStructWithoutParamTagsIsFine(t *testing.T) {
	if err := mountWith[bodyOnlyEmbedInput](t, nil); err != nil {
		t.Fatalf("an embed with no param tags must mount: %v", err)
	}
}

// An ordinary unexported field with a param tag is dropped by the same rule.
func TestUnexportedFieldParamIsFound(t *testing.T) {
	err := gat.CheckEmbeddedParams(reflect.TypeFor[struct {
		hidden string `query:"hidden"`
		Spec   string `query:"spec"`
	}]())
	if err == nil || !strings.Contains(err.Error(), "the field is unexported") {
		t.Fatalf("an unexported field with a param tag must be reported, got %v", err)
	}
}

func TestCheckEmbeddedParamsPassesCleanInputs(t *testing.T) {
	if err := gat.CheckEmbeddedParams(
		reflect.TypeFor[flatParamInput](),
		reflect.TypeFor[exportedEmbedInput](),
	); err != nil {
		t.Fatalf("clean inputs must pass: %v", err)
	}
}

func TestAllowEmbeddedParamsDowngradesToWarning(t *testing.T) {
	err := mountWith[embeddedParamInput](t, func(g *gat.Gateway) { g.AllowEmbeddedParams(true) })
	if err != nil {
		t.Fatalf("AllowEmbeddedParams(true) must mount: %v", err)
	}
}

// The check is a diagnostic for a real defect, so prove the defect from both
// sides: huma drops the UNEXPORTED embed and handles the EXPORTED one. If huma
// ever starts reading unexported fields, this test fails and the check can go.
func TestHumaDropsUnexportedButHandlesExported(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	var got embeddedParamInput
	huma.Register(api, huma.Operation{
		OperationID: "probe", Method: http.MethodGet, Path: "/probe",
	}, func(ctx context.Context, in *embeddedParamInput) (*okOutput, error) {
		got = *in
		return &okOutput{}, nil
	})

	doc, err := api.OpenAPI().MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(doc), `"name":"dir"`) {
		t.Fatal("huma now documents embedded params — the gat check can be removed")
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/probe?dir=D&now=N&spec=S", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("probe: %d", rec.Code)
	}
	if got.Spec != "S" {
		t.Fatalf("directly-declared param must bind, got %q", got.Spec)
	}
	if got.Dir != "" || got.Now != "" {
		t.Fatalf("huma now binds params under an unexported embed (dir=%q now=%q) — the gat check can be removed",
			got.Dir, got.Now)
	}

	// And the exported spelling works, which is the documented fix.
	mux2 := http.NewServeMux()
	api2 := humago.New(mux2, huma.DefaultConfig("Demo", "1.0.0"))
	var ok exportedEmbedInput
	huma.Register(api2, huma.Operation{
		OperationID: "probe2", Method: http.MethodGet, Path: "/probe2",
	}, func(ctx context.Context, in *exportedEmbedInput) (*okOutput, error) {
		ok = *in
		return &okOutput{}, nil
	})
	doc2, err := api2.OpenAPI().MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc2), `"dir"`) {
		t.Fatal("an exported embed must be documented")
	}
	rec2 := httptest.NewRecorder()
	mux2.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/probe2?dir=D&now=N&spec=S", nil))
	if ok.Dir != "D" || ok.Now != "N" || ok.Spec != "S" {
		t.Fatalf("an exported embed must bind, got %+v", ok)
	}
}

// huma promotes parameters out of an exported embedded struct, so gat's binder
// has to follow the field path there. Declaring the arg in the schema but never
// binding it is the same silent zero-value failure, one layer down.
func TestExportedEmbedBindsThroughGraphQL(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)

	var got exportedEmbedInput
	gat.Register(api, g, huma.Operation{
		OperationID: "probe", Method: http.MethodGet, Path: "/probe",
	}, func(ctx context.Context, in *exportedEmbedInput) (*okOutput, error) {
		got = *in
		out := &okOutput{}
		out.Body.OK = true
		return out, nil
	})
	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}

	body := `{"query":"{ Demo { probe(dir:\"D\", now:\"N\", spec:\"S\") { ok } } }"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("graphql: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"errors"`) {
		t.Fatalf("graphql errors: %s", rec.Body.String())
	}
	if got.Spec != "S" {
		t.Fatalf("directly-declared arg must bind, got %q", got.Spec)
	}
	if got.Dir != "D" || got.Now != "N" {
		t.Fatalf("args promoted out of an exported embed must bind, got dir=%q now=%q", got.Dir, got.Now)
	}
}
