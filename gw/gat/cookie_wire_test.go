package gat_test

// Set-Cookie must survive the GraphQL ingress, not just the REST one.
//
// cookies_test.go already covers the sink in isolation: given an output
// struct, it extracts the right cookies. That test cannot fail for the
// bug this file exists to catch, because the bug was in WHEN the sink is
// emitted, not in what it extracts.
//
// The regression: Handler() called cookies.emit(w) after building the
// query plan but BEFORE executing it. Resolvers are what populate the
// sink (dispatch calls sink.addFromOutput), so emit ran against an empty
// sink and every Set-Cookie was dropped. Nothing failed loudly — the
// operation still returned its data and a 200 — so a login mutation
// reported success while setting no session. It shipped because the
// isolated sink test stayed green and the REST surface, which does not
// go through this path, kept working.
//
// Hence both surfaces are asserted here from one handler. If they ever
// disagree again, the failure names which one broke — that comparison is
// what localised the original bug after a long hunt.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

type startSessionInput struct {
	ID string `path:"id"`
}

type startSessionOutput struct {
	SetCookie http.Cookie `header:"Set-Cookie"`
	Body      struct {
		Code string `json:"code"`
	}
}

// cookieGateway mounts one login-shaped operation: it succeeds AND sets a
// session cookie, the combination that hides a dropped header.
func cookieGateway(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Cook", "1.0.0"))
	g := mustNewGat(t)

	gat.Register(api, g, huma.Operation{
		OperationID: "startSession",
		Method:      http.MethodGet,
		Path:        "/sessions/{id}",
	}, func(_ context.Context, in *startSessionInput) (*startSessionOutput, error) {
		out := &startSessionOutput{
			SetCookie: http.Cookie{
				Name: "token", Value: "sess-" + in.ID, Path: "/", HttpOnly: true,
			},
		}
		out.Body.Code = "OK"
		return out, nil
	})

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}
	return mux
}

func TestSetCookie_SurvivesGraphQLIngress(t *testing.T) {
	h := cookieGateway(t)

	// REST first — the control. If this fails the fixture is wrong, not
	// the ingress, and the GraphQL assertion below would be misleading.
	restRec := httptest.NewRecorder()
	h.ServeHTTP(restRec, httptest.NewRequest(http.MethodGet, "/sessions/abc", nil))
	restCookie := restRec.Result().Header.Get("Set-Cookie")
	if restCookie == "" {
		t.Fatalf("REST set no cookie (status %d) — fixture is broken, fix it before reading the GraphQL case", restRec.Code)
	}

	// GraphQL: same handler, same process, same cookie expected.
	payload, _ := json.Marshal(map[string]any{
		"query": `{ Cook { startSession(id: "abc") { code } } }`,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/graphql", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	gqlRec := httptest.NewRecorder()
	h.ServeHTTP(gqlRec, req)

	if gqlRec.Code != http.StatusOK {
		t.Fatalf("graphql status = %d; want 200", gqlRec.Code)
	}

	cookies := gqlRec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("GraphQL dropped Set-Cookie while REST kept it (%q).\n"+
			"The op still succeeded, which is what makes this silent: a login "+
			"mutation returns OK and sets no session.\n"+
			"Check that Handler() emits the sink AFTER executing the plan — "+
			"resolvers are what fill it.\nbody=%s", restCookie, gqlRec.Body)
	}
	if got, want := cookies[0].Name, "token"; got != want {
		t.Errorf("cookie name = %q; want %q", got, want)
	}
	if got, want := cookies[0].Value, "sess-abc"; got != want {
		t.Errorf("cookie value = %q; want %q — the value must come from the resolver that ran, "+
			"not from a zero-valued output", got, want)
	}

	// The body still has to be intact: the fix reorders header emission
	// around body assembly, so a regression could produce a cookie and a
	// truncated or empty payload.
	var out map[string]any
	if err := json.Unmarshal(gqlRec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode graphql body: %v body=%s", err, gqlRec.Body)
	}
	data, _ := out["data"].(map[string]any)
	cook, _ := data["Cook"].(map[string]any)
	sess, _ := cook["startSession"].(map[string]any)
	if sess == nil || sess["code"] != "OK" {
		t.Errorf("graphql data = %v; want startSession.code = OK", out["data"])
	}
}
