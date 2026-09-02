package gat_test

// End-to-end coverage for what the GraphQL ingress puts on the wire
// when a query goes wrong.
//
// gat serves GraphQL through a plan cache plus append-mode execution,
// which writes field-level errors into the response bytes rather than
// returning them for the transport to format. Two things have to hold
// across that path and neither is visible from a unit test of the
// error types:
//
//   - the HTTP status is 200 for any query that reached execution,
//     matching gw/ and GraphQL-over-HTTP — an errors envelope is a
//     successful exchange, not a failed request;
//   - statusExtendedError's `extensions` survive into the envelope, so
//     a client can still tell a 401 from a 500 without string-matching
//     the message.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

// errGateway mounts one operation that always fails with the given
// huma status, so the test can drive a specific error class.
func errGateway(t *testing.T, status int) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Errs", "1.0.0"))
	g := mustNewGat(t)

	gat.Register(api, g, huma.Operation{
		OperationID: "getProject",
		Method:      http.MethodGet,
		Path:        "/projects/{id}",
	}, func(ctx context.Context, in *getProjectInput) (*getProjectOutput, error) {
		if in.ID == "ok" {
			return &getProjectOutput{Body: project{ID: "ok", Name: "fine"}}, nil
		}
		return nil, huma.NewError(status, "handler said no")
	})

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}
	return mux
}

// postGraphQL fires a query and returns the status plus decoded body.
func postGraphQL(t *testing.T, h http.Handler, query string) (int, map[string]any) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"query": query})
	req := httptest.NewRequest(http.MethodPost, "/api/graphql", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, body)
	}
	return rec.Code, out
}

// firstError returns the first entry of the response's errors array.
func firstError(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	errs, _ := resp["errors"].([]any)
	if len(errs) == 0 {
		t.Fatalf("expected an errors array, got %v", resp)
	}
	e, ok := errs[0].(map[string]any)
	if !ok {
		t.Fatalf("errors[0] is not an object: %v", errs[0])
	}
	return e
}

func TestGraphQL_HandlerError_Is200WithExtensions(t *testing.T) {
	h := errGateway(t, http.StatusUnauthorized)

	code, resp := postGraphQL(t, h, `{ Errs { getProject(id: "nope") { id } } }`)
	if code != http.StatusOK {
		t.Errorf("status = %d; want 200 — a resolved-to-errors query is a successful exchange", code)
	}

	ext, ok := firstError(t, resp)["extensions"].(map[string]any)
	if !ok {
		t.Fatalf("error carries no extensions: %v", resp["errors"])
	}
	// JSON numbers decode as float64.
	if got, want := ext["status"], float64(http.StatusUnauthorized); got != want {
		t.Errorf("extensions.status = %v; want %v", got, want)
	}
	if got := ext["code"]; got != "unauthenticated" {
		t.Errorf("extensions.code = %v; want unauthenticated", got)
	}
}

func TestGraphQL_ServerError_CarriesStatusExtension(t *testing.T) {
	h := errGateway(t, http.StatusInternalServerError)

	code, resp := postGraphQL(t, h, `{ Errs { getProject(id: "nope") { id } } }`)
	if code != http.StatusOK {
		t.Errorf("status = %d; want 200", code)
	}
	ext, ok := firstError(t, resp)["extensions"].(map[string]any)
	if !ok {
		t.Fatalf("error carries no extensions: %v", resp["errors"])
	}
	if got, want := ext["status"], float64(http.StatusInternalServerError); got != want {
		t.Errorf("extensions.status = %v; want %v", got, want)
	}
}

func TestGraphQL_ValidationError_Is200(t *testing.T) {
	h := errGateway(t, http.StatusNotFound)

	// No such field — fails validation, so no plan is produced and the
	// response comes from the pre-execution branch.
	code, resp := postGraphQL(t, h, `{ Errs { noSuchField } }`)
	if code != http.StatusOK {
		t.Errorf("status = %d; want 200", code)
	}
	if _, ok := resp["errors"].([]any); !ok {
		t.Fatalf("expected an errors array for an invalid query, got %v", resp)
	}
}

func TestGraphQL_RepeatedQueryHitsPlanCache(t *testing.T) {
	h := errGateway(t, http.StatusNotFound)
	const q = `{ Errs { getProject(id: "ok") { id name } } }`

	// The second execution runs off the cached plan. Same bytes out is
	// the observable contract — a stale or mutated plan would show up
	// as a diverging response.
	_, first := postGraphQL(t, h, q)
	code, second := postGraphQL(t, h, q)
	if code != http.StatusOK {
		t.Fatalf("status = %d; want 200", code)
	}
	if errs, ok := second["errors"]; ok {
		t.Fatalf("unexpected errors on a valid query: %v", errs)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Errorf("cached-plan response differs\nfirst:  %s\nsecond: %s", a, b)
	}
	if name := digPath(second, "data", "Errs", "getProject", "name"); name != "fine" {
		t.Errorf("data.Errs.getProject.name = %v; want %q", name, "fine")
	}
}
