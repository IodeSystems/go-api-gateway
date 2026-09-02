package gat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

type listProjectsInput struct {
	Limit int `query:"limit"`
}

type listProjectsOutput struct {
	Body struct {
		Projects []project `json:"projects"`
	}
}

type project struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type getProjectInput struct {
	ID string `path:"id"`
}

type getProjectOutput struct {
	Body project
}

func TestPairedRegister_GraphQLAndREST(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)

	gat.Register(api, g, huma.Operation{
		OperationID: "listProjects",
		Method:      http.MethodGet,
		Path:        "/projects",
	}, func(ctx context.Context, in *listProjectsInput) (*listProjectsOutput, error) {
		out := &listProjectsOutput{}
		out.Body.Projects = []project{
			{ID: "p1", Name: "First"},
			{ID: "p2", Name: "Second"},
		}
		if in.Limit > 0 && in.Limit < len(out.Body.Projects) {
			out.Body.Projects = out.Body.Projects[:in.Limit]
		}
		return out, nil
	})

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

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// REST path still works (huma's native surface).
	t.Run("rest", func(t *testing.T) {
		body := mustGET(t, srv.URL+"/projects/p1")
		var got project
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode: %v body=%s", err, body)
		}
		if got.ID != "p1" || got.Name != "Project p1" {
			t.Errorf("got %+v", got)
		}
	})

	// GraphQL path dispatches in-process to the same handler.
	t.Run("graphql_get", func(t *testing.T) {
		resp := mustGraphQL(t, srv.URL+"/api/graphql",
			`query { Demo { getProject(id: "p1") { id name } } }`)
		path := digPath(resp, "data", "Demo", "getProject")
		if path == nil {
			t.Fatalf("missing getProject: %v", resp)
		}
		m := path.(map[string]any)
		if m["id"] != "p1" || m["name"] != "Project p1" {
			t.Errorf("got %v", m)
		}
	})

	t.Run("graphql_list", func(t *testing.T) {
		resp := mustGraphQL(t, srv.URL+"/api/graphql",
			`query { Demo { listProjects { projects { id } } } }`)
		path := digPath(resp, "data", "Demo", "listProjects", "projects")
		if path == nil {
			t.Fatalf("missing projects: %v", resp)
		}
		got := path.([]any)
		if len(got) != 2 {
			t.Errorf("expected 2 projects, got %d", len(got))
		}
	})

	t.Run("schema_graphql_sdl", func(t *testing.T) {
		body := mustGET(t, srv.URL+"/api/schema/graphql")
		s := string(body)
		if !strings.Contains(s, "getProject") || !strings.Contains(s, "listProjects") {
			t.Errorf("SDL missing operations: %s", s)
		}
	})

	t.Run("schema_graphql_introspect", func(t *testing.T) {
		body := mustGET(t, srv.URL+"/api/schema/graphql?format=json")
		var doc map[string]any
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("introspect decode: %v body=%s", err, body)
		}
		if doc["data"] == nil {
			t.Errorf("introspection result missing data: %v", doc)
		}
	})

	// graphql-codegen introspects via POST. The schema URL must answer it
	// (RegisterHuma path) so codegen can target the documented endpoint.
	t.Run("schema_graphql_introspect_post", func(t *testing.T) {
		resp := mustGraphQL(t, srv.URL+"/api/schema/graphql",
			`{ __schema { queryType { name } } }`)
		if name := digPath(resp, "data", "__schema", "queryType", "name"); name == nil {
			t.Fatalf("POST introspection to schema URL returned no queryType: %v", resp)
		}
	})
}

func TestRegister_AfterFinalizeIsNoOp(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		// No captured ops + no services — build will error. That's OK
		// here: the second RegisterHuma below should fail with
		// "already finalized" if the first one *did* succeed. We're
		// really testing the second branch.
		t.Logf("first RegisterHuma errored as expected: %v", err)
	}

	if err := gat.RegisterHuma(api, g, "/api"); err == nil {
		t.Errorf("expected second RegisterHuma to error")
	}
}

func mustNewGat(t *testing.T) *gat.Gateway {
	t.Helper()
	g, err := gat.New()
	if err != nil {
		t.Fatalf("gat.New: %v", err)
	}
	return g
}

func mustGET(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	return b
}

func mustGraphQL(t *testing.T, url, query string) map[string]any {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"query": query})
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode %s: %v body=%s", url, err, b)
	}
	if errs, ok := out["errors"]; ok && errs != nil {
		t.Logf("graphql errors: %v", errs)
	}
	return out
}

func digPath(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}
