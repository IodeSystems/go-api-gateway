// gat example server.
//
// One huma source of truth → three typed surfaces out:
//
//	REST     (huma's native)          GET    /projects, /projects/{id}
//	GraphQL  (gat)                    POST   /api/graphql
//	gRPC     (gat, via connect-go)    POST   /api/grpc/{service}/{method}
//
//	Schema views (gat)
//	GET /api/schema/graphql           SDL or introspection JSON
//	GET /api/schema/proto             FileDescriptorSet (binary)
//	GET /api/schema/openapi           re-emitted OpenAPI document
//
// The handlers are written once with huma.Operation + a typed
// (*Input) → (*Output, error) function. Swapping `huma.Register` for
// `gat.Register` is the only change needed to surface an op via the
// other transports.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

type Project struct {
	ID   string `json:"id" doc:"Stable project identifier."`
	Name string `json:"name" doc:"Human-readable project name."`
	Tags []string `json:"tags,omitempty" doc:"Optional labels."`
}

// In-memory store. Real apps would back this with a DB.
var (
	storeMu sync.RWMutex
	store   = map[string]*Project{
		"alpha": {ID: "alpha", Name: "Alpha", Tags: []string{"core"}},
		"beta":  {ID: "beta", Name: "Beta", Tags: []string{"experimental"}},
	}
)

type ListProjectsInput struct {
	Limit int `query:"limit" doc:"Maximum projects to return." default:"50"`
}

type ListProjectsOutput struct {
	Body struct {
		Projects []*Project `json:"projects"`
	}
}

type GetProjectInput struct {
	ID string `path:"id" doc:"Project identifier."`
}

type GetProjectOutput struct {
	Body Project
}

type CreateProjectInput struct {
	Body struct {
		ID   string   `json:"id" doc:"Project identifier (unique)."`
		Name string   `json:"name" doc:"Human-readable name."`
		Tags []string `json:"tags,omitempty"`
	}
}

type CreateProjectOutput struct {
	Body Project
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Projects", "1.0.0"))
	g, err := gat.New()
	if err != nil {
		log.Fatalf("gat.New: %v", err)
	}

	// Each gat.Register call registers the operation with huma (so
	// REST/OpenAPI works) AND captures the handler ref for in-process
	// dispatch when GraphQL or gRPC come in for the same op.
	gat.Register(api, g, huma.Operation{
		OperationID: "listProjects",
		Method:      http.MethodGet,
		Path:        "/projects",
		Summary:     "List projects.",
		// SDL-visible annotation: carried verbatim into the GraphQL SDL
		// (GET /api/schema/graphql) as `@audited`. Metadata only.
		Extensions: map[string]any{
			"x-gwag-annotations": []map[string]any{{"name": "audited"}},
		},
	}, listProjects)

	gat.Register(api, g, huma.Operation{
		OperationID: "getProject",
		Method:      http.MethodGet,
		Path:        "/projects/{id}",
		Summary:     "Get one project by id.",
	}, getProject)

	gat.Register(api, g, huma.Operation{
		OperationID: "createProject",
		Method:      http.MethodPost,
		Path:        "/projects",
		Summary:     "Create a new project.",
		// Renders as `@hasRole(role: "ADMIN")` on the GraphQL mutation.
		Extensions: map[string]any{
			"x-gwag-annotations": []map[string]any{
				{"name": "hasRole", "args": map[string]any{"role": "ADMIN"}},
			},
		},
	}, createProject)

	// Mount gat's surfaces. RegisterHuma covers GraphQL + the three
	// schema-view endpoints under /api. RegisterGRPC mounts connect-go
	// handlers under /api/grpc.
	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		log.Fatalf("RegisterHuma: %v", err)
	}
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		log.Fatalf("RegisterGRPC: %v", err)
	}

	log.Printf("listening on http://localhost%s", *addr)
	log.Printf("  REST       GET  http://localhost%s/projects", *addr)
	log.Printf("  GraphQL    POST http://localhost%s/api/graphql", *addr)
	log.Printf("  gRPC       POST http://localhost%s/api/grpc/projects.v1.Service/getProject", *addr)
	log.Printf("  SDL        GET  http://localhost%s/api/schema/graphql", *addr)
	log.Printf("  proto FDS  GET  http://localhost%s/api/schema/proto", *addr)
	log.Printf("  OpenAPI    GET  http://localhost%s/api/schema/openapi", *addr)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

// listProjects is a plain huma handler. Nothing about it knows or
// cares that GraphQL/gRPC clients also reach it — gat dispatches
// in-process via captured handler refs.
func listProjects(ctx context.Context, in *ListProjectsInput) (*ListProjectsOutput, error) {
	storeMu.RLock()
	defer storeMu.RUnlock()

	out := &ListProjectsOutput{}
	out.Body.Projects = make([]*Project, 0, len(store))
	for _, p := range store {
		out.Body.Projects = append(out.Body.Projects, p)
		if in.Limit > 0 && len(out.Body.Projects) >= in.Limit {
			break
		}
	}
	return out, nil
}

func getProject(ctx context.Context, in *GetProjectInput) (*GetProjectOutput, error) {
	storeMu.RLock()
	defer storeMu.RUnlock()

	p, ok := store[in.ID]
	if !ok {
		return nil, huma.Error404NotFound(fmt.Sprintf("project %q not found", in.ID))
	}
	return &GetProjectOutput{Body: *p}, nil
}

func createProject(ctx context.Context, in *CreateProjectInput) (*CreateProjectOutput, error) {
	storeMu.Lock()
	defer storeMu.Unlock()

	if _, exists := store[in.Body.ID]; exists {
		return nil, huma.Error409Conflict(fmt.Sprintf("project %q already exists", in.Body.ID))
	}
	p := &Project{ID: in.Body.ID, Name: in.Body.Name, Tags: in.Body.Tags}
	store[p.ID] = p
	return &CreateProjectOutput{Body: *p}, nil
}
