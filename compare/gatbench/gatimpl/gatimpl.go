// Package gatimpl serves the gatbench workload through gat: two huma
// operations registered with gat.Register, surfaced as REST (huma's
// own), GraphQL, and Connect/gRPC.
//
// This is the "one registration, three surfaces" side of the
// comparison. The gqlgen and connect-go packages next door implement
// the same two operations by hand, once per surface.
package gatimpl

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
	"github.com/iodesystems/gwag/compare/gatbench/model"
)

type GetProjectInput struct {
	ID string `path:"id" doc:"Project identifier."`
}

type GetProjectOutput struct {
	Body model.Project
}

type ListProjectsInput struct {
	Limit int `query:"limit" doc:"Maximum projects to return."`
}

type ListProjectsOutput struct {
	Body struct {
		Projects []model.Project `json:"projects"`
	}
}

func getProject(ctx context.Context, in *GetProjectInput) (*GetProjectOutput, error) {
	p, ok := model.Get(in.ID)
	if !ok {
		return nil, huma.Error404NotFound("no such project")
	}
	return &GetProjectOutput{Body: p}, nil
}

func listProjects(ctx context.Context, in *ListProjectsInput) (*ListProjectsOutput, error) {
	out := &ListProjectsOutput{}
	out.Body.Projects = model.List(in.Limit)
	return out, nil
}

// New builds the mux with huma REST at /projects, gat GraphQL at
// /api/graphql, and gat Connect handlers under /api/grpc.
//
// The huma API title becomes the GraphQL namespace, so queries are
// `{ Gatbench { getProject(...) } }`. GraphQLQuery/ListQuery below
// spell that out so callers don't have to know the rule.
func New() (*http.ServeMux, error) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Gatbench", "1.0.0"))
	g, err := gat.New()
	if err != nil {
		return nil, err
	}
	gat.Register(api, g, huma.Operation{
		OperationID: "getProject",
		Method:      http.MethodGet,
		Path:        "/projects/{id}",
	}, getProject)
	gat.Register(api, g, huma.Operation{
		OperationID: "listProjects",
		Method:      http.MethodGet,
		Path:        "/projects",
	}, listProjects)

	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		return nil, err
	}
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		return nil, err
	}
	return mux, nil
}

// Queries the benchmarks fire. Kept here so gat's namespace rule
// lives in one place.
const (
	GetQuery  = `{ Gatbench { getProject(id: "p1") { id name tags } } }`
	ListQuery = `{ Gatbench { listProjects(limit: 25) { projects { id name tags } } } }`
)
