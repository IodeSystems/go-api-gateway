package gat_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/gat"
)

// A Go map on a response body reached the proto renderer with no field type at
// all, and RegisterGRPC died with `invalid name reference: ""` — a hard startup
// failure rather than the lossy degradation every other unrepresentable shape
// gets. These tests pin the whole path: huma → IR → descriptor → protocompile.

type mapOutput struct {
	Body struct {
		Prompt string            `json:"prompt"`
		Simple map[string]string `json:"simple"`
	}
}

type nestedMapOutput struct {
	Body struct {
		ByGroup map[string]map[string]string            `json:"by_group"`
		Deep    map[string]map[string]map[string]string `json:"deep"`
	}
}

type mapOfStructOutput struct {
	Body struct {
		Things map[string]project `json:"things"`
	}
}

type mapInput struct {
	Body struct {
		Tags map[string]string `json:"tags"`
	}
}

func registerAndMount(t *testing.T, register func(huma.API, *gat.Gateway)) {
	t.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)
	register(api, g)
	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}
	// The regression: this compiles the synthesised FileDescriptorSet.
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}
}

func TestMapFieldInResponseCompiles(t *testing.T) {
	registerAndMount(t, func(api huma.API, g *gat.Gateway) {
		gat.Register(api, g, huma.Operation{
			OperationID: "withMap", Method: http.MethodGet, Path: "/with-map",
		}, func(ctx context.Context, _ *struct{}) (*mapOutput, error) { return &mapOutput{}, nil })
	})
}

func TestNestedMapsCompile(t *testing.T) {
	registerAndMount(t, func(api huma.API, g *gat.Gateway) {
		gat.Register(api, g, huma.Operation{
			OperationID: "nested", Method: http.MethodGet, Path: "/nested",
		}, func(ctx context.Context, _ *struct{}) (*nestedMapOutput, error) {
			return &nestedMapOutput{}, nil
		})
	})
}

func TestMapOfMessagesCompiles(t *testing.T) {
	registerAndMount(t, func(api huma.API, g *gat.Gateway) {
		gat.Register(api, g, huma.Operation{
			OperationID: "mapOfStruct", Method: http.MethodGet, Path: "/map-of-struct",
		}, func(ctx context.Context, _ *struct{}) (*mapOfStructOutput, error) {
			return &mapOfStructOutput{}, nil
		})
	})
}

func TestMapInRequestBodyCompiles(t *testing.T) {
	registerAndMount(t, func(api huma.API, g *gat.Gateway) {
		gat.Register(api, g, huma.Operation{
			OperationID: "takesMap", Method: http.MethodPost, Path: "/takes-map",
		}, func(ctx context.Context, in *mapInput) (*mapOutput, error) { return &mapOutput{}, nil })
	})
}

// GraphQL already handled maps; the SDL must keep working alongside the proto
// surface rather than one fix breaking the other.
func TestMapFieldStillRendersInGraphQLSDL(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Demo", "1.0.0"))
	g := mustNewGat(t)
	gat.Register(api, g, huma.Operation{
		OperationID: "withMap", Method: http.MethodGet, Path: "/with-map",
	}, func(ctx context.Context, _ *struct{}) (*mapOutput, error) { return &mapOutput{}, nil })
	if err := gat.RegisterHuma(api, g, "/api"); err != nil {
		t.Fatalf("RegisterHuma: %v", err)
	}
	if err := gat.RegisterGRPC(mux, g, "/api/grpc"); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/schema/graphql", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("SDL request: %d", rec.Code)
	}
	if sdl := rec.Body.String(); !strings.Contains(sdl, "simple") {
		t.Fatalf("map field missing from the SDL:\n%s", sdl)
	}
}
