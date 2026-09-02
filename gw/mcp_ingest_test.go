package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/mark3labs/mcp-go/server/servertest"
)

// newTestMCPServer builds an in-process MCP server exposing two tools
// and serves it over the Streamable HTTP transport via httptest.
func newTestMCPServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := mcpserver.NewMCPServer("test-mcp", "1.0.0",
		mcpserver.WithToolCapabilities(false),
	)
	s.AddTool(
		mcp.NewTool("echo",
			mcp.WithDescription("Echo the message back"),
			mcp.WithString("message", mcp.Required(), mcp.Description("Text to echo")),
		),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("echo: " + req.GetArguments()["message"].(string)), nil
		},
	)
	s.AddTool(
		mcp.NewTool("greet",
			mcp.WithDescription("Greet someone in a tone"),
			mcp.WithString("name", mcp.Required()),
			mcp.WithString("tone", mcp.Enum("formal", "casual")),
		),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			tone, _ := args["tone"].(string)
			return mcp.NewToolResultText(fmt.Sprintf("greet(%v, tone=%q)", args["name"], tone)), nil
		},
	)
	srv := servertest.NewTestStreamableHTTPServer(s)
	t.Cleanup(srv.Close)
	return srv
}

func mcpEndpoint(t *testing.T) string {
	t.Helper()
	return newTestMCPServer(t).URL
}

// TestAddMCP_ToolsSurfaceAsMutations pins that AddMCP introspects the
// upstream and exposes each tool as a GraphQL Mutation.
func TestAddMCP_ToolsSurfaceAsMutations(t *testing.T) {
	endpoint := mcpEndpoint(t)
	gw := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)
	if err := gw.AddMCP(MCPHTTP, endpoint, As("tools")); err != nil {
		t.Fatalf("AddMCP: %v", err)
	}
	srv := httptest.NewServer(gw.SchemaHandler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/schema/graphql")
	if err != nil {
		t.Fatalf("schema GET: %v", err)
	}
	defer resp.Body.Close()
	sdl, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"type Mutation", "echo", "greet", "message: String"} {
		if !strings.Contains(string(sdl), want) {
			t.Errorf("SDL missing %q; got:\n%s", want, sdl)
		}
	}
}

// TestAddMCP_DispatchToolCall runs a tool end-to-end: GraphQL mutation
// → mcpDispatcher → tools/call → result conversion.
func TestAddMCP_DispatchToolCall(t *testing.T) {
	endpoint := mcpEndpoint(t)
	gw := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)
	if err := gw.AddMCP(MCPHTTP, endpoint, As("tools")); err != nil {
		t.Fatalf("AddMCP: %v", err)
	}
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	body := `{"query":"mutation { tools { echo(message:\"hello\") } }"}`
	resp, err := http.Post(srv.URL+"/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	var out struct {
		Data struct {
			Tools struct {
				Echo any `json:"echo"`
			} `json:"tools"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(out.Errors) > 0 {
		t.Fatalf("graphql errors: %v", out.Errors)
	}
	result, ok := out.Data.Tools.Echo.(map[string]any)
	if !ok {
		t.Fatalf("echo result not an object: %T %v", out.Data.Tools.Echo, out.Data.Tools.Echo)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) == 0 {
		t.Fatalf("result has no content blocks: %v", result)
	}
	first, _ := content[0].(map[string]any)
	if first["text"] != "echo: hello" {
		t.Errorf("echo text = %v; want %q", first["text"], "echo: hello")
	}
}

// TestAddMCP_UnreachableServerFails pins that a dead upstream at boot
// surfaces as a registration error rather than a half-registered slot.
func TestAddMCP_UnreachableServerFails(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)
	err := gw.AddMCP(MCPHTTP, "http://127.0.0.1:1/mcp", As("dead"))
	if err == nil {
		t.Fatal("AddMCP against an unreachable server should fail")
	}
}
