package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// mcpDispatcher implements ir.Dispatcher for one tool on an ingested
// downstream MCP server. Dispatch issues a `tools/call` over the
// slot's shared MCP client and returns the result as a JSON-shaped
// map — the IR Output of every MCP operation is the JSON scalar, so
// the runtime serialises whatever the tool returns verbatim.
//
// Built once per (slot, tool) at schema-build time. The mcp-go client
// multiplexes concurrent requests by JSON-RPC id, so one dispatcher
// per tool sharing the slot's client is safe.
type mcpDispatcher struct {
	client    *client.Client
	toolName  string // upstream wire name — Operation.Name is sanitised
	namespace string
	version   string
	op        string
}

// Dispatch satisfies ir.Dispatcher. Canonical args pass straight
// through as the tool's arguments object; the CallToolResult is
// JSON-round-tripped into a map so the JSON scalar can serialise it.
func (d *mcpDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	ctx = withDispatchOpInfo(ctx, d.namespace, d.version, d.op)
	req := mcp.CallToolRequest{}
	req.Params.Name = d.toolName
	if len(args) > 0 {
		req.Params.Arguments = args
	}
	res, err := d.client.CallTool(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("mcp: tools/call %s: %w", d.toolName, err)
	}
	return mcpResultToAny(res)
}

// mcpResultToAny converts a CallToolResult to the JSON-shaped map the
// JSON scalar serialises. Marshalling the whole result preserves the
// content blocks, isError flag, and structuredContent verbatim.
func mcpResultToAny(res *mcp.CallToolResult) (any, error) {
	if res == nil {
		return map[string]any{}, nil
	}
	raw, err := json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("mcp: encode tool result: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("mcp: decode tool result: %w", err)
	}
	return out, nil
}

// Compile-time assertion: mcpDispatcher implements ir.Dispatcher.
var _ ir.Dispatcher = (*mcpDispatcher)(nil)
