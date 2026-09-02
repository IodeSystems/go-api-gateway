package gateway

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// MCP schema tools.
//
// These are the format-agnostic primitives the MCP server core
// exposes as `schema_list` / `schema_search` / `schema_expand`. They
// walk the gateway's slot IR (already post-transform, post-
// internal-filter) and project through MCPAllows so the operator-
// curated allowlist gates every result.
//
// Path representation: dot-segmented, starting with the namespace,
// then any group chain, ending with the op name. Examples:
//   admin.listPeers
//   users.list
//   greeter.greetings
//   library.book.list           (op `list` inside group `book`)
//
// Operators write include / exclude entries against the same shape.
// The matcher (mcpMatch) treats `*` as a one-segment wildcard and
// `**` as zero-or-more segments — see mcp_config.go for the glob
// rules.

// schemaOpKind is the MCP-surface name for an operation's GraphQL
// root. Stable string identifiers ("Query" / "Mutation" /
// "Subscription") that the MCP client / agent reads back; the
// gateway-internal ir.OpKind is an int enum so this exists to give
// the wire shape a documented contract.
type schemaOpKind string

const (
	schemaOpQuery        schemaOpKind = "Query"
	schemaOpMutation     schemaOpKind = "Mutation"
	schemaOpSubscription schemaOpKind = "Subscription"
)

func schemaOpKindFromIR(k ir.OpKind) schemaOpKind {
	switch k {
	case ir.OpMutation:
		return schemaOpMutation
	case ir.OpSubscription:
		return schemaOpSubscription
	default:
		return schemaOpQuery
	}
}

// schemaListEntry is one operation row from MCPSchemaList. The
// description is the first line of the IR Description; the full doc
// is reachable via MCPSchemaExpand.
type schemaListEntry struct {
	Path        string       `json:"path"`
	Kind        schemaOpKind `json:"kind"`
	Namespace   string       `json:"namespace"`
	Version     string       `json:"version"`
	Description string       `json:"description,omitempty"`
}

// mcpSchemaList returns every allowed operation across the gateway's
// registered services, grouped by root kind. The result is sorted
// alphabetically by path within each group for stable output.
func (g *Gateway) mcpSchemaList() []schemaListEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	svcs := g.collectSlotIRLocked(schemaFilter{})
	out := make([]schemaListEntry, 0, 16)
	for _, s := range svcs {
		walkServiceOps(s, "", func(path string, op *ir.Operation) {
			if !g.mcpAllowsLocked(path) {
				return
			}
			out = append(out, schemaListEntry{
				Path:        path,
				Kind:        schemaOpKindFromIR(op.Kind),
				Namespace:   s.Namespace,
				Version:     s.Version,
				Description: firstLine(op.Description),
			})
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Version < out[j].Version
	})
	return out
}

// schemaSearchInput is the query envelope for MCPSchemaSearch. Both
// fields are optional; an empty input matches every allowed op (i.e.
// behaves like MCPSchemaList but with the richer entry shape).
type schemaSearchInput struct {
	// PathGlob is a dot-segmented glob applied to each op's path
	// (`admin.*`, `*.list*`, `**`). Empty → no path filter.
	PathGlob string `json:"pathGlob,omitempty"`
	// Regex is matched against op name, every arg name, and the op
	// description body. Empty → no text filter. Invalid regex
	// surfaces as an error.
	Regex string `json:"regex,omitempty"`
}

// schemaSearchEntry is one result row. Args / Description / Output
// come from the IR so they reflect the post-transform truth the
// gateway will actually expose.
type schemaSearchEntry struct {
	Path        string             `json:"path"`
	Kind        schemaOpKind       `json:"kind"`
	Namespace   string             `json:"namespace"`
	Version     string             `json:"version"`
	Description string             `json:"description,omitempty"`
	Args        []schemaSearchArg  `json:"args"`
	OutputType  string             `json:"outputType,omitempty"`
}

// schemaSearchArg projects ir.Arg onto the MCP wire shape. Type is a
// human-readable summary (GraphQL-style: `String!`, `[User!]!`, etc.)
// rather than the structured TypeRef so the agent can consume it
// without re-implementing the IR type system.
type schemaSearchArg struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
}

// mcpSchemaSearch returns every allowed op matching the input
// filters. Path glob and regex are AND-combined; a missing filter is
// treated as "match all".
func (g *Gateway) mcpSchemaSearch(in schemaSearchInput) ([]schemaSearchEntry, error) {
	var re *regexp.Regexp
	if in.Regex != "" {
		var err error
		re, err = regexp.Compile(in.Regex)
		if err != nil {
			return nil, fmt.Errorf("mcp: invalid regex %q: %w", in.Regex, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	svcs := g.collectSlotIRLocked(schemaFilter{})
	out := make([]schemaSearchEntry, 0, 16)
	for _, s := range svcs {
		walkServiceOps(s, "", func(path string, op *ir.Operation) {
			if !g.mcpAllowsLocked(path) {
				return
			}
			if in.PathGlob != "" && !mcpMatch(in.PathGlob, path) {
				return
			}
			if re != nil && !opMatchesRegex(re, op) {
				return
			}
			out = append(out, schemaSearchEntry{
				Path:        path,
				Kind:        schemaOpKindFromIR(op.Kind),
				Namespace:   s.Namespace,
				Version:     s.Version,
				Description: op.Description,
				Args:        argsToSearch(op.Args),
				OutputType:  outputTypeSummary(op),
			})
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}

// mcpAllowsLocked is the g.mu-held variant of MCPAllows so callers
// already holding the lock (the schema tool walkers) don't re-enter.
func (g *Gateway) mcpAllowsLocked(p string) bool {
	if p == "" {
		return false
	}
	head := p
	if i := strings.IndexByte(p, '.'); i > 0 {
		head = p[:i]
	}
	if g.isInternal(head) {
		return false
	}
	cfg := g.mcpConfigSnapshotLocked()
	if cfg.AutoInclude {
		for _, pat := range cfg.Exclude {
			if mcpMatch(pat, p) {
				return false
			}
		}
		return true
	}
	for _, pat := range cfg.Include {
		if mcpMatch(pat, p) {
			return true
		}
	}
	return false
}

// walkServiceOps invokes `yield` for every Operation under a Service,
// passing the dot-segmented path and the op pointer. Recurses through
// Groups; the prefix carries the group chain. Skips internal namespaces
// (Service.Internal == true) so transforms.HideInternal stays honored.
//
// Op names are projected through the same per-kind rename
// (lowerCamel for proto, identity otherwise) RenderGraphQLRuntime
// applies, so MCP paths match the GraphQL field names an agent will
// hand to the `query` tool. The proto IR keeps wire-native PascalCase
// op names (matching SchemaID / dispatcher registration); the rename
// runs only at the agent-facing edge.
func walkServiceOps(s *ir.Service, prefix string, yield func(path string, op *ir.Operation)) {
	if s.Internal {
		return
	}
	base := prefix
	if base == "" {
		base = s.Namespace
	}
	rename := mcpOpName(s.OriginKind)
	for _, op := range s.Operations {
		yield(base+"."+rename(op.Name), op)
	}
	for _, grp := range s.Groups {
		walkGroupOps(grp, base, rename, yield)
	}
}

func walkGroupOps(g *ir.OperationGroup, prefix string, rename func(string) string, yield func(string, *ir.Operation)) {
	base := prefix + "." + g.Name
	for _, op := range g.Operations {
		yield(base+"."+rename(op.Name), op)
	}
	for _, sub := range g.Groups {
		walkGroupOps(sub, base, rename, yield)
	}
}

// mcpOpName mirrors opNameForRuntime: proto IR carries PascalCase
// method names but renders as lowerCamel GraphQL fields; OpenAPI and
// downstream-GraphQL ingest already use the GraphQL convention.
func mcpOpName(kind ir.Kind) func(string) string {
	if kind == ir.KindProto {
		return lowerCamel
	}
	return ir.IdentityName
}

// opMatchesRegex applies `re` to op name, every arg name, and the
// description body (in that order, short-circuiting on first hit).
func opMatchesRegex(re *regexp.Regexp, op *ir.Operation) bool {
	if re.MatchString(op.Name) {
		return true
	}
	for _, a := range op.Args {
		if re.MatchString(a.Name) {
			return true
		}
	}
	if op.Description != "" && re.MatchString(op.Description) {
		return true
	}
	return false
}

// argsToSearch projects ir.Arg list onto the search-result shape.
// Nil-safe; returns a non-nil empty slice so JSON serialization
// surfaces `"args": []` rather than `"args": null`.
func argsToSearch(args []*ir.Arg) []schemaSearchArg {
	out := make([]schemaSearchArg, 0, len(args))
	for _, a := range args {
		out = append(out, schemaSearchArg{
			Name:        a.Name,
			Type:        typeRefSummary(a.Type, a.Repeated, a.Required, a.ItemRequired),
			Required:    a.Required,
			Description: a.Description,
		})
	}
	return out
}

// outputTypeSummary builds the GraphQL-style summary for an Operation's
// return type ("User", "[User!]!", "String"). Empty when Output is nil
// (void return) so the wire shape distinguishes "no output" from "void".
func outputTypeSummary(op *ir.Operation) string {
	if op.Output == nil {
		return ""
	}
	return typeRefSummary(*op.Output, op.OutputRepeated, op.OutputRequired, op.OutputItemRequired)
}

// typeRefSummary renders a TypeRef + repeated/required flags as a
// GraphQL-style string. Scalar refs project to their GraphQL name
// (`String`, `Int`, etc.); named refs use the Type's name; maps render
// as `Map<K, V>` (no native GraphQL map type, so this is informational).
func typeRefSummary(ref ir.TypeRef, repeated, required, itemRequired bool) string {
	var inner string
	switch {
	case ref.IsBuiltin():
		inner = scalarKindName(ref.Builtin)
	case ref.IsMap():
		inner = "Map<" + typeRefSummary(ref.Map.KeyType, false, false, false) + ", " + typeRefSummary(ref.Map.ValueType, false, false, false) + ">"
	case ref.IsNamed():
		inner = ref.Named
	default:
		inner = "Unknown"
	}
	if repeated {
		if itemRequired {
			inner += "!"
		}
		inner = "[" + inner + "]"
	}
	if required {
		inner += "!"
	}
	return inner
}

func scalarKindName(k ir.ScalarKind) string {
	switch k {
	case ir.ScalarString:
		return "String"
	case ir.ScalarBool:
		return "Boolean"
	case ir.ScalarInt32:
		return "Int"
	case ir.ScalarInt64:
		return "Long"
	case ir.ScalarUInt32:
		return "Int"
	case ir.ScalarUInt64:
		return "Long"
	case ir.ScalarFloat:
		return "Float"
	case ir.ScalarDouble:
		return "Float"
	case ir.ScalarBytes:
		return "Bytes"
	case ir.ScalarID:
		return "ID"
	case ir.ScalarTimestamp:
		return "Timestamp"
	default:
		return "Unknown"
	}
}

// schemaExpandResult is the structured "drill in to this op or type"
// response. Exactly one of Op / Type is populated; the other is nil.
// Types holds every type transitively reachable from the focused
// entity (args + return for an op; fields + variants for a type),
// keyed by IR name, so the agent can resolve nested references
// without round-tripping. The closure walk is bounded by the IR's
// natural cycle-breaking (Types is keyed by name; we mark visited).
type schemaExpandResult struct {
	Path  string            `json:"path,omitempty"`
	Op    *schemaExpandOp   `json:"op,omitempty"`
	Type  *schemaExpandType `json:"type,omitempty"`
	Types []schemaExpandType `json:"types"`
}

type schemaExpandOp struct {
	Path        string             `json:"path"`
	Kind        schemaOpKind       `json:"kind"`
	Namespace   string             `json:"namespace"`
	Version     string             `json:"version"`
	Description string             `json:"description,omitempty"`
	Args        []schemaSearchArg  `json:"args"`
	OutputType  string             `json:"outputType,omitempty"`
	Deprecated  string             `json:"deprecated,omitempty"`
}

type schemaExpandType struct {
	Name        string                 `json:"name"`
	Kind        string                 `json:"kind"` // "object" / "enum" / "union" / "interface" / "scalar" / "input"
	Description string                 `json:"description,omitempty"`
	Fields      []schemaExpandField    `json:"fields,omitempty"`
	EnumValues  []schemaExpandEnumValue `json:"enumValues,omitempty"`
	Variants    []string               `json:"variants,omitempty"`
}

type schemaExpandField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
	Deprecated  string `json:"deprecated,omitempty"`
}

type schemaExpandEnumValue struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Deprecated  string `json:"deprecated,omitempty"`
}

// mcpSchemaExpand returns the structured definition of one entity. The
// caller's `nameOrPath` is matched first as a dotted op path (e.g.
// "things.getThing"), then as a type name (e.g. "ThingResponse").
// Returns an error when nothing matches or when the focus is filtered
// out by the allowlist.
//
// The Types slice carries every type transitively reachable from the
// focus (args + output for ops; fields + variants for types). The
// closure walk is cycle-safe — each Type is emitted at most once.
func (g *Gateway) mcpSchemaExpand(nameOrPath string) (*schemaExpandResult, error) {
	if nameOrPath == "" {
		return nil, fmt.Errorf("mcp: name is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	svcs := g.collectSlotIRLocked(schemaFilter{})

	// First pass: try to resolve as an op path.
	for _, s := range svcs {
		var found *ir.Operation
		var foundPath string
		walkServiceOps(s, "", func(p string, op *ir.Operation) {
			if found == nil && p == nameOrPath {
				found = op
				foundPath = p
			}
		})
		if found == nil {
			continue
		}
		if !g.mcpAllowsLocked(foundPath) {
			return nil, fmt.Errorf("mcp: %q is not on the MCP surface (allowlist denies)", foundPath)
		}
		res := &schemaExpandResult{Path: foundPath}
		res.Op = &schemaExpandOp{
			Path:        foundPath,
			Kind:        schemaOpKindFromIR(found.Kind),
			Namespace:   s.Namespace,
			Version:     s.Version,
			Description: found.Description,
			Args:        argsToSearch(found.Args),
			OutputType:  outputTypeSummary(found),
			Deprecated:  found.Deprecated,
		}
		res.Types = collectTypeClosureForOp(s, found)
		return res, nil
	}

	// Second pass: resolve as a type name. We scan every service's
	// Types map; the same name across services collides today (one
	// IR Service per (ns, ver)), so the first hit wins. Document
	// behavior: ambiguous type names need to be disambiguated by the
	// adopter via path-style ops above (which carry the ns/ver
	// implicitly).
	for _, s := range svcs {
		if s.Internal {
			continue
		}
		// Internal-NS check before allowlist (allowlist works on op
		// paths, not type names — types ride along with whatever op
		// references them).
		if g.isInternal(s.Namespace) {
			continue
		}
		t, ok := s.Types[nameOrPath]
		if !ok {
			continue
		}
		res := &schemaExpandResult{}
		res.Type = expandTypePtr(t)
		res.Types = collectTypeClosureFromType(s, t)
		return res, nil
	}
	return nil, fmt.Errorf("mcp: %q is neither an op path nor a known type", nameOrPath)
}

// collectTypeClosureForOp walks every TypeRef on the op's Args and
// Output and emits the transitive type closure. Built-in scalars and
// map types don't enter the closure (they have no Type entry).
func collectTypeClosureForOp(s *ir.Service, op *ir.Operation) []schemaExpandType {
	visited := map[string]bool{}
	out := []schemaExpandType{}
	for _, a := range op.Args {
		walkTypeRefClosure(s, a.Type, visited, &out)
	}
	if op.Output != nil {
		walkTypeRefClosure(s, *op.Output, visited, &out)
	}
	return out
}

func collectTypeClosureFromType(s *ir.Service, root *ir.Type) []schemaExpandType {
	visited := map[string]bool{root.Name: true}
	out := []schemaExpandType{}
	walkTypeClosure(s, root, visited, &out)
	return out
}

func walkTypeRefClosure(s *ir.Service, ref ir.TypeRef, visited map[string]bool, out *[]schemaExpandType) {
	switch {
	case ref.IsBuiltin():
		return
	case ref.IsMap():
		walkTypeRefClosure(s, ref.Map.KeyType, visited, out)
		walkTypeRefClosure(s, ref.Map.ValueType, visited, out)
		return
	case ref.IsNamed():
		if visited[ref.Named] {
			return
		}
		t, ok := s.Types[ref.Named]
		if !ok {
			return
		}
		visited[ref.Named] = true
		walkTypeClosure(s, t, visited, out)
	}
}

func walkTypeClosure(s *ir.Service, t *ir.Type, visited map[string]bool, out *[]schemaExpandType) {
	*out = append(*out, *expandTypePtr(t))
	for _, f := range t.Fields {
		walkTypeRefClosure(s, f.Type, visited, out)
	}
	for _, vname := range t.Variants {
		if visited[vname] {
			continue
		}
		v, ok := s.Types[vname]
		if !ok {
			continue
		}
		visited[vname] = true
		walkTypeClosure(s, v, visited, out)
	}
}

func expandTypePtr(t *ir.Type) *schemaExpandType {
	out := &schemaExpandType{
		Name:        t.Name,
		Kind:        typeKindName(t.TypeKind),
		Description: t.Description,
	}
	for _, f := range t.Fields {
		out.Fields = append(out.Fields, schemaExpandField{
			Name:        f.Name,
			Type:        typeRefSummary(f.Type, f.Repeated, f.Required, f.ItemRequired),
			Required:    f.Required,
			Description: f.Description,
			Deprecated:  f.Deprecated,
		})
	}
	for _, e := range t.Enum {
		out.EnumValues = append(out.EnumValues, schemaExpandEnumValue{
			Name:        e.Name,
			Description: e.Description,
			Deprecated:  e.Deprecated,
		})
	}
	if len(t.Variants) > 0 {
		out.Variants = append(out.Variants, t.Variants...)
	}
	return out
}

func typeKindName(k ir.TypeKind) string {
	switch k {
	case ir.TypeEnum:
		return "enum"
	case ir.TypeUnion:
		return "union"
	case ir.TypeInterface:
		return "interface"
	case ir.TypeScalar:
		return "scalar"
	case ir.TypeInput:
		return "input"
	default:
		return "object"
	}
}

// mcpQueryInput is the payload for the MCP `query` tool.
type mcpQueryInput struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
	OperationName string         `json:"operationName,omitempty"`
}

// mcpResponseWithEvents is the uniform wrapper carried from v1
// onward so a future subscription-stitching contract can land
// additively without breaking the wire shape. v1 Events.Level is
// always "none" and Channels is always empty.
type mcpResponseWithEvents struct {
	Response any             `json:"response"`
	Events   mcpEventsBundle `json:"events"`
}

type mcpEventsBundle struct {
	// Level is one of: "none" (v1), "by_channel" (v2 default), "all"
	// (v2 with byte threshold tripped or operator opt-in). The agent
	// reads this to know whether the channels slice is summary or
	// full payload.
	Level    string            `json:"level"`
	Channels []mcpChannelEvent `json:"channels"`
}

// mcpChannelEvent is one entry in the v2 events bundle. Always
// empty in v1 (the slice is empty); the type is defined now so the
// JSON Channels[] field has a stable shape from the start.
type mcpChannelEvent struct {
	Channel string `json:"channel"`
	Count   int    `json:"count"`
	Preview any    `json:"preview,omitempty"`
}

// mcpQuery executes a GraphQL operation against the gateway's
// runtime schema in-process. Bearer auth on the MCP transport is the
// security boundary (same posture as admin writes); the MCPConfig
// allowlist is operator-curated discovery guidance, not an execution
// gate. An agent only learns about an op via schema_list /
// schema_search / schema_expand, so its working set is implicitly
// bounded by what the operator has surfaced —
// without making the gateway pay per-call AST validation cost.
// Per-op execution gating may layer on in v1.x if adopters pull on
// it.
func (g *Gateway) mcpQuery(ctx context.Context, in mcpQueryInput) (*mcpResponseWithEvents, error) {
	if in.Query == "" {
		return nil, fmt.Errorf("mcp: query is required")
	}
	schema := g.schema.Load()
	if schema == nil {
		return nil, fmt.Errorf("mcp: gateway has no schema yet (no services registered)")
	}
	result := graphql.Do(graphql.Params{
		Schema:         *schema,
		RequestString:  in.Query,
		VariableValues: in.Variables,
		OperationName:  in.OperationName,
		Context:        ctx,
	})
	return &mcpResponseWithEvents{
		Response: result,
		Events: mcpEventsBundle{
			Level:    "none",
			Channels: []mcpChannelEvent{},
		},
	}, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
