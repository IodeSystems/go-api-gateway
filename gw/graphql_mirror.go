package gateway

import (
	"encoding/json"

	"github.com/IodeSystems/graphql-go/v2/language/ast"
)

// graphQLMirror is the per-source AST helper used by graphQLDispatcher
// to forward the caller's selection set to a downstream GraphQL
// service. The pre-cutover graphQLMirror also did type construction;
// that role moved to RenderGraphQLRuntime via IRTypeBuilder, leaving
// only the AST rewrite (rewriteFieldForRemote / unprefixTypeName)
// here. The mirror's lifetime tracks the schema build — a fresh mirror
// per source per build, captured by the dispatcher.
type graphQLMirror struct {
	src *graphQLSource
	// isLatest controls which prefix unprefixTypeName strips off
	// inline-fragment type conditions: "<ns>_" for latest, otherwise
	// "<ns>_<vN>_". Set by registerGraphQLDispatchersLocked after
	// construction so a single mirror can serve any version's
	// dispatchers in the registry.
	isLatest bool
}

func newGraphQLMirror(src *graphQLSource) *graphQLMirror {
	return &graphQLMirror{src: src, isLatest: true}
}

// rewriteFieldForRemote returns a clone of the AST field with its
// Name set to remoteName and any inline-fragment type-conditions
// in the selection tree un-prefixed (`<ns>_Cat` → `Cat`) so the
// remote sees its own type names. Nested field names pass through
// unchanged — the gateway only prefixes top-level names in the
// local schema. Argument lists and aliases pass through unchanged.
// Always uses ast.NewX constructors so the Kind fields the
// printer's visitor relies on are populated.
func (m *graphQLMirror) rewriteFieldForRemote(field *ast.Field, remoteName string) *ast.Field {
	out := ast.NewField(&ast.Field{
		Alias:      field.Alias,
		Name:       ast.NewName(&ast.Name{Value: remoteName}),
		Arguments:  field.Arguments,
		Directives: field.Directives,
	})
	if field.SelectionSet != nil {
		out.SelectionSet = m.rewriteSelectionSet(field.SelectionSet)
	}
	return out
}

func (m *graphQLMirror) rewriteSelectionSet(sel *ast.SelectionSet) *ast.SelectionSet {
	if sel == nil {
		return nil
	}
	out := ast.NewSelectionSet(&ast.SelectionSet{
		Selections: make([]ast.Selection, 0, len(sel.Selections)),
	})
	for _, s := range sel.Selections {
		switch n := s.(type) {
		case *ast.Field:
			cloned := ast.NewField(&ast.Field{
				Alias:      n.Alias,
				Name:       n.Name,
				Arguments:  n.Arguments,
				Directives: n.Directives,
			})
			if n.SelectionSet != nil {
				cloned.SelectionSet = m.rewriteSelectionSet(n.SelectionSet)
			}
			out.Selections = append(out.Selections, cloned)
		case *ast.InlineFragment:
			frag := ast.NewInlineFragment(&ast.InlineFragment{
				Directives: n.Directives,
			})
			if n.TypeCondition != nil && n.TypeCondition.Name != nil {
				frag.TypeCondition = ast.NewNamed(&ast.Named{
					Name: ast.NewName(&ast.Name{Value: m.unprefixTypeName(n.TypeCondition.Name.Value)}),
				})
			}
			if n.SelectionSet != nil {
				frag.SelectionSet = m.rewriteSelectionSet(n.SelectionSet)
			}
			out.Selections = append(out.Selections, frag)
		default:
			// Fragment spreads pass through unchanged. v1 callers don't
			// synthesise these locally — every selection the gateway
			// sees comes from rp.Info.FieldASTs which only carries
			// inline fragments. If a real client ever sends a named
			// fragment we'd need its definition forwarded too.
			out.Selections = append(out.Selections, s)
		}
	}
	return out
}

// unprefixTypeName strips the source's "<ns>_" or "<ns>_<vN>_" prefix
// from a local type name. Returns the name unchanged when there's no
// match — the rewriter is best-effort, and a non-prefixed name is
// either a built-in scalar or an introspection mismatch we forward
// verbatim.
func (m *graphQLMirror) unprefixTypeName(name string) string {
	if !m.isLatest {
		long := m.src.namespace + "_" + m.src.version + "_"
		if len(name) > len(long) && name[:len(long)] == long {
			return name[len(long):]
		}
	}
	short := m.src.namespace + "_"
	if len(name) > len(short) && name[:len(short)] == short {
		return name[len(short):]
	}
	return name
}

// jsonUnmarshalLoose decodes the upstream's `data` envelope into a
// generic map. Numbers stay as float64 — graphql-go's coerceInt
// does not match json.Number (typed-string), so UseNumber would
// silently null out every Int field on the response. ID values
// large enough to lose float64 precision should be carried as
// strings on the wire (which is the conventional GraphQL ID shape
// anyway).
func jsonUnmarshalLoose(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// prefixResponseTypenames rewrites every `"__typename":"<value>"`
// occurrence in `src` so the value is prepended with the gateway's
// namespace prefix (matching what mirror.unprefixTypeName strips on
// the way out). Needed on the DispatchAppend path: the fork's
// ExecutePlanAppend walker bypasses per-field resolution for fields
// that set ResolveAppend, so graphql-go's __typename machinery never
// fires — the upstream's bare type name would leak into the local
// response unless we patch it here.
//
// The scanner tracks string-literal context so a string VALUE that
// literally contains the substring `"__typename":"..."` doesn't get
// rewritten (rare in practice — JSON would have escaped the inner
// quotes, but defensive). Numbers, true/false/null on the value side
// are not type names; the scanner requires a quoted value.
func (m *graphQLMirror) prefixResponseTypenames(src []byte) []byte {
	prefix := m.src.namespace + "_"
	target := []byte(`"__typename"`)
	out := make([]byte, 0, len(src)+8)
	i := 0
	for i < len(src) {
		// Find next candidate key position. To be a key, the
		// `"__typename"` literal must appear at a JSON-key slot,
		// preceded by `{` or `,` (with optional whitespace) and
		// followed by `:` (with optional whitespace).
		j := indexAtKey(src[i:], target)
		if j < 0 {
			out = append(out, src[i:]...)
			break
		}
		// Append up to and including the key + colon + whitespace.
		out = append(out, src[i:i+j+len(target)]...)
		i += j + len(target)
		// Skip optional whitespace.
		for i < len(src) && (src[i] == ' ' || src[i] == '\t' || src[i] == '\n' || src[i] == '\r') {
			out = append(out, src[i])
			i++
		}
		// Expect colon.
		if i >= len(src) || src[i] != ':' {
			continue
		}
		out = append(out, src[i])
		i++
		// Skip optional whitespace after colon.
		for i < len(src) && (src[i] == ' ' || src[i] == '\t' || src[i] == '\n' || src[i] == '\r') {
			out = append(out, src[i])
			i++
		}
		// Expect quoted value.
		if i >= len(src) || src[i] != '"' {
			continue
		}
		// Emit opening quote, then prefix, then the rest of the value
		// up to the closing quote.
		out = append(out, '"')
		out = append(out, prefix...)
		i++
		for i < len(src) {
			c := src[i]
			if c == '\\' && i+1 < len(src) {
				out = append(out, c, src[i+1])
				i += 2
				continue
			}
			out = append(out, c)
			i++
			if c == '"' {
				break
			}
		}
	}
	return out
}

// indexAtKey returns the index of the first occurrence of `key`
// (a quoted JSON identifier, e.g. `"__typename"`) in src that sits
// at a JSON-object key position — preceded by `{` or `,` with
// optional whitespace, and not inside a string literal. Returns -1
// when no such occurrence exists.
//
// The scanner is byte-level; doesn't allocate.
func indexAtKey(src, key []byte) int {
	inString := false
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch {
		case inString:
			if c == '\\' && i+1 < len(src) {
				i++ // skip escaped char
				continue
			}
			if c == '"' {
				inString = false
			}
		case c == '"':
			// Check whether this `"` begins our target key. The key
			// itself starts with `"`; if the surrounding context is a
			// key slot, this is the match. The preceding non-whitespace
			// byte should be `{` or `,`.
			pos := i - 1
			for pos >= 0 && (src[pos] == ' ' || src[pos] == '\t' || src[pos] == '\n' || src[pos] == '\r') {
				pos--
			}
			if pos < 0 || src[pos] == '{' || src[pos] == ',' {
				if i+len(key) <= len(src) && bytesEqual(src[i:i+len(key)], key) {
					return i
				}
			}
			inString = true
		}
	}
	return -1
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
