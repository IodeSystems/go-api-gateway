package ir

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/IodeSystems/graphql-go/v2"
)

// IntrospectionQuery is the canonical GraphQL introspection query —
// what every codegen tool sends to learn a server's schema. Mirrors
// graphql-js' getIntrospectionQuery (without descriptions for brevity).
//
// Stability: stable
const IntrospectionQuery = `query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives {
      name
      description
      locations
      args { ...InputValue }
    }
  }
}
fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: true) {
    name
    description
    args { ...InputValue }
    type { ...TypeRef }
    isDeprecated
    deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) {
    name
    description
    isDeprecated
    deprecationReason
  }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue {
  name
  description
  type { ...TypeRef }
  defaultValue
}
fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType { kind name ofType { kind name ofType { kind name } } }
        }
      }
    }
  }
}`

// WriteJSON encodes v as indented JSON to w.
//
// Stability: stable
func WriteJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// PrintSchemaSDL walks a graphql.Schema and emits a GraphQL SDL string
// representing it. Covers what this gateway emits: object types, input
// objects, enums, scalars, and field deprecation. Runtime concerns
// (resolvers, defaults beyond String/Int/Bool/Enum) are not reflected.
//
// Stability: stable
func PrintSchemaSDL(s *graphql.Schema, ann ...*AnnotationIndex) string {
	var idx *AnnotationIndex
	if len(ann) > 0 {
		idx = ann[0]
	}
	var b strings.Builder
	tm := s.TypeMap()

	names := make([]string, 0, len(tm))
	for n := range tm {
		// Skip the introspection types (__Schema etc.) and built-in
		// scalars; they're implicit in every SDL document.
		if strings.HasPrefix(n, "__") {
			continue
		}
		switch n {
		case "Int", "Float", "String", "Boolean", "ID":
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)

	first := true
	separate := func() {
		if !first {
			b.WriteString("\n")
		}
		first = false
	}

	// Schema declaration includes whichever roots graphql-go assembled.
	q := s.QueryType()
	sub := s.SubscriptionType()
	mut := s.MutationType()
	if q != nil || sub != nil || mut != nil {
		separate()
		b.WriteString("schema {\n")
		if q != nil {
			b.WriteString("  query: ")
			b.WriteString(q.Name())
			b.WriteString("\n")
		}
		if mut != nil {
			b.WriteString("  mutation: ")
			b.WriteString(mut.Name())
			b.WriteString("\n")
		}
		if sub != nil {
			b.WriteString("  subscription: ")
			b.WriteString(sub.Name())
			b.WriteString("\n")
		}
		b.WriteString("}\n")
	}

	// Synthesized directive declarations the annotated fields/types
	// below reference, so the emitted SDL stays self-validating.
	if decls := idx.declarations(); len(decls) > 0 {
		separate()
		for _, d := range decls {
			b.WriteString(d)
			b.WriteString("\n")
		}
	}

	for _, name := range names {
		switch t := tm[name].(type) {
		case *graphql.Object:
			separate()
			writeObject(&b, t, idx)
		case *graphql.InputObject:
			separate()
			writeInputObject(&b, t, idx)
		case *graphql.Enum:
			separate()
			writeEnum(&b, t, idx)
		case *graphql.Scalar:
			separate()
			writeScalar(&b, t, idx)
		case *graphql.Interface:
			separate()
			writeInterface(&b, t, idx)
		case *graphql.Union:
			separate()
			writeUnion(&b, t, idx)
		}
	}
	return b.String()
}

func writeDescription(b *strings.Builder, indent, desc string) {
	if desc == "" {
		return
	}
	// Emit one block-string description (triple-quoted). One block
	// per line was previously emitted but produces consecutive
	// triple-quoted strings that GraphQL lexers reject (codegen tools
	// in particular). Collapse internal whitespace-only lines to a
	// single blank line and trim leading/trailing whitespace per line
	// to keep the rendered SDL readable.
	lines := strings.Split(desc, "\n")
	trimmed := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed = append(trimmed, strings.TrimRight(line, " \t"))
	}
	// Drop leading/trailing blank lines.
	for len(trimmed) > 0 && strings.TrimSpace(trimmed[0]) == "" {
		trimmed = trimmed[1:]
	}
	for len(trimmed) > 0 && strings.TrimSpace(trimmed[len(trimmed)-1]) == "" {
		trimmed = trimmed[:len(trimmed)-1]
	}
	if len(trimmed) == 0 {
		return
	}
	body := strings.Join(trimmed, "\n")
	// Escape any embedded `"""` so the block-string remains parseable.
	body = strings.ReplaceAll(body, "\"\"\"", "\\\"\\\"\\\"")
	if strings.Contains(body, "\n") {
		b.WriteString(indent)
		b.WriteString("\"\"\"\n")
		for _, line := range strings.Split(body, "\n") {
			b.WriteString(indent)
			b.WriteString(line)
			b.WriteString("\n")
		}
		b.WriteString(indent)
		b.WriteString("\"\"\"\n")
		return
	}
	b.WriteString(indent)
	b.WriteString("\"\"\"")
	b.WriteString(body)
	b.WriteString("\"\"\"\n")
}

func writeObject(b *strings.Builder, o *graphql.Object, idx *AnnotationIndex) {
	writeDescription(b, "", o.Description())
	b.WriteString("type ")
	b.WriteString(o.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(o.Name())))
	b.WriteString(" {\n")
	writeFields(b, o.Fields(), idx, o.Name())
	b.WriteString("}\n")
}

func writeInterface(b *strings.Builder, o *graphql.Interface, idx *AnnotationIndex) {
	writeDescription(b, "", o.Description())
	b.WriteString("interface ")
	b.WriteString(o.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(o.Name())))
	b.WriteString(" {\n")
	writeFields(b, o.Fields(), idx, o.Name())
	b.WriteString("}\n")
}

func writeUnion(b *strings.Builder, u *graphql.Union, idx *AnnotationIndex) {
	writeDescription(b, "", u.Description())
	b.WriteString("union ")
	b.WriteString(u.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(u.Name())))
	b.WriteString(" =")
	for i, t := range u.Types() {
		if i > 0 {
			b.WriteString(" |")
		}
		b.WriteString(" ")
		b.WriteString(t.Name())
	}
	b.WriteString("\n")
}

func writeFields(b *strings.Builder, fields graphql.FieldDefinitionMap, idx *AnnotationIndex, typeName string) {
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := fields[n]
		writeDescription(b, "  ", f.Description)
		b.WriteString("  ")
		b.WriteString(f.Name)
		if len(f.Args) > 0 {
			writeArgs(b, f.Args)
		}
		b.WriteString(": ")
		b.WriteString(typeRef(f.Type))
		if f.DeprecationReason != "" {
			b.WriteString(" @deprecated(reason: ")
			b.WriteString(quoteString(f.DeprecationReason))
			b.WriteString(")")
		}
		b.WriteString(gqlAnnotations(idx.fieldAnnotations(typeName, f.Name)))
		b.WriteString("\n")
	}
}

func writeArgs(b *strings.Builder, args []*graphql.Argument) {
	// graphql-go materializes Args from FieldConfigArgument (a map), so
	// the slice order is randomized per process. Sort by name so the SDL
	// is reproducible — matches writeFields / writeInputObject / writeEnum.
	args = append([]*graphql.Argument(nil), args...)
	sort.Slice(args, func(i, j int) bool { return args[i].Name() < args[j].Name() })
	hasDesc := false
	for _, a := range args {
		if a.Description() != "" {
			hasDesc = true
			break
		}
	}
	if !hasDesc {
		b.WriteString("(")
		for i, a := range args {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(a.Name())
			b.WriteString(": ")
			b.WriteString(typeRef(a.Type))
			if a.DefaultValue != nil {
				b.WriteString(" = ")
				b.WriteString(formatDefault(a.DefaultValue))
			}
		}
		b.WriteString(")")
		return
	}
	b.WriteString("(\n")
	for _, a := range args {
		writeDescription(b, "    ", a.Description())
		b.WriteString("    ")
		b.WriteString(a.Name())
		b.WriteString(": ")
		b.WriteString(typeRef(a.Type))
		if a.DefaultValue != nil {
			b.WriteString(" = ")
			b.WriteString(formatDefault(a.DefaultValue))
		}
		b.WriteString("\n")
	}
	b.WriteString("  )")
}

func writeInputObject(b *strings.Builder, io *graphql.InputObject, idx *AnnotationIndex) {
	writeDescription(b, "", io.Description())
	b.WriteString("input ")
	b.WriteString(io.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(io.Name())))
	b.WriteString(" {\n")
	fields := io.Fields()
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := fields[n]
		writeDescription(b, "  ", f.Description())
		b.WriteString("  ")
		b.WriteString(f.Name())
		b.WriteString(": ")
		b.WriteString(typeRef(f.Type))
		if dv := f.DefaultValue; dv != nil {
			b.WriteString(" = ")
			b.WriteString(formatDefault(dv))
		}
		b.WriteString(gqlAnnotations(idx.fieldAnnotations(io.Name(), f.Name())))
		b.WriteString("\n")
	}
	b.WriteString("}\n")
}

func writeEnum(b *strings.Builder, e *graphql.Enum, idx *AnnotationIndex) {
	writeDescription(b, "", e.Description())
	b.WriteString("enum ")
	b.WriteString(e.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(e.Name())))
	b.WriteString(" {\n")
	values := append([]*graphql.EnumValueDefinition(nil), e.Values()...)
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	for _, v := range values {
		writeDescription(b, "  ", v.Description)
		b.WriteString("  ")
		b.WriteString(v.Name)
		if v.DeprecationReason != "" {
			b.WriteString(" @deprecated(reason: ")
			b.WriteString(quoteString(v.DeprecationReason))
			b.WriteString(")")
		}
		b.WriteString("\n")
	}
	b.WriteString("}\n")
}

func writeScalar(b *strings.Builder, s *graphql.Scalar, idx *AnnotationIndex) {
	writeDescription(b, "", s.Description())
	b.WriteString("scalar ")
	b.WriteString(s.Name())
	b.WriteString(gqlAnnotations(idx.typeAnnotations(s.Name())))
	b.WriteString("\n")
}

// typeRef formats a graphql.Output as its SDL type reference: lists are
// [T] and non-null is T!.
func typeRef(t graphql.Type) string {
	switch tt := t.(type) {
	case *graphql.NonNull:
		return typeRef(tt.OfType) + "!"
	case *graphql.List:
		return "[" + typeRef(tt.OfType) + "]"
	default:
		return t.Name()
	}
}

func formatDefault(v any) string {
	switch x := v.(type) {
	case string:
		return quoteString(x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func quoteString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
