package gateway

import (
	"encoding/json"

	"github.com/IodeSystems/graphql-go/v2/language/ast"

	"github.com/iodesystems/gwag/gw/ir"
)

// openAPIWalker projects an upstream JSON response onto the local
// GraphQL selection. It carries the dispatcher's IR Service + local
// type-name prefix so it can:
//
//  1. Emit __typename with the local prefixed type name on object
//     selections (matching what graphql-go's projector would do).
//  2. Discriminate Union types (TypeKind=TypeUnion) using the IR's
//     DiscriminatorProperty + DiscriminatorMapping (Case 3a) with a
//     required-fields heuristic fallback (Case 3b), mirroring
//     IRTypeBuilder.unionFor's ResolveType implementation.
//
// Walker fields are read-only after construction. Nil svc/prefix
// degrades the walker to the pre-Phase-3a behavior (untyped
// passthrough, __typename emits null).
type openAPIWalker struct {
	svc    *ir.Service
	prefix string // "<ns>_" or "<ns>_<vN>_" — matches renderer naming
}

// appendValue walks JSON `raw` in lockstep with `selSet`, projecting
// only the fields the local selection asks for. `outRef` is the IR
// type ref for this position; nil means an unknown context (e.g.
// missing op metadata) and the walker degrades gracefully.
//
// The walker is one-shallow-Unmarshal per object level: outer JSON
// gets unmarshaled into map[string]json.RawMessage (one allocation,
// no recursion into nested objects), then each selected child's raw
// bytes are walked recursively.
//
// Fundamental limitation: OpenAPI specs that publish a looser JSON
// type than the local GraphQL schema declares (e.g. `"42"` for an
// Int) end up with the upstream's representation flowing through
// verbatim — the legacy Dispatch path coerced via graphql-go's
// Serialize; the append path doesn't. Most well-behaved specs match
// upstream shapes exactly, so this is rarely visible.
func (w openAPIWalker) appendValue(dst []byte, raw json.RawMessage, selSet *ast.SelectionSet, outRef *ir.TypeRef) ([]byte, error) {
	if len(raw) == 0 {
		return append(dst, "null"...), nil
	}
	i := 0
	for i < len(raw) && isJSONWhitespace(raw[i]) {
		i++
	}
	if i >= len(raw) {
		return append(dst, "null"...), nil
	}
	switch raw[i] {
	case '{':
		if selSet == nil {
			return append(dst, raw...), nil
		}
		t := w.resolveType(outRef)
		if t != nil && t.TypeKind == ir.TypeUnion {
			return w.appendUnion(dst, raw, selSet, t)
		}
		return w.appendObject(dst, raw, selSet, t)
	case '[':
		if selSet == nil {
			return append(dst, raw...), nil
		}
		return w.appendList(dst, raw, selSet, outRef)
	default:
		return append(dst, raw...), nil
	}
}

// appendObject projects a single JSON object. `t` may be nil when
// the IR can't be resolved — emission still works, __typename just
// falls back to "null" (legacy behavior).
func (w openAPIWalker) appendObject(dst []byte, raw json.RawMessage, selSet *ast.SelectionSet, t *ir.Type) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return dst, err
	}
	dst = append(dst, '{')
	dst, _, err := w.appendSelection(dst, obj, selSet, true, t)
	if err != nil {
		return dst, err
	}
	return append(dst, '}'), nil
}

// appendSelection walks `selSet` against `obj`, appending each
// matched field's response-key:value pair. `first` tracks whether a
// comma is needed; returned alongside dst so inline fragments can
// merge into the parent object cleanly.
func (w openAPIWalker) appendSelection(dst []byte, obj map[string]json.RawMessage, selSet *ast.SelectionSet, first bool, t *ir.Type) ([]byte, bool, error) {
	if selSet == nil {
		return dst, first, nil
	}
	for _, sel := range selSet.Selections {
		switch n := sel.(type) {
		case *ast.Field:
			fieldName := n.Name.Value
			respKey := fieldName
			if n.Alias != nil && n.Alias.Value != "" {
				respKey = n.Alias.Value
			}
			if fieldName == "__typename" {
				if !first {
					dst = append(dst, ',')
				}
				first = false
				dst = appendJSONString(dst, respKey)
				dst = append(dst, ':')
				if t != nil && w.prefix != "" {
					dst = appendJSONString(dst, w.prefix+t.Name)
				} else {
					dst = append(dst, "null"...)
				}
				continue
			}
			rawChild, ok := obj[fieldName]
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, respKey)
			dst = append(dst, ':')
			if !ok || len(rawChild) == 0 {
				dst = append(dst, "null"...)
				continue
			}
			var childRef *ir.TypeRef
			if t != nil {
				for _, f := range t.Fields {
					if f.Name == fieldName {
						childRef = &f.Type
						break
					}
				}
			}
			var err error
			dst, err = w.appendValue(dst, rawChild, n.SelectionSet, childRef)
			if err != nil {
				return dst, first, err
			}
		case *ast.InlineFragment:
			if n.SelectionSet == nil {
				continue
			}
			// On non-union types, inline fragments on the same type are
			// rare; splice unconditionally. Union-typed parents are
			// handled by appendUnion before reaching this loop.
			var err error
			dst, first, err = w.appendSelection(dst, obj, n.SelectionSet, first, t)
			if err != nil {
				return dst, first, err
			}
		default:
			// Named fragment spreads — pass through as no-op (renderer
			// inlines them before reaching the walker in normal flow).
		}
	}
	return dst, first, nil
}

// appendUnion handles a TypeUnion at this position: discriminate the
// upstream value to a variant Type, then emit a projected object
// using only inline fragments matching the variant. __typename emits
// the local prefixed variant name.
//
// Discrimination order:
//  1. DiscriminatorProperty + DiscriminatorMapping (Case 3a)
//  2. DiscriminatorProperty identity-match (Case 3a fallback)
//  3. Required-fields heuristic (Case 3b)
//
// Mirrors IRTypeBuilder.unionFor's ResolveType for byte-mode parity.
func (w openAPIWalker) appendUnion(dst []byte, raw json.RawMessage, selSet *ast.SelectionSet, u *ir.Type) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return dst, err
	}
	variant := w.resolveUnionVariant(obj, u)
	dst = append(dst, '{')
	first := true
	// Walk selections: __typename emits variant name; inline fragments
	// gated by type-condition match the variant; field selections on
	// the union directly (rare; only __typename is legal on raw union
	// in spec) get a null fallback.
	for _, sel := range selSet.Selections {
		switch n := sel.(type) {
		case *ast.Field:
			fieldName := n.Name.Value
			respKey := fieldName
			if n.Alias != nil && n.Alias.Value != "" {
				respKey = n.Alias.Value
			}
			if fieldName != "__typename" {
				// Direct field on union — illegal at the GraphQL layer;
				// emit null defensively (graphql-go's validator should
				// reject before reaching us).
				continue
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, respKey)
			dst = append(dst, ':')
			if variant != nil && w.prefix != "" {
				dst = appendJSONString(dst, w.prefix+variant.Name)
			} else {
				dst = append(dst, "null"...)
			}
		case *ast.InlineFragment:
			if n.SelectionSet == nil || variant == nil {
				continue
			}
			if n.TypeCondition != nil {
				cond := n.TypeCondition.Name.Value
				// TypeCondition is the local prefixed name; strip prefix
				// to match against IR variant name.
				bare := cond
				if w.prefix != "" && len(cond) > len(w.prefix) && cond[:len(w.prefix)] == w.prefix {
					bare = cond[len(w.prefix):]
				}
				if bare != variant.Name {
					continue
				}
			}
			var err error
			dst, first, err = w.appendSelection(dst, obj, n.SelectionSet, first, variant)
			if err != nil {
				return dst, err
			}
		}
	}
	return append(dst, '}'), nil
}

// resolveUnionVariant runs the discriminator+heuristic chain on the
// shallow-unmarshaled object, returning the matched variant *ir.Type
// or nil if nothing matches.
func (w openAPIWalker) resolveUnionVariant(obj map[string]json.RawMessage, u *ir.Type) *ir.Type {
	if w.svc == nil {
		return nil
	}
	// 1. Discriminator-driven (Case 3a).
	if u.DiscriminatorProperty != "" {
		if rawDisc, ok := obj[u.DiscriminatorProperty]; ok {
			var discVal string
			if json.Unmarshal(rawDisc, &discVal) == nil {
				if mapped, ok := u.DiscriminatorMapping[discVal]; ok {
					if t, ok := w.svc.Types[mapped]; ok && t.TypeKind == ir.TypeObject {
						if w.variantMember(u, t.Name) {
							return t
						}
					}
				}
				// Identity fallback (Case 3a fallback) — discriminator
				// value equals variant Type name.
				if t, ok := w.svc.Types[discVal]; ok && t.TypeKind == ir.TypeObject {
					if w.variantMember(u, t.Name) {
						return t
					}
				}
			}
		}
	}
	// 2. Wire __typename (rare on openapi but cheap to honor).
	if rawTN, ok := obj["__typename"]; ok {
		var tn string
		if json.Unmarshal(rawTN, &tn) == nil {
			if t, ok := w.svc.Types[tn]; ok && t.TypeKind == ir.TypeObject {
				if w.variantMember(u, t.Name) {
					return t
				}
			}
		}
	}
	// 3. Required-fields heuristic (Case 3b).
	for _, variantName := range u.Variants {
		v, ok := w.svc.Types[variantName]
		if !ok || v.TypeKind != ir.TypeObject {
			continue
		}
		match := true
		for _, f := range v.Fields {
			if !f.Required {
				continue
			}
			if _, present := obj[f.Name]; !present {
				match = false
				break
			}
		}
		if match {
			return v
		}
	}
	return nil
}

func (w openAPIWalker) variantMember(u *ir.Type, name string) bool {
	for _, v := range u.Variants {
		if v == name {
			return true
		}
	}
	return false
}

// resolveType walks an IR TypeRef to its named Type entry. Returns
// nil for built-in scalars, maps, or when svc isn't set.
func (w openAPIWalker) resolveType(ref *ir.TypeRef) *ir.Type {
	if ref == nil || w.svc == nil || ref.Named == "" {
		return nil
	}
	if t, ok := w.svc.Types[ref.Named]; ok {
		return t
	}
	return nil
}

// appendList parses `raw` as a JSON array and walks each element
// with the same selection and element-type ref.
func (w openAPIWalker) appendList(dst []byte, raw json.RawMessage, selSet *ast.SelectionSet, outRef *ir.TypeRef) ([]byte, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return dst, err
	}
	dst = append(dst, '[')
	for i, item := range items {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		dst, err = w.appendValue(dst, item, selSet, outRef)
		if err != nil {
			return dst, err
		}
	}
	return append(dst, ']'), nil
}

func isJSONWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
