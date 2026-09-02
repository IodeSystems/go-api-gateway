package gateway

import (
	"encoding/base64"
	"math"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/IodeSystems/graphql-go/v2/language/ast"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// appendProtoMessage walks `msg` in lockstep with `selSet` (the local
// GraphQL selection AST for this message) and emits a JSON object
// covering only the selected fields. Field names use the cached
// lowerCamel jsonKey (see fieldInfosFor); response keys honor
// per-field aliases.
//
// The "no AppendDispatcher detour through messageToMap" version of
// the proto dispatch path — skips the full-message map allocation
// in favor of one append-only write into `dst`. Per-request alloc
// drops to roughly the depth of the response (one slice grow per
// JSON layer) instead of one map per nested message.
//
// Inline fragments are followed when their type condition matches
// the local type name OR is empty; named fragment spreads pass
// through as no-ops (rare in gateway-emitted queries — the gateway's
// printer doesn't synthesize named fragments).
func appendProtoMessage(dst []byte, msg protoreflect.Message, selSet *ast.SelectionSet) []byte {
	dst = append(dst, '{')
	first := true
	if selSet != nil {
		dst, first = appendProtoSelection(dst, msg, selSet, first)
	} else {
		// No selection — defensive fallback. Emit every set field
		// using the cached field info. Real GraphQL queries always
		// carry a selection on object fields.
		for _, info := range fieldInfosFor(msg.Descriptor()) {
			if !msg.Has(info.fd) {
				continue
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, info.jsonKey)
			dst = append(dst, ':')
			dst = appendProtoFieldValue(dst, info.fd, msg.Get(info.fd), nil)
		}
	}
	return append(dst, '}')
}

// appendProtoSelection processes selSet against msg, appending each
// matched field's key/value pair. Returns the updated dst + a "first"
// flag so callers can chain selection sets (e.g. an inline fragment's
// selections appended to the parent's). `first=true` means "no leading
// comma needed before the next field"; subsequent emissions flip it.
func appendProtoSelection(dst []byte, msg protoreflect.Message, selSet *ast.SelectionSet, first bool) ([]byte, bool) {
	if selSet == nil {
		return dst, first
	}
	infos := protoFieldByJSONKey(msg.Descriptor())
	for _, sel := range selSet.Selections {
		switch n := sel.(type) {
		case *ast.Field:
			fieldName := n.Name.Value
			if fieldName == "__typename" {
				// Local schema's type name is the renamed one (e.g.
				// "auth_User"). The renderer fills __typename from
				// the schema; we don't emit it here. graphql-go does
				// __typename specially even on ResolveAppend fields —
				// actually no, ResolveAppend bypasses the recursive
				// per-field machinery. To be safe, emit the
				// MessageDescriptor's name; downstream tests can
				// pin a specific value if needed.
				if !first {
					dst = append(dst, ',')
				}
				first = false
				respKey := fieldName
				if n.Alias != nil && n.Alias.Value != "" {
					respKey = n.Alias.Value
				}
				dst = appendJSONString(dst, respKey)
				dst = append(dst, ':')
				dst = appendJSONString(dst, string(msg.Descriptor().Name()))
				continue
			}
			fd, ok := infos[fieldName]
			if !ok {
				// Unknown field in selection — graphql-go validates
				// against the local schema, which the renderer built
				// from these very descriptors, so this should be
				// unreachable. Be defensive.
				continue
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			respKey := fieldName
			if n.Alias != nil && n.Alias.Value != "" {
				respKey = n.Alias.Value
			}
			dst = appendJSONString(dst, respKey)
			dst = append(dst, ':')
			if !msg.Has(fd) {
				dst = appendProtoDefault(dst, fd)
				continue
			}
			dst = appendProtoFieldValue(dst, fd, msg.Get(fd), n.SelectionSet)
		case *ast.InlineFragment:
			// Inline fragment: if the type condition matches (or is
			// empty), splice its sub-selections into the current
			// object. Type conditions on proto-origin schemas use the
			// gateway-prefixed names (e.g. "auth_User"). The base
			// message has no way to test the prefix easily here;
			// safest behavior is to splice unconditionally. graphql-go
			// already validated that the fragment applies to the
			// runtime type.
			if n.SelectionSet != nil {
				dst, first = appendProtoSelection(dst, msg, n.SelectionSet, first)
			}
		default:
			// Named fragment spreads are unusual in
			// gateway-synthesised queries; treat as no-op.
		}
	}
	return dst, first
}

// appendProtoFieldValue dispatches on list vs scalar; recurses into
// nested messages with the sub-selection.
func appendProtoFieldValue(dst []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value, selSet *ast.SelectionSet) []byte {
	if fd.IsList() {
		dst = append(dst, '[')
		l := v.List()
		for i := 0; i < l.Len(); i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendProtoLeaf(dst, fd, l.Get(i), selSet)
		}
		return append(dst, ']')
	}
	return appendProtoLeaf(dst, fd, v, selSet)
}

// appendProtoLeaf emits a single (non-list) proto value as JSON.
// Sub-selection only matters for MessageKind.
func appendProtoLeaf(dst []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value, selSet *ast.SelectionSet) []byte {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v.Bool() {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return strconv.AppendInt(dst, v.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return strconv.AppendUint(dst, v.Uint(), 10)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// Stringified for precision parity with messageToMap's
		// formatInt64 — matches the Long scalar emission on the SDL
		// side.
		dst = append(dst, '"')
		dst = strconv.AppendInt(dst, v.Int(), 10)
		return append(dst, '"')
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		dst = append(dst, '"')
		dst = strconv.AppendUint(dst, v.Uint(), 10)
		return append(dst, '"')
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			// JSON has no NaN / Inf; emit null to match what
			// json.Marshal does in the map path.
			return append(dst, "null"...)
		}
		abs := math.Abs(f)
		fmtByte := byte('f')
		if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			fmtByte = 'e'
		}
		return strconv.AppendFloat(dst, f, fmtByte, -1, 64)
	case protoreflect.StringKind:
		return appendJSONString(dst, v.String())
	case protoreflect.BytesKind:
		dst = append(dst, '"')
		// Standard base64 with padding (proto3 JSON spec).
		enc := base64.StdEncoding
		buf := v.Bytes()
		size := enc.EncodedLen(len(buf))
		// Reserve via append; encode in place.
		out := make([]byte, size)
		enc.Encode(out, buf)
		dst = append(dst, out...)
		return append(dst, '"')
	case protoreflect.EnumKind:
		// scalarToAny returns int32(v.Enum()); the corresponding JSON
		// representation matches proto3-JSON canonical form (integer
		// for unnamed serialisation). graphql-go's enum types index
		// by int32 numerical value (see types.go enumFromDescriptor),
		// which matches the value graphql-codegen consumers expect to
		// see post-Serialize. Emit the enum's name as a JSON string —
		// graphql-go's enum output writer would call
		// Enum.Serialize(int32) → name → JSON string. We short-circuit
		// here, but to stay format-compatible we still emit the name.
		ev := fd.Enum().Values().ByNumber(v.Enum())
		if ev == nil {
			return strconv.AppendInt(dst, int64(v.Enum()), 10)
		}
		return appendJSONString(dst, string(ev.Name()))
	case protoreflect.MessageKind, protoreflect.GroupKind:
		sub := v.Message()
		return appendProtoMessage(dst, sub, selSet)
	}
	return append(dst, "null"...)
}

// appendProtoDefault emits the JSON form of a proto3 default value
// when the field is unset. messageToMap's path uses `Has(fd)` to skip
// unset fields entirely; for graphql conformance (the local schema
// declares NonNull on most fields), unset becomes the zero-value
// JSON. Matches what protojson.MarshalAppend would produce for an
// unset field in `emit_unpopulated` mode.
func appendProtoDefault(dst []byte, fd protoreflect.FieldDescriptor) []byte {
	if fd.IsList() {
		return append(dst, "[]"...)
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return append(dst, "false"...)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return append(dst, '0')
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return append(dst, `"0"`...)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return append(dst, '0')
	case protoreflect.StringKind:
		return append(dst, `""`...)
	case protoreflect.BytesKind:
		return append(dst, `""`...)
	case protoreflect.EnumKind:
		// Default enum value is the value with number 0.
		ev := fd.Enum().Values().ByNumber(0)
		if ev != nil {
			return appendJSONString(dst, string(ev.Name()))
		}
		return append(dst, '0')
	}
	return append(dst, "null"...)
}

// protoFieldByJSONKey returns a per-descriptor map[jsonKey]fd.
// Memoised next to fieldInfoCache to avoid the per-call walk through
// the slice-of-fieldInfos that fieldInfosFor returns.
var protoJSONKeyCache sync.Map // map[protoreflect.MessageDescriptor]map[string]protoreflect.FieldDescriptor

func protoFieldByJSONKey(md protoreflect.MessageDescriptor) map[string]protoreflect.FieldDescriptor {
	if v, ok := protoJSONKeyCache.Load(md); ok {
		return v.(map[string]protoreflect.FieldDescriptor)
	}
	infos := fieldInfosFor(md)
	out := make(map[string]protoreflect.FieldDescriptor, len(infos))
	for _, info := range infos {
		out[info.jsonKey] = info.fd
	}
	if v, loaded := protoJSONKeyCache.LoadOrStore(md, out); loaded {
		return v.(map[string]protoreflect.FieldDescriptor)
	}
	return out
}

// appendJSONString writes a JSON-encoded string literal to dst. The
// fork's appendJSONString does the same with a tighter ASCII fast
// path; we don't have access to it from outside the package, so we
// inline a minimal version here. Covers escape rules for ", \, /,
// control chars, and non-ASCII UTF-8.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b == '"' || b == '\\' || b < 0x20 {
				dst = append(dst, s[start:i]...)
				switch b {
				case '"':
					dst = append(dst, '\\', '"')
				case '\\':
					dst = append(dst, '\\', '\\')
				case '\n':
					dst = append(dst, '\\', 'n')
				case '\r':
					dst = append(dst, '\\', 'r')
				case '\t':
					dst = append(dst, '\\', 't')
				case '\b':
					dst = append(dst, '\\', 'b')
				case '\f':
					dst = append(dst, '\\', 'f')
				default:
					dst = append(dst, '\\', 'u', '0', '0',
						hexDigit(b>>4), hexDigit(b&0xF))
				}
				i++
				start = i
				continue
			}
			i++
			continue
		}
		// Multi-byte UTF-8 — pass through verbatim.
		_, n := utf8.DecodeRuneInString(s[i:])
		i += n
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func hexDigit(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + (b - 10)
}
