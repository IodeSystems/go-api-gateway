package gat

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
	"google.golang.org/protobuf/types/dynamicpb"
)

// lowerCamel converts a snake_case proto field name to lowerCamelCase,
// matching the GraphQL field name the IR / runtime expects in args
// maps. Same algorithm as gw/ir's internal helper; reproduced here
// because gat is the experimental sibling and uses its own conversion
// path.
func lowerCamel(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "_")
	for i := range parts {
		if i == 0 || parts[i] == "" {
			continue
		}
		r := []rune(parts[i])
		r[0] = unicode.ToUpper(r[0])
		parts[i] = string(r)
	}
	r := []rune(parts[0])
	if len(r) > 0 {
		r[0] = unicode.ToLower(r[0])
		parts[0] = string(r)
	}
	return strings.Join(parts, "")
}

// protoArgsToMessage walks args (lowerCamel keys, GraphQL-canonical
// values) and writes them into msg according to its descriptor.
// Unknown args are ignored; type mismatches return errors.
func protoArgsToMessage(args map[string]any, msg *dynamicpb.Message) error {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		key := lowerCamel(string(fd.Name()))
		v, ok := args[key]
		if !ok {
			continue
		}
		if err := setProtoField(msg, fd, v); err != nil {
			return fmt.Errorf("field %s: %w", fd.Name(), err)
		}
	}
	return nil
}

func setProtoField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, v any) error {
	if fd.IsList() {
		items, ok := v.([]any)
		if !ok {
			return fmt.Errorf("expected list, got %T", v)
		}
		list := msg.Mutable(fd).List()
		for _, item := range items {
			elem, err := toProtoScalar(fd, item)
			if err != nil {
				return err
			}
			list.Append(elem)
		}
		return nil
	}
	val, err := toProtoScalar(fd, v)
	if err != nil {
		return err
	}
	msg.Set(fd, val)
	return nil
}

func toProtoScalar(fd protoreflect.FieldDescriptor, v any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, ok := v.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", v)
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(n)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(n)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(n), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := toInt64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(uint64(n)), nil
	case protoreflect.FloatKind:
		f, err := toFloat64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, err := toFloat64(v)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.StringKind:
		s, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", v)
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BytesKind:
		s, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for bytes, got %T", v)
		}
		return protoreflect.ValueOfBytes([]byte(s)), nil
	case protoreflect.EnumKind:
		switch x := v.(type) {
		case int32:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(x)), nil
		case int:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(x)), nil
		case string:
			// Rendered enums carry a type prefix on the descriptor; every other
			// surface speaks the unprefixed name, so resolve both spellings.
			ev := ir.EnumValueByExternalName(fd.Enum(), x)
			if ev == nil {
				return protoreflect.Value{}, fmt.Errorf("unknown enum value %q for %s", x, fd.Enum().FullName())
			}
			return protoreflect.ValueOfEnum(ev.Number()), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected enum int|string, got %T", v)
		}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		nested, ok := v.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected object, got %T", v)
		}
		sub := dynamicpb.NewMessage(fd.Message())
		if err := protoArgsToMessage(nested, sub); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(sub), nil
	}
	return protoreflect.Value{}, fmt.Errorf("unhandled kind: %v", fd.Kind())
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

// protoMessageToMap converts msg to map[string]any with lowerCamel
// keys. Unset fields are omitted. Numeric int64/uint64 fields are
// stringified to preserve precision (matches the Long scalar). Enums
// emit as int32 number (matches the graphql-go enum config built by
// the IR type-builder).
func protoMessageToMap(msg *dynamicpb.Message) map[string]any {
	if msg == nil {
		return map[string]any{}
	}
	fields := msg.Descriptor().Fields()
	out := make(map[string]any, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !msg.Has(fd) {
			continue
		}
		out[lowerCamel(string(fd.Name()))] = protoToAny(fd, msg.Get(fd))
	}
	return out
}

func protoToAny(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	if fd.IsList() {
		l := v.List()
		out := make([]any, l.Len())
		for i := 0; i < l.Len(); i++ {
			out[i] = scalarToAny(fd, l.Get(i))
		}
		return out
	}
	return scalarToAny(fd, v)
}

func scalarToAny(fd protoreflect.FieldDescriptor, v protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return v.Uint()
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return string(v.Bytes())
	case protoreflect.EnumKind:
		// Match the graphql-go enum config built from the IR — values
		// are int32 numbers, not name strings.
		return int32(v.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		sub, ok := v.Message().Interface().(*dynamicpb.Message)
		if !ok {
			return nil
		}
		return protoMessageToMap(sub)
	}
	return nil
}
