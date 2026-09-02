package gateway

import (
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoFieldInfo caches a field's descriptor + its graphql arg key
// (lowerCamel of the proto name). lowerCamel runs once per descriptor
// instead of once per dispatch — saved alloc per field per call.
type protoFieldInfo struct {
	fd      protoreflect.FieldDescriptor
	jsonKey string
}

// fieldInfoCache memoises per-MessageDescriptor field metadata. Keyed
// by descriptor identity (the interface value), not FullName: two
// gateways in the same process can register the same .proto and hold
// distinct MessageDescriptor instances with identical FullNames —
// dynamicpb.Set rejects a FieldDescriptor whose parent descriptor
// doesn't match the receiver, so cross-gateway sharing breaks
// dispatch on the second gateway.
var fieldInfoCache sync.Map // map[protoreflect.MessageDescriptor][]protoFieldInfo

func fieldInfosFor(md protoreflect.MessageDescriptor) []protoFieldInfo {
	if v, ok := fieldInfoCache.Load(md); ok {
		return v.([]protoFieldInfo)
	}
	fields := md.Fields()
	out := make([]protoFieldInfo, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		out[i] = protoFieldInfo{
			fd:      fd,
			jsonKey: lowerCamel(string(fd.Name())),
		}
	}
	if v, loaded := fieldInfoCache.LoadOrStore(md, out); loaded {
		return v.([]protoFieldInfo)
	}
	return out
}

// argsToMessage walks `args` (graphql-decoded JSON-ish: map[string]any,
// []any, primitives) and writes them into `msg` according to its
// descriptor. Unknown args are ignored; type mismatches return errors.
//
// Unmaterialised *Upload values on bytes fields are silently skipped:
// the proto dispatcher's materializeUploadArgs replaces them with
// []byte before this runs; the canonical-chain wrapper (synth-message
// path used by openapi/graphql ingest with runtime middleware) has
// no upload store and shouldn't fail when an Upload arg flows
// through — preserving the *Upload in the args map lets the inner
// dispatcher consume it.
func argsToMessage(args map[string]any, msg *dynamicpb.Message) error {
	for _, info := range fieldInfosFor(msg.Descriptor()) {
		v, ok := args[info.jsonKey]
		if !ok {
			continue
		}
		if _, isUpload := v.(*Upload); isUpload {
			continue
		}
		if err := setField(msg, info.fd, v); err != nil {
			return fmt.Errorf("field %s: %w", info.fd.Name(), err)
		}
	}
	return nil
}

func setField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, v any) error {
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
		switch x := v.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(x), nil
		case string:
			return protoreflect.ValueOfBytes([]byte(x)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected string or []byte for bytes, got %T", v)
		}
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
		if err := argsToMessage(nested, sub); err != nil {
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
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, err
		}
		return n, nil
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

// messageToMap converts a dynamicpb.Message to map[string]any for
// graphql to serialise. Unset fields are omitted; lists become []any;
// nested messages recurse.
func messageToMap(msg *dynamicpb.Message) map[string]any {
	if msg == nil {
		return map[string]any{}
	}
	infos := fieldInfosFor(msg.Descriptor())
	out := make(map[string]any, len(infos))
	for _, info := range infos {
		if !msg.Has(info.fd) {
			continue
		}
		out[info.jsonKey] = protoToAny(info.fd, msg.Get(info.fd))
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
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		// Stringified to preserve precision (matches schema mapping above).
		return strconv.FormatInt(v.Int(), 10)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return string(v.Bytes())
	case protoreflect.EnumKind:
		// Proto enums register with graphql-go as `Value: int32(number)`
		// (see types.go:enumFromDescriptor). graphql-go's enum
		// Serialize matches the resolver value against that config
		// value by typed equality, so we MUST return int32 here —
		// returning the name as a string yields null on the wire.
		return int32(v.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		sub, ok := v.Message().Interface().(*dynamicpb.Message)
		if !ok {
			return nil
		}
		return messageToMap(sub)
	}
	return nil
}
