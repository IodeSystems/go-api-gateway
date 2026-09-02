package gat

// Direct proto → huma-input binding for the Connect/gRPC ingress.
//
// The ingress reaches the handler's typed input in two hops: walk the
// request message into a map[string]any (connect_args.go), then bind
// that map into the struct by tag (bindInput). The map is pure
// overhead — it exists only because the two ends were written against
// different representations.
//
// bindPlan removes it. Built once per operation at mount time, it
// pairs each proto field with the Go field path it feeds and writes
// values straight across.
//
// Most of what assignValue does on the map path is repair work for
// protojson's encoding: a proto int64 arrives as the string "5"
// because JSON cannot hold it, so the binder parses it back. Reading
// the message directly means an int64 field is an int64 the whole way
// and none of that is needed — which is the real win here, not the
// map allocation.
//
// The same discipline as the response encode plan applies, for the
// same reason: bindInput silently skips what it cannot map, so a
// divergence here would hand the handler a zero value rather than an
// error. The plan REFUSES to form for anything it cannot prove it
// binds identically — enums, proto maps, a Go type with a custom
// JSON/text unmarshaler — and that operation keeps the map path. The
// differential test in connect_bind_test.go pins the two together.

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// bindPlan is one operation's proto-to-input wiring: the parameter
// fields, plus the body field when the operation has one.
type bindPlan struct {
	params []bindField
	body   *bindBody
}

// bindField binds one proto field to a parameter on the input struct.
// path is a field path rather than an index because huma promotes
// parameters out of exported embedded structs.
type bindField struct {
	fd   protoreflect.FieldDescriptor
	path []int
}

// bindBody binds the body message to the input's Body field.
type bindBody struct {
	fd   protoreflect.FieldDescriptor
	path []int
	sub  *structBind
}

// structBind maps a proto message onto a Go struct by JSON property
// name — the naming the body schema and the struct tags share.
type structBind struct {
	fields []structBindField
}

type structBindField struct {
	fd  protoreflect.FieldDescriptor
	idx []int
	sub *structBind // set for message-kinded fields
}

var (
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

// buildBindPlan pairs an operation's proto input message with the Go
// input type. A nil return means "not provably equivalent" and is a
// normal outcome; the caller falls back to messageToArgs + bindInput.
func buildBindPlan(inputType reflect.Type, op *ir.Operation, bodyArg string, md protoreflect.MessageDescriptor) *bindPlan {
	if inputType == nil || op == nil || md == nil {
		return nil
	}
	if inputType.Kind() != reflect.Struct || hasCustomUnmarshaler(inputType) {
		return nil
	}

	// Same tag map bindInput builds, so the two agree on which Go field
	// a given argument lands in — including the untagged path-param
	// fallback.
	tagLookup := map[string][]int{}
	var bodyIdx []int
	collect(inputType, nil, tagLookup, &bodyIdx)

	argByName := make(map[string]*ir.Arg, len(op.Args))
	for _, a := range op.Args {
		argByName[a.Name] = a
	}

	plan := &bindPlan{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())

		if bodyArg != "" && name == bodyArg {
			if bodyIdx == nil {
				// The operation declares a body the input struct has no
				// field for. bindInput drops it; rather than replicate
				// that quietly, decline the plan.
				return nil
			}
			if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
				return nil
			}
			bodyType := derefType(fieldTypeAt(inputType, bodyIdx))
			if bodyType == nil || bodyType.Kind() != reflect.Struct {
				return nil
			}
			sub := buildStructBind(bodyType, fd.Message(), 0)
			if sub == nil {
				return nil
			}
			plan.body = &bindBody{fd: fd, path: bodyIdx, sub: sub}
			continue
		}

		a, ok := argByName[name]
		if !ok {
			// A proto field with no matching IR arg is not something
			// bindInput would have bound either, so ignoring it is
			// faithful — but only when the message is otherwise fully
			// understood, which the rest of this loop establishes.
			continue
		}

		path, ok := tagLookup[strings.ToLower(a.OpenAPILocation)+":"+a.Name]
		if !ok {
			path, ok = tagLookup["name:"+strings.ToLower(a.Name)]
		}
		if !ok {
			// bindInput skips unmappable args. Matching that is fine,
			// so leave the field out of the plan rather than refusing.
			continue
		}
		dstType := fieldTypeAt(inputType, path)
		if !bindableInto(fd, dstType) {
			return nil
		}
		plan.params = append(plan.params, bindField{fd: fd, path: path})
	}

	if len(plan.params) == 0 && plan.body == nil {
		return nil
	}
	return plan
}

// buildStructBind pairs a proto message with a Go struct by JSON name.
func buildStructBind(t reflect.Type, md protoreflect.MessageDescriptor, depth int) *structBind {
	if depth > maxPlanDepth {
		return nil
	}
	t = derefType(t)
	if t == nil || t.Kind() != reflect.Struct || hasCustomUnmarshaler(t) {
		return nil
	}
	byName := map[string][]int{}
	collectJSONFields(t, nil, byName)

	sb := &structBind{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		idx, ok := byName[string(fd.Name())]
		if !ok {
			// No Go field for this proto field: the map path would have
			// dropped it too (assignMapToStruct matches by name), so
			// skipping is faithful.
			continue
		}
		ft := derefType(fieldTypeAt(t, idx))
		if ft == nil || hasCustomUnmarshaler(ft) {
			return nil
		}
		f := structBindField{fd: fd, idx: idx}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			if fd.IsMap() {
				return nil
			}
			elem := ft
			if fd.IsList() {
				if ft.Kind() != reflect.Slice {
					return nil
				}
				elem = derefType(ft.Elem())
			}
			sub := buildStructBind(elem, fd.Message(), depth+1)
			if sub == nil {
				return nil
			}
			f.sub = sub
		} else if !bindableInto(fd, ft) {
			return nil
		}
		sb.fields = append(sb.fields, f)
	}
	if len(sb.fields) == 0 {
		return nil
	}
	return sb
}

// bindableInto reports whether a proto field can be written into a Go
// type without the coercions assignValue performs. Deliberately
// strict — an unlisted pairing refuses the plan instead of guessing.
//
// Enums are excluded on purpose: the map path renders them as value
// names and lets the JSON fallback resolve whatever the Go type wants,
// which is more behaviour than this walker should try to reproduce.
func bindableInto(fd protoreflect.FieldDescriptor, dst reflect.Type) bool {
	if dst == nil {
		return false
	}
	dst = derefType(dst)
	if dst == nil || hasCustomUnmarshaler(dst) {
		return false
	}
	if fd.IsMap() {
		return false
	}
	if fd.IsList() {
		if dst.Kind() != reflect.Slice {
			return false
		}
		// []byte is a bytes scalar, not a repeated field.
		return bindableScalar(fd.Kind(), derefType(dst.Elem()))
	}
	return bindableScalar(fd.Kind(), dst)
}

func bindableScalar(pk protoreflect.Kind, dst reflect.Type) bool {
	if dst == nil {
		return false
	}
	switch pk {
	case protoreflect.BoolKind:
		return dst.Kind() == reflect.Bool
	case protoreflect.StringKind:
		return dst.Kind() == reflect.String
	case protoreflect.BytesKind:
		return dst.Kind() == reflect.Slice && dst.Elem().Kind() == reflect.Uint8
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		switch dst.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return true
		}
		return false
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		switch dst.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return true
		}
		return false
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return dst.Kind() == reflect.Float32 || dst.Kind() == reflect.Float64
	}
	return false
}

// hasCustomUnmarshaler reports whether t (or *t) decodes itself. The
// map path routes such a type through a JSON round-trip that honours
// the method; a field walk would bypass it.
func hasCustomUnmarshaler(t reflect.Type) bool {
	if t == nil {
		return false
	}
	for _, x := range []reflect.Type{t, reflect.PointerTo(t)} {
		if x.Implements(jsonUnmarshalerType) || x.Implements(textUnmarshalerType) {
			return true
		}
	}
	return false
}

// bind writes msg into the input struct. Unpopulated proto fields are
// left alone, so whatever default the handler's struct carries
// survives — the same outcome the map path reached by omitting the key.
func (p *bindPlan) bind(in reflect.Value, msg *dynamicpb.Message) error {
	if msg == nil {
		return nil
	}
	for _, f := range p.params {
		if !msg.Has(f.fd) {
			continue
		}
		dst, err := fieldByPath(in, f.path)
		if err != nil {
			return fmt.Errorf("arg %q: %w", f.fd.Name(), err)
		}
		if err := setFromProto(dst, f.fd, msg.Get(f.fd)); err != nil {
			return fmt.Errorf("arg %q: %w", f.fd.Name(), err)
		}
	}
	if p.body != nil && msg.Has(p.body.fd) {
		dst, err := fieldByPath(in, p.body.path)
		if err != nil {
			return fmt.Errorf("body: %w", err)
		}
		if err := p.body.sub.apply(dst, msg.Get(p.body.fd).Message()); err != nil {
			return fmt.Errorf("body: %w", err)
		}
	}
	return nil
}

// apply writes a proto message into a Go struct.
func (sb *structBind) apply(dst reflect.Value, m protoreflect.Message) error {
	dst = allocDeref(dst)
	if !dst.IsValid() {
		return fmt.Errorf("unsettable destination")
	}
	for _, f := range sb.fields {
		if !m.Has(f.fd) {
			continue
		}
		target, err := fieldByPath(dst, f.idx)
		if err != nil {
			return fmt.Errorf("field %q: %w", f.fd.Name(), err)
		}
		v := m.Get(f.fd)
		if f.sub == nil {
			if err := setFromProto(target, f.fd, v); err != nil {
				return fmt.Errorf("field %q: %w", f.fd.Name(), err)
			}
			continue
		}
		if f.fd.IsList() {
			list := v.List()
			target = allocDeref(target)
			out := reflect.MakeSlice(target.Type(), list.Len(), list.Len())
			for i := 0; i < list.Len(); i++ {
				if err := f.sub.apply(out.Index(i), list.Get(i).Message()); err != nil {
					return fmt.Errorf("field %q[%d]: %w", f.fd.Name(), i, err)
				}
			}
			target.Set(out)
			continue
		}
		if err := f.sub.apply(target, v.Message()); err != nil {
			return fmt.Errorf("field %q: %w", f.fd.Name(), err)
		}
	}
	return nil
}

// setFromProto writes one proto value into a Go field, allocating
// through pointers so an optional field behaves as the map path's
// assignValue did.
func setFromProto(dst reflect.Value, fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	dst = allocDeref(dst)
	if !dst.IsValid() || !dst.CanSet() {
		return fmt.Errorf("field not settable")
	}
	if fd.IsList() {
		list := v.List()
		out := reflect.MakeSlice(dst.Type(), list.Len(), list.Len())
		for i := 0; i < list.Len(); i++ {
			if err := setScalar(out.Index(i), fd, list.Get(i)); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		dst.Set(out)
		return nil
	}
	return setScalar(dst, fd, v)
}

func setScalar(dst reflect.Value, fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	dst = allocDeref(dst)
	if !dst.IsValid() || !dst.CanSet() {
		return fmt.Errorf("field not settable")
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		dst.SetBool(v.Bool())
	case protoreflect.StringKind:
		dst.SetString(v.String())
	case protoreflect.BytesKind:
		// Copy: the proto value's backing array belongs to the message.
		b := v.Bytes()
		out := make([]byte, len(b))
		copy(out, b)
		dst.SetBytes(out)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n := v.Int()
		if dst.OverflowInt(n) {
			return fmt.Errorf("value %d overflows %s", n, dst.Type())
		}
		dst.SetInt(n)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n := v.Uint()
		if dst.OverflowUint(n) {
			return fmt.Errorf("value %d overflows %s", n, dst.Type())
		}
		dst.SetUint(n)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		f := v.Float()
		if dst.OverflowFloat(f) {
			return fmt.Errorf("value %v overflows %s", f, dst.Type())
		}
		dst.SetFloat(f)
	default:
		return fmt.Errorf("unsupported kind %s", fd.Kind())
	}
	return nil
}

// allocDeref follows pointers, allocating nils, so the returned value
// is the settable leaf.
func allocDeref(v reflect.Value) reflect.Value {
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if !v.CanSet() {
				return reflect.Value{}
			}
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}
