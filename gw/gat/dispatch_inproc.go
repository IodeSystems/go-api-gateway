package gat

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// inprocDispatcher invokes a captured huma handler directly. GraphQL
// args (map[string]any keyed by ir.Arg.Name) are bound into a fresh
// instance of the handler's typed input struct via reflection; the
// handler's typed output is unwrapped (huma convention: the JSON body
// lives in a field named `Body`).
type inprocDispatcher struct {
	captured       *capturedOp
	op             *ir.Operation
	bodyArg        string           // name of the body Arg, if any; empty when none
	emptyNilSlices bool             // coerce nil slices to `[]` before serializing
	observe        DispatchObserver // per-operation timing hook; nil = off
}

func newInprocDispatcher(c *capturedOp, op *ir.Operation, emptyNilSlices bool, observe DispatchObserver) ir.Dispatcher {
	var bodyArg string
	for _, a := range op.Args {
		if strings.EqualFold(a.OpenAPILocation, "body") {
			bodyArg = a.Name
			break
		}
	}
	return &inprocDispatcher{captured: c, op: op, bodyArg: bodyArg, emptyNilSlices: emptyNilSlices, observe: observe}
}

// Dispatch is the per-operation join point: every in-process GraphQL/gRPC operation invocation
// flows through here. When an observer is set it wraps the call with timing keyed by the
// operation id, so consumers can emit per-operation metrics (the AOP-pointcut analog).
func (d *inprocDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	if d.observe == nil {
		return d.dispatch(ctx, args)
	}
	start := time.Now()
	out, err := d.dispatch(ctx, args)
	d.observe(d.captured.op.OperationID, time.Since(start), err)
	return out, err
}

func (d *inprocDispatcher) dispatch(ctx context.Context, args map[string]any) (any, error) {
	inPtr := reflect.New(d.captured.inputType)
	in := inPtr.Elem()

	if err := bindInput(in, d.op, args, d.bodyArg); err != nil {
		return nil, fmt.Errorf("gat: bind input for %s: %w", d.op.Name, err)
	}

	out, err := d.captured.invoke(ctx, inPtr.Interface())
	if err != nil {
		// Surface the handler's HTTP status as GraphQL error extensions so
		// clients can classify (e.g. 401/403 → login, 5xx → error dialog).
		// Transparent to gRPC/REST (Error/Unwrap/GetStatus preserved).
		return nil, withStatusExtensions(err)
	}
	// Capture any Set-Cookie the handler emitted so the GraphQL HTTP layer
	// can write it to the response (no-op on transports without a sink).
	if sink := cookieSinkFrom(ctx); sink != nil {
		sink.addFromOutput(out)
	}
	if d.emptyNilSlices {
		// Replace nil slices with empty ones in place (the output is ephemeral)
		// so they serialize as `[]` not null — required by the non-null list
		// rendering, and a cleaner contract regardless.
		coerceNilSlices(reflect.ValueOf(out))
	}
	return normalizeJSON(extractBody(out))
}

// coerceNilSlices walks an output value and replaces every nil slice with an
// empty (non-nil) slice of the same type, so json.Marshal emits `[]` not null.
// Recurses through pointers, interfaces, structs, slice elements, and map
// values. Mutates in place — safe because the handler output is used once.
func coerceNilSlices(v reflect.Value) {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if !v.IsNil() {
			coerceNilSlices(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if f := v.Field(i); f.CanSet() {
				coerceNilSlices(f)
			}
		}
	case reflect.Slice:
		if v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			}
			return
		}
		for i := 0; i < v.Len(); i++ {
			coerceNilSlices(v.Index(i))
		}
	case reflect.Map:
		// Map values aren't addressable; nested nil slices inside map values
		// stay nil (rare in output bodies — maps render as the JSON scalar).
	}
}

// normalizeJSON round-trips v through encoding/json so the GraphQL
// runtime receives generic JSON types (map[string]any, []any, scalars)
// — the same shape proto/OpenAPI-origin dispatchers produce. Without
// this the inproc path hands graphql-go raw Go structs: custom
// json.Marshaler implementations never run, union ResolveType cannot
// read discriminator properties off a struct, and typed scalar fields
// (enums) fail to resolve.
func normalizeJSON(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("gat: marshal result: %w", err)
	}
	var norm any
	if err := json.Unmarshal(raw, &norm); err != nil {
		return nil, fmt.Errorf("gat: normalize result: %w", err)
	}
	return norm, nil
}

// bindInput maps GraphQL args into the typed huma input struct. Huma
// uses field tags for non-body locations (`path:"id"`, `query:"limit"`,
// `header:"X-Foo"`, `cookie:"sid"`); the request body is a field
// literally named `Body` whose type is the body schema.
func bindInput(in reflect.Value, op *ir.Operation, args map[string]any, bodyArg string) error {
	t := in.Type()

	// Paths, not indices: huma promotes parameters out of EXPORTED embedded
	// structs, so a tag can sit several levels down and the binder has to follow
	// it there. Unexported fields are skipped, matching huma — an unexported
	// embed hides everything inside it (see embedded_params.go).
	tagLookup := map[string][]int{} // location:name → field path
	var bodyIdx []int
	collect(t, nil, tagLookup, &bodyIdx)

	for _, a := range op.Args {
		if a.Name == bodyArg {
			continue
		}
		v, present := args[a.Name]
		if !present {
			continue
		}
		path, ok := tagLookup[strings.ToLower(a.OpenAPILocation)+":"+a.Name]
		if !ok {
			// Some huma inputs omit explicit tags for path params
			// when the field name matches the param name. Fall back
			// to a case-insensitive field-name match.
			path, ok = tagLookup["name:"+strings.ToLower(a.Name)]
		}
		if !ok {
			continue // best-effort; ignore unmappable args
		}
		dst, err := fieldByPath(in, path)
		if err != nil {
			return fmt.Errorf("arg %q: %w", a.Name, err)
		}
		if err := assignValue(dst, v); err != nil {
			return fmt.Errorf("arg %q: %w", a.Name, err)
		}
	}

	if bodyArg != "" && bodyIdx != nil {
		if v, present := args[bodyArg]; present && v != nil {
			dst, err := fieldByPath(in, bodyIdx)
			if err != nil {
				return fmt.Errorf("unmarshal body: %w", err)
			}
			if err := assignValue(dst, v); err != nil {
				return fmt.Errorf("unmarshal body: %w", err)
			}
		}
	}

	return nil
}

// collect maps every reachable parameter tag to its field path, descending into
// exported embedded structs the way huma does.
func collect(t reflect.Type, prefix []int, tagLookup map[string][]int, bodyIdx *[]int) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		path := append(append([]int{}, prefix...), i)
		if f.Name == "Body" && *bodyIdx == nil {
			*bodyIdx = path
		}
		for _, loc := range []string{"path", "query", "header", "cookie"} {
			if v, ok := f.Tag.Lookup(loc); ok {
				// huma allows `path:"id,required"` etc. — keep only the name
				// before the first comma.
				name := strings.SplitN(v, ",", 2)[0]
				if _, seen := tagLookup[loc+":"+name]; !seen {
					tagLookup[loc+":"+name] = path
				}
			}
		}
		// Untagged fallback for path params named after the field.
		key := "name:" + strings.ToLower(f.Name)
		if _, seen := tagLookup[key]; !seen {
			tagLookup[key] = path
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if f.Anonymous && ft.Kind() == reflect.Struct {
			collect(ft, path, tagLookup, bodyIdx)
		}
	}
}

// fieldByPath walks a field path, allocating any nil pointer-embedded struct on
// the way so the leaf is addressable.
func fieldByPath(v reflect.Value, path []int) (reflect.Value, error) {
	for n, i := range path {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				if !v.CanSet() {
					return reflect.Value{}, fmt.Errorf("cannot allocate embedded pointer at depth %d", n)
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, nil
}

// assignValue converts v to dst's type. It prefers direct
// assign/convert; falls back to a JSON round-trip for the simple cases
// (string→string, number→number); and applies two protojson-specific
// coercions for the cases where the simple round-trip fails:
//
//  1. A stringified number into a numeric field. protojson encodes
//     proto int64 / uint64 as JSON strings to preserve precision past
//     Number.MAX_SAFE_INTEGER; once protojson has gone map[string]any,
//     "5" is a Go string with a numeric Go destination.
//
//  2. A map[string]any into a struct, walking field-by-field. The
//     same int64-as-string issue applies recursively to body args
//     whose schema contains nested numeric fields.
//
// Both coercions are applied in the gat bind path so consumer code
// can declare Huma input structs with their natural Go types (int64
// for limits, etc.) rather than working around protojson at every
// call site.
func assignValue(dst reflect.Value, v any) error {
	if !dst.CanSet() {
		return fmt.Errorf("field not settable")
	}
	src := reflect.ValueOf(v)
	if src.IsValid() && src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return nil
	}
	if src.IsValid() && src.Type().ConvertibleTo(dst.Type()) {
		// Don't convert string ↔ numeric here — strconv handles those
		// below with bounds checks; raw Go conversion of "5" → int
		// would silently succeed only for runes (string is []byte).
		if !(src.Kind() == reflect.String && isNumericKind(dst.Kind())) {
			dst.Set(src.Convert(dst.Type()))
			return nil
		}
	}

	// Pointer destination (e.g. an optional/nullable input field typed
	// *int64): a nil source leaves it nil, otherwise allocate and assign
	// into the element so the coercions below also reach pointer fields.
	if dst.Kind() == reflect.Ptr {
		if v == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assignValue(dst.Elem(), v)
	}

	// (1) protojson-encoded number landing in a numeric Go field.
	if s, ok := v.(string); ok && isNumericKind(dst.Kind()) {
		return assignNumericFromString(dst, s)
	}

	// (2) deep walk: map[string]any → struct, applying assignValue
	// recursively so nested int64-as-strings survive.
	if m, ok := v.(map[string]any); ok {
		target := dst
		if target.Kind() == reflect.Ptr {
			if target.IsNil() {
				target.Set(reflect.New(target.Type().Elem()))
			}
			target = target.Elem()
		}
		if target.Kind() == reflect.Struct {
			return assignMapToStruct(target, m)
		}
	}

	// Slice walk.
	if sl, ok := v.([]any); ok && dst.Kind() == reflect.Slice {
		out := reflect.MakeSlice(dst.Type(), len(sl), len(sl))
		for i, item := range sl {
			if err := assignValue(out.Index(i), item); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		dst.Set(out)
		return nil
	}

	// Fallback: JSON round-trip. Covers booleans, time strings, enums,
	// and any other shape with a working UnmarshalJSON.
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst.Addr().Interface())
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func assignNumericFromString(dst reflect.Value, s string) error {
	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int %q: %w", s, err)
		}
		if dst.OverflowInt(n) {
			return fmt.Errorf("int overflow for %q", s)
		}
		dst.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("parse uint %q: %w", s, err)
		}
		if dst.OverflowUint(n) {
			return fmt.Errorf("uint overflow for %q", s)
		}
		dst.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("parse float %q: %w", s, err)
		}
		if dst.OverflowFloat(n) {
			return fmt.Errorf("float overflow for %q", s)
		}
		dst.SetFloat(n)
	default:
		return fmt.Errorf("not a numeric kind: %v", dst.Kind())
	}
	return nil
}

// assignMapToStruct walks m's keys and dispatches each value through
// assignValue into the matching struct field. Field lookup mirrors
// encoding/json: exact JSON tag name, then exact Go field name, then
// case-insensitive fallback.
func assignMapToStruct(dst reflect.Value, m map[string]any) error {
	t := dst.Type()
	byTag := map[string]int{}
	byName := map[string]int{}
	byLower := map[string]int{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		byName[f.Name] = i
		byLower[strings.ToLower(f.Name)] = i
		if tag, ok := f.Tag.Lookup("json"); ok {
			name := strings.SplitN(tag, ",", 2)[0]
			if name != "" && name != "-" {
				byTag[name] = i
			}
		}
	}
	for k, v := range m {
		idx, ok := byTag[k]
		if !ok {
			idx, ok = byName[k]
		}
		if !ok {
			idx, ok = byLower[strings.ToLower(k)]
		}
		if !ok {
			continue // ignore unknown keys (DiscardUnknown semantics)
		}
		if err := assignValue(dst.Field(idx), v); err != nil {
			return fmt.Errorf("field %q: %w", k, err)
		}
	}
	return nil
}

// extractBody pulls the JSON payload off a huma output. Huma outputs
// commonly look like `type Out struct { Body T }`; the Body field is
// the JSON response. For outputs without a Body field, return the
// dereferenced struct as-is.
func extractBody(out any) any {
	if out == nil {
		return nil
	}
	v := reflect.ValueOf(out)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}
	if body := v.FieldByName("Body"); body.IsValid() {
		return body.Interface()
	}
	return v.Interface()
}
