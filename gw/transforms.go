package gateway

import (
	"reflect"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

// HideType returns a SchemaRewrite that strips every field/arg whose
// IR-named type matches Go type T from every Object/Input type and
// Operation.Args across the schema.
//
// Resolution of T → IR type name:
//   - Pointer-to-proto-message (e.g. *authpb.Context): the message's
//     proto FullName, which equals the IR Type.Named for proto-origin
//     services.
//   - Other Go types: the package-qualified name (e.g. "x/y/pkg.Foo").
//     Won't match anything in IR until a non-proto path registers a
//     binding for the same name.
//
// Stability: stable
func HideType[T any]() SchemaRewrite {
	return hideTypeRewrite{Name: irNameForGoType[T]()}
}

// hideTypeRewrite is the concrete SchemaRewrite returned by HideType.
// Exported so renderers operating below the IR (e.g. the proto FDS
// exporter post-processing the same-kind shortcut) can recover the
// hidden type name.
type hideTypeRewrite struct {
	Name string
}

func (h hideTypeRewrite) apply(svcs []*ir.Service) {
	if h.Name == "" {
		return
	}
	ir.Hides(svcs, map[string]bool{h.Name: true})
}

// irNameForGoType derives the IR-named-type string for Go type T.
func irNameForGoType[T any]() string {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	pt := rt
	if pt.Kind() != reflect.Ptr {
		pt = reflect.PointerTo(pt)
	}
	if pt.Implements(protoMessageType) {
		zero := reflect.New(pt.Elem()).Interface().(proto.Message)
		return string(zero.ProtoReflect().Descriptor().FullName())
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if pkg := rt.PkgPath(); pkg != "" {
		return pkg + "." + rt.Name()
	}
	return rt.Name()
}

// applySchemaRewrites runs every SchemaRewrite from the gateway's
// Transforms over `svcs` in registration order. Caller holds g.mu.
func (g *Gateway) applySchemaRewrites(svcs []*ir.Service) {
	for _, tx := range g.transforms {
		for _, rw := range tx.Schema {
			rw.apply(svcs)
		}
	}
}

// hiddenTypeNames returns every IR type name registered via HideType
// across the gateway's Transforms. Used by the proto FDS exporter and
// the proto IRTypeBuilder, which both work outside the IR-service walk
// that applySchemaRewrites covers. Caller holds g.mu.
func (g *Gateway) hiddenTypeNames() []string {
	var out []string
	for _, tx := range g.transforms {
		for _, rw := range tx.Schema {
			if h, ok := rw.(hideTypeRewrite); ok && h.Name != "" {
				out = append(out, h.Name)
			}
		}
	}
	return out
}

// hidePathRewrite is the concrete SchemaRewrite returned by InjectPath
// under Hide(true). It strips a single named arg from one specific
// operation, identified by the namespace.op.arg path. The match
// applies to every version of the namespace.
type hidePathRewrite struct {
	Path string // "namespace.op.arg"
}

func (h hidePathRewrite) apply(svcs []*ir.Service) {
	ns, op, arg, ok := splitInjectPath(h.Path)
	if !ok {
		return
	}
	for _, svc := range svcs {
		if svc.Namespace != ns {
			continue
		}
		stripPathArgFromOps(svc.Operations, op, arg)
		for _, grp := range svc.Groups {
			stripPathArgFromGroup(grp, op, arg)
		}
	}
}

func stripPathArgFromOps(ops []*ir.Operation, opName, arg string) {
	for _, op := range ops {
		if op.Name != opName {
			continue
		}
		n := 0
		for _, a := range op.Args {
			if a.Name == arg {
				continue
			}
			op.Args[n] = a
			n++
		}
		op.Args = op.Args[:n]
	}
}

func stripPathArgFromGroup(grp *ir.OperationGroup, op, arg string) {
	stripPathArgFromOps(grp.Operations, op, arg)
	for _, sub := range grp.Groups {
		stripPathArgFromGroup(sub, op, arg)
	}
}

// splitInjectPath parses "namespace.op.arg" into its three segments.
// Returns ok=false on malformed input.
func splitInjectPath(path string) (ns, op, arg string, ok bool) {
	parts := strings.Split(path, ".")
	if len(parts) != 3 {
		return "", "", "", false
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// nullableTypeRewrite is the concrete SchemaRewrite emitted by
// InjectType[T] under Nullable(true). It flips the Required flag on
// every arg/field whose IR-named type matches Name, leaving the type
// otherwise intact.
type nullableTypeRewrite struct {
	Name string
}

func (n nullableTypeRewrite) apply(svcs []*ir.Service) {
	if n.Name == "" {
		return
	}
	for _, svc := range svcs {
		for _, t := range svc.Types {
			if t.TypeKind != ir.TypeObject && t.TypeKind != ir.TypeInput {
				continue
			}
			for _, f := range t.Fields {
				if f.Type.IsNamed() && f.Type.Named == n.Name {
					f.Required = false
				}
			}
		}
		nullableArgsOfType(svc.Operations, n.Name)
		for _, grp := range svc.Groups {
			nullableArgsOfTypeInGroup(grp, n.Name)
		}
	}
}

func nullableArgsOfType(ops []*ir.Operation, name string) {
	for _, op := range ops {
		for _, a := range op.Args {
			if a.Type.IsNamed() && a.Type.Named == name {
				a.Required = false
			}
		}
	}
}

func nullableArgsOfTypeInGroup(grp *ir.OperationGroup, name string) {
	nullableArgsOfType(grp.Operations, name)
	for _, sub := range grp.Groups {
		nullableArgsOfTypeInGroup(sub, name)
	}
}

// nullablePathRewrite is the concrete SchemaRewrite emitted by
// InjectPath under Nullable(true). It flips the Required flag on the
// one named arg in the matching op across every version of the
// namespace.
type nullablePathRewrite struct {
	Path string
}

func (n nullablePathRewrite) apply(svcs []*ir.Service) {
	ns, opName, arg, ok := splitInjectPath(n.Path)
	if !ok {
		return
	}
	for _, svc := range svcs {
		if svc.Namespace != ns {
			continue
		}
		nullableArgInOps(svc.Operations, opName, arg)
		for _, grp := range svc.Groups {
			nullableArgInGroup(grp, opName, arg)
		}
	}
}

func nullableArgInOps(ops []*ir.Operation, opName, arg string) {
	for _, op := range ops {
		if op.Name != opName {
			continue
		}
		for _, a := range op.Args {
			if a.Name == arg {
				a.Required = false
			}
		}
	}
}

func nullableArgInGroup(grp *ir.OperationGroup, opName, arg string) {
	nullableArgInOps(grp.Operations, opName, arg)
	for _, sub := range grp.Groups {
		nullableArgInGroup(sub, opName, arg)
	}
}
