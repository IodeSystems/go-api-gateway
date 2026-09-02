package gateway

import (
	"github.com/IodeSystems/graphql-go/v2"

	"github.com/iodesystems/gwag/gw/ir"
)

// newProtoIRTypeBuilder builds a single *IRTypeBuilder shared across
// every proto slot. Proto FullNames are globally unique, so a merged
// Types map is collision-free; sharing the builder keeps cyclic
// references resolving to a single *graphql.Object across slots and
// across the Query / Subscription roots.
//
// Hides is applied to the merged service in-place so the type-builder
// emits Object/Input types without hidden fields. uploadType is the
// graphql Output bound to TypeRef{Builtin: ScalarUpload} args/fields
// — the gateway threads gw.UploadScalar() so proto bytes fields
// marked `(gwag.upload.v1.upload) = true` render as `Upload` rather
// than the `String` fallback the typebuilder uses when no Upload
// type is registered.
func newProtoIRTypeBuilder(slots map[poolKey]*slot, hides map[string]bool, uploadType graphql.Output) *ir.IRTypeBuilder {
	merged := &ir.Service{Types: map[string]*ir.Type{}}
	for _, slot := range slots {
		var svcs []*ir.Service
		switch slot.kind {
		case slotKindProto:
			if slot.proto == nil {
				continue
			}
			svcs = ir.IngestProto(slot.proto.file)
		case slotKindInternalProto:
			if slot.internalProto == nil {
				continue
			}
			svcs = ir.IngestProto(slot.internalProto.file)
		default:
			continue
		}
		for _, s := range svcs {
			for k, v := range s.Types {
				if _, ok := merged.Types[k]; !ok {
					merged.Types[k] = v
				}
			}
		}
	}
	if len(hides) > 0 {
		ir.Hides([]*ir.Service{merged}, hides)
	}
	return ir.NewIRTypeBuilder(merged, ir.IRTypeNaming{
		ObjectName: exportedName,
		EnumName:   exportedName,
		UnionName:  exportedName,
		InputName:  func(s string) string { return exportedName(s) + "_Input" },
		FieldName:  lowerCamel,
	}, ir.IRTypeBuilderOptions{
		UploadType: uploadType,
	})
}
