package ir

import (
	"fmt"
	"sort"

	"github.com/getkin/kin-openapi/openapi3"
)

// RenderOpenAPI projects svc onto an OpenAPI 3.0 document.
// Same-kind shortcut (KindOpenAPI Origin) emits the source spec
// verbatim. Cross-kind synthesizes from the canonical fields:
//
//   - Operation.HTTPMethod / .HTTPPath drive the path + verb when
//     present; cross-kind ingest from proto sets HTTPMethod="POST"
//     and HTTPPath="/<package>.<Service>/<Method>" by default.
//   - Args land as parameters (by OpenAPILocation when set; else
//     query for primitives, "body" for object refs).
//   - Output renders as the 200 response schema.
//   - Service.Types map onto components/schemas. Object Types
//     become object schemas; Enum Types become string-enum schemas.
//   - oneOf / interface / map render lossy when not natively
//     representable.
//
// Stability: stable
func RenderOpenAPI(svc *Service) (*openapi3.T, error) {
	if svc.OriginKind == KindOpenAPI {
		if doc, ok := svc.Origin.(*openapi3.T); ok && doc != nil {
			return doc, nil
		}
	}
	title := svc.Namespace
	if svc.Version != "" {
		title = svc.Namespace + " " + svc.Version
	}
	doc := &openapi3.T{
		OpenAPI: "3.0.0",
		Info: &openapi3.Info{
			Title:       title,
			Version:     svc.Version,
			Description: svc.Description,
		},
		Paths: &openapi3.Paths{},
		Components: &openapi3.Components{
			Schemas: openapi3.Schemas{},
		},
	}

	// Components first so $ref resolution lines up.
	for _, name := range stableTypeOrder(svc) {
		t := svc.Types[name.Name]
		if t == nil {
			continue
		}
		switch t.TypeKind {
		case TypeObject, TypeInput:
			doc.Components.Schemas[t.Name] = renderOpenAPIObject(t)
		case TypeEnum:
			doc.Components.Schemas[t.Name] = renderOpenAPIEnum(t)
		case TypeUnion:
			doc.Components.Schemas[t.Name] = renderOpenAPIUnion(t)
		}
	}

	// Operations. Streaming operations (Subscription / client-
	// streaming proto methods) drop here — OpenAPI has no native
	// streaming shape and the gateway's existing /api/schema/openapi
	// path was unary-only. Same-kind shortcut already preserves
	// these for OpenAPI-origin services (which never have them
	// anyway). Service.Groups flatten via FlatOperations so nested
	// namespaces collapse to underscored op names + synthesized
	// paths.
	for _, op := range svc.FlatOperations() {
		if op.Kind == OpSubscription || op.StreamingClient {
			continue
		}
		path := op.HTTPPath
		method := op.HTTPMethod
		if path == "" {
			// Synthetic path: proto-origin services use the
			// gRPC-over-HTTP convention `/<package>.<Service>/<Method>`
			// when the package can be reconstructed from
			// Namespace/Version; everything else falls back to
			// `/<NamespaceOrService>/<op>`.
			pkg := svc.Namespace
			if svc.Version != "" {
				pkg = svc.Namespace + "." + svc.Version
			}
			svcName := svc.ServiceName
			if svcName == "" {
				svcName = "Service"
			}
			if pkg != "" {
				path = fmt.Sprintf("/%s.%s/%s", pkg, svcName, op.Name)
			} else {
				path = fmt.Sprintf("/%s/%s", svcName, op.Name)
			}
		}
		if method == "" {
			// Proto-origin operations always render as POST under
			// the gRPC-over-HTTP convention; the proto OpKind
			// (Query vs Mutation) doesn't carry transport
			// semantics. Non-proto cross-kind synthesis honors
			// OpKind.
			switch {
			case op.OriginKind == KindProto:
				method = "POST"
			case op.Kind == OpQuery:
				method = "GET"
			default:
				method = "POST"
			}
		}
		oop := renderOpenAPIOp(svc, op, method)
		item := doc.Paths.Value(path)
		if item == nil {
			item = &openapi3.PathItem{}
			doc.Paths.Set(path, item)
		}
		switch method {
		case "GET":
			item.Get = oop
		case "POST":
			item.Post = oop
		case "PUT":
			item.Put = oop
		case "PATCH":
			item.Patch = oop
		case "DELETE":
			item.Delete = oop
		}
	}
	return doc, nil
}

func renderOpenAPIObject(t *Type) *openapi3.SchemaRef {
	if t.OriginKind == KindOpenAPI {
		if r, ok := t.Origin.(*openapi3.SchemaRef); ok && r != nil {
			return r
		}
	}
	props := openapi3.Schemas{}
	required := []string{}
	for _, f := range t.Fields {
		props[f.Name] = renderOpenAPIFieldSchema(f)
		if f.Required {
			required = append(required, f.Name)
		}
	}
	return &openapi3.SchemaRef{
		Value: &openapi3.Schema{
			Type:        &openapi3.Types{"object"},
			Description: t.Description,
			Properties:  props,
			Required:    required,
		},
	}
}

// renderOpenAPIUnion projects a TypeUnion into an OpenAPI oneOf
// schema with one $ref per Variant. Same-kind shortcut returns the
// captured *openapi3.SchemaRef when present so discriminator
// metadata round-trips verbatim. On the synthesis path the canonical
// DiscriminatorProperty / DiscriminatorMapping fields populate
// schema.discriminator.
func renderOpenAPIUnion(t *Type) *openapi3.SchemaRef {
	if t.OriginKind == KindOpenAPI {
		if r, ok := t.Origin.(*openapi3.SchemaRef); ok && r != nil {
			return r
		}
	}
	refs := make([]*openapi3.SchemaRef, 0, len(t.Variants))
	for _, v := range t.Variants {
		refs = append(refs, &openapi3.SchemaRef{Ref: "#/components/schemas/" + v})
	}
	schema := &openapi3.Schema{
		Description: t.Description,
		OneOf:       refs,
	}
	if t.DiscriminatorProperty != "" {
		d := &openapi3.Discriminator{PropertyName: t.DiscriminatorProperty}
		if len(t.DiscriminatorMapping) > 0 {
			d.Mapping = map[string]openapi3.MappingRef{}
			for k, name := range t.DiscriminatorMapping {
				d.Mapping[k] = openapi3.MappingRef{Ref: "#/components/schemas/" + name}
			}
		}
		schema.Discriminator = d
	}
	return &openapi3.SchemaRef{Value: schema}
}

func renderOpenAPIEnum(t *Type) *openapi3.SchemaRef {
	if t.OriginKind == KindOpenAPI {
		if r, ok := t.Origin.(*openapi3.SchemaRef); ok && r != nil {
			return r
		}
	}
	enumVals := make([]any, len(t.Enum))
	for i, ev := range t.Enum {
		enumVals[i] = ev.Name
	}
	return &openapi3.SchemaRef{
		Value: &openapi3.Schema{
			Type:        &openapi3.Types{"string"},
			Description: t.Description,
			Enum:        enumVals,
		},
	}
}

// renderOpenAPIArgFieldSchema is the *Arg counterpart of
// renderOpenAPIFieldSchema. Used by the proto-unary requestBody
// synthesis path where args collapse into one body schema.
func renderOpenAPIArgFieldSchema(a *Arg) *openapi3.SchemaRef {
	if a.Repeated {
		inner := renderOpenAPIRefForType(a.Type)
		return &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:  &openapi3.Types{"array"},
				Items: inner,
			},
		}
	}
	if a.Type.IsMap() {
		val := renderOpenAPIRefForType(a.Type.Map.ValueType)
		return &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type: &openapi3.Types{"object"},
				AdditionalProperties: openapi3.AdditionalProperties{
					Schema: val,
				},
			},
		}
	}
	return renderOpenAPIRefForType(a.Type)
}

func renderOpenAPIFieldSchema(f *Field) *openapi3.SchemaRef {
	if f.Repeated {
		inner := renderOpenAPIRefForType(f.Type)
		return &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:  &openapi3.Types{"array"},
				Items: inner,
			},
		}
	}
	if f.Type.IsMap() {
		val := renderOpenAPIRefForType(f.Type.Map.ValueType)
		return &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type: &openapi3.Types{"object"},
				AdditionalProperties: openapi3.AdditionalProperties{
					Schema: val,
				},
			},
		}
	}
	return renderOpenAPIRefForType(f.Type)
}

func renderOpenAPIRefForType(r TypeRef) *openapi3.SchemaRef {
	if r.IsNamed() {
		return &openapi3.SchemaRef{Ref: "#/components/schemas/" + r.Named}
	}
	if r.IsMap() {
		return &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type: &openapi3.Types{"object"},
				AdditionalProperties: openapi3.AdditionalProperties{
					Schema: renderOpenAPIRefForType(r.Map.ValueType),
				},
			},
		}
	}
	return primitiveOpenAPI(r.Builtin)
}

func primitiveOpenAPI(s ScalarKind) *openapi3.SchemaRef {
	switch s {
	case ScalarBool:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"boolean"}}}
	case ScalarBytes:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"string"}, Format: "byte"}}
	case ScalarInt32, ScalarUInt32:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"integer"}, Format: "int32"}}
	case ScalarInt64, ScalarUInt64:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"integer"}, Format: "int64"}}
	case ScalarFloat:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"number"}, Format: "float"}}
	case ScalarDouble:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"number"}, Format: "double"}}
	case ScalarTimestamp:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"string"}, Format: "date-time"}}
	case ScalarID, ScalarString:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"string"}}}
	case ScalarUpload:
		return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"string"}, Format: "binary"}}
	}
	return &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &openapi3.Types{"string"}}}
}

func renderOpenAPIOp(svc *Service, op *Operation, method string) *openapi3.Operation {
	if op.OriginKind == KindOpenAPI {
		if oop, ok := op.Origin.(*openapi3.Operation); ok && oop != nil {
			return oop
		}
	}
	out := &openapi3.Operation{
		OperationID: op.Name,
		Description: op.Description,
		Tags:        op.Tags,
		Responses:   openapi3.NewResponses(),
	}
	// Carry source-of-truth + annotation metadata as extensions. Same-
	// kind OpenAPI ops return their origin verbatim above (keeping any
	// author-set extensions); this covers proto/GraphQL-origin renders.
	if op.Ref != "" {
		out.Extensions = map[string]any{xRefExtension: op.Ref}
	}
	if len(op.Annotations) > 0 {
		if out.Extensions == nil {
			out.Extensions = map[string]any{}
		}
		out.Extensions[xAnnotationsExtension] = annotationsToExt(op.Annotations)
	}

	// Proto-origin unary RPCs land at ingressShapeProtoPost in the
	// HTTP ingress (see http_ingress.go), which reads canonical args
	// from the JSON body verbatim — top-level keys are the args.
	// Mirror that on the OpenAPI export so codegen clients / bench
	// adapters send the same shape the gateway actually decodes:
	// collapse every arg into one requestBody schema instead of
	// emitting them as parameters[in=query]. Streaming proto methods
	// (server-streaming → SSE GET) keep the per-arg query shape — the
	// SSE ingress reads them out of r.URL.Query.
	if op.OriginKind == KindProto && op.Kind != OpSubscription && len(op.Args) > 0 {
		props := openapi3.Schemas{}
		var required []string
		for _, a := range op.Args {
			props[a.Name] = renderOpenAPIArgFieldSchema(a)
			if a.Required {
				required = append(required, a.Name)
			}
		}
		out.RequestBody = &openapi3.RequestBodyRef{
			Value: &openapi3.RequestBody{
				Required: len(required) > 0,
				Content: openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: &openapi3.SchemaRef{
							Value: &openapi3.Schema{
								Type:       &openapi3.Types{"object"},
								Properties: props,
								Required:   required,
							},
						},
					},
				},
			},
		}
	} else if op.MultipartBody {
		// Round-trip the multipart/form-data shape: every formdata arg
		// becomes a property; binary props re-emit as
		// string/format:binary. Path/query/header args stay as
		// Parameters (a multipart op may still have those).
		props := openapi3.Schemas{}
		var required []string
		for _, a := range op.Args {
			if a.OpenAPILocation != "formdata" {
				out.Parameters = append(out.Parameters, &openapi3.ParameterRef{
					Value: &openapi3.Parameter{
						Name:        a.Name,
						In:          a.OpenAPILocation,
						Required:    a.Required,
						Description: a.Description,
						Schema:      renderOpenAPIRefForType(a.Type),
					},
				})
				continue
			}
			props[a.Name] = renderOpenAPIArgFieldSchema(a)
			if a.Required {
				required = append(required, a.Name)
			}
		}
		out.RequestBody = &openapi3.RequestBodyRef{
			Value: &openapi3.RequestBody{
				Required: len(required) > 0,
				Content: openapi3.Content{
					"multipart/form-data": &openapi3.MediaType{
						Schema: &openapi3.SchemaRef{
							Value: &openapi3.Schema{
								Type:       &openapi3.Types{"object"},
								Properties: props,
								Required:   required,
							},
						},
					},
				},
			},
		}
	} else {
		for _, a := range op.Args {
			switch a.OpenAPILocation {
			case "body", "":
				// Default to body when location unset and the type is
				// non-primitive; query for primitives.
				if a.Type.IsNamed() || a.OpenAPILocation == "body" {
					out.RequestBody = &openapi3.RequestBodyRef{
						Value: &openapi3.RequestBody{
							Required:    a.Required,
							Description: a.Description,
							Content: openapi3.Content{
								"application/json": &openapi3.MediaType{
									Schema: renderOpenAPIRefForType(a.Type),
								},
							},
						},
					}
					continue
				}
				out.Parameters = append(out.Parameters, &openapi3.ParameterRef{
					Value: &openapi3.Parameter{
						Name:        a.Name,
						In:          "query",
						Required:    a.Required,
						Description: a.Description,
						Schema:      renderOpenAPIRefForType(a.Type),
					},
				})
			default:
				out.Parameters = append(out.Parameters, &openapi3.ParameterRef{
					Value: &openapi3.Parameter{
						Name:        a.Name,
						In:          a.OpenAPILocation,
						Required:    a.Required,
						Description: a.Description,
						Schema:      renderOpenAPIRefForType(a.Type),
					},
				})
			}
		}
	}
	if op.Output != nil {
		desc := "ok"
		out.Responses.Set("200", &openapi3.ResponseRef{
			Value: &openapi3.Response{
				Description: &desc,
				Content: openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: renderOpenAPIRefForType(*op.Output),
					},
				},
			},
		})
	} else {
		desc := "ok"
		out.Responses.Set("200", &openapi3.ResponseRef{
			Value: &openapi3.Response{Description: &desc},
		})
	}
	_ = sort.StringSlice(nil) // keep import usage stable; sort referenced via stableTypeOrder elsewhere
	return out
}
