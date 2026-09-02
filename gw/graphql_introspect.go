package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// introspectionSchema is the parsed shape of the response to the
// canonical IntrospectionQuery (the same query Apollo, graphql-codegen,
// graphiql, etc. all use). Only the bits we need are modelled.
type introspectionSchema struct {
	QueryTypeName        string
	MutationTypeName     string
	SubscriptionTypeName string
	Types                map[string]*introspectionType
}

type introspectionType struct {
	Kind          string                  // SCALAR / OBJECT / INTERFACE / UNION / ENUM / INPUT_OBJECT / LIST / NON_NULL
	Name          string                  // top-level types only — non-empty when Kind is one of the named kinds
	Description   string                  //
	Fields        []*introspectionField   // OBJECT, INTERFACE
	InputFields   []*introspectionInputV  // INPUT_OBJECT
	EnumValues    []*introspectionEnumVal // ENUM
	Interfaces    []*introspectionTypeRef // OBJECT — interfaces this object implements
	PossibleTypes []*introspectionTypeRef // INTERFACE / UNION — concrete object variants
}

type introspectionField struct {
	Name        string
	Description string
	Args        []*introspectionInputV
	Type        *introspectionTypeRef
}

type introspectionInputV struct {
	Name        string
	Description string
	Type        *introspectionTypeRef
	// DefaultValue intentionally omitted from the v1 model — surfaces
	// as the GraphQL default in the remote schema; the gateway
	// passes args through verbatim, so the remote applies its own
	// defaults when fields are unset.
}

type introspectionEnumVal struct {
	Name        string
	Description string
}

// introspectionTypeRef is the recursive (LIST/NON_NULL) wrapper plus
// final named type reference. When Kind is one of LIST/NON_NULL the
// payload lives in OfType.
type introspectionTypeRef struct {
	Kind   string
	Name   string
	OfType *introspectionTypeRef
}

// fetchIntrospection sends the canonical introspection query to the
// remote endpoint and decodes the response. Returns a parsed model
// suitable for handing to newGraphQLMirror.
func fetchIntrospection(ctx context.Context, client *http.Client, endpoint string) (*introspectionSchema, error) {
	raw, err := fetchIntrospectionBytes(ctx, client, endpoint)
	if err != nil {
		return nil, err
	}
	return parseIntrospectionData(raw)
}

// fetchIntrospectionBytes returns the raw `data` JSON from running
// the canonical introspection query against `endpoint`. Used by both
// the parsing path (AddGraphQL local) and the control-plane path
// (which caches the bytes in the registry KV so other peers don't
// have to re-fetch).
func fetchIntrospectionBytes(ctx context.Context, client *http.Client, endpoint string) (json.RawMessage, error) {
	resp, err := dispatchGraphQL(ctx, client, endpoint, ir.IntrospectionQuery, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("introspection errors: %s", resp.Errors)
	}
	return resp.Data, nil
}

// parseIntrospectionData turns the JSON `data` field into an
// introspectionSchema. Kept separate from fetch so tests can drive
// the parser with hand-rolled responses.
func parseIntrospectionData(data json.RawMessage) (*introspectionSchema, error) {
	var wire struct {
		Schema struct {
			QueryType        *struct{ Name string }  `json:"queryType"`
			MutationType     *struct{ Name string }  `json:"mutationType"`
			SubscriptionType *struct{ Name string }  `json:"subscriptionType"`
			Types            []wireIntrospectionType `json:"types"`
		} `json:"__schema"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("decode __schema: %w", err)
	}
	out := &introspectionSchema{Types: map[string]*introspectionType{}}
	if wire.Schema.QueryType != nil {
		out.QueryTypeName = wire.Schema.QueryType.Name
	}
	if wire.Schema.MutationType != nil {
		out.MutationTypeName = wire.Schema.MutationType.Name
	}
	if wire.Schema.SubscriptionType != nil {
		out.SubscriptionTypeName = wire.Schema.SubscriptionType.Name
	}
	for _, t := range wire.Schema.Types {
		// Skip internal __ types; clients don't need them.
		if len(t.Name) >= 2 && t.Name[:2] == "__" {
			continue
		}
		conv := &introspectionType{
			Kind:        t.Kind,
			Name:        t.Name,
			Description: t.Description,
		}
		for _, f := range t.Fields {
			conv.Fields = append(conv.Fields, &introspectionField{
				Name:        f.Name,
				Description: f.Description,
				Args:        wireArgs(f.Args),
				Type:        wireTypeRef(f.Type),
			})
		}
		conv.InputFields = wireArgs(t.InputFields)
		for _, ev := range t.EnumValues {
			conv.EnumValues = append(conv.EnumValues, &introspectionEnumVal{
				Name:        ev.Name,
				Description: ev.Description,
			})
		}
		for _, i := range t.Interfaces {
			if r := wireTypeRef(i); r != nil {
				conv.Interfaces = append(conv.Interfaces, r)
			}
		}
		for _, p := range t.PossibleTypes {
			if r := wireTypeRef(p); r != nil {
				conv.PossibleTypes = append(conv.PossibleTypes, r)
			}
		}
		out.Types[t.Name] = conv
	}
	return out, nil
}

type wireIntrospectionType struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Fields      []struct {
		Name        string                    `json:"name"`
		Description string                    `json:"description"`
		Args        []wireIntrospectionInput  `json:"args"`
		Type        *wireIntrospectionTypeRef `json:"type"`
	} `json:"fields"`
	InputFields []wireIntrospectionInput `json:"inputFields"`
	EnumValues  []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	} `json:"enumValues"`
	Interfaces    []*wireIntrospectionTypeRef `json:"interfaces"`
	PossibleTypes []*wireIntrospectionTypeRef `json:"possibleTypes"`
}

type wireIntrospectionInput struct {
	Name        string                    `json:"name"`
	Description string                    `json:"description"`
	Type        *wireIntrospectionTypeRef `json:"type"`
}

type wireIntrospectionTypeRef struct {
	Kind   string                    `json:"kind"`
	Name   string                    `json:"name"`
	OfType *wireIntrospectionTypeRef `json:"ofType"`
}

func wireArgs(in []wireIntrospectionInput) []*introspectionInputV {
	out := make([]*introspectionInputV, 0, len(in))
	for _, a := range in {
		out = append(out, &introspectionInputV{
			Name:        a.Name,
			Description: a.Description,
			Type:        wireTypeRef(a.Type),
		})
	}
	return out
}

func wireTypeRef(r *wireIntrospectionTypeRef) *introspectionTypeRef {
	if r == nil {
		return nil
	}
	return &introspectionTypeRef{
		Kind:   r.Kind,
		Name:   r.Name,
		OfType: wireTypeRef(r.OfType),
	}
}
