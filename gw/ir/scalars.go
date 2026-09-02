package ir

import (
	"encoding/json"
	"strconv"

	"github.com/IodeSystems/graphql-go/v2"
	"github.com/IodeSystems/graphql-go/v2/language/ast"
)

// maxSafeInteger is JavaScript's Number.MAX_SAFE_INTEGER (2^53 - 1): the largest
// magnitude a JS number holds exactly. At or below it a Long round-trips losslessly
// as a JSON number; above it only a decimal string preserves precision.
const maxSafeInteger = int64(1)<<53 - 1

// longToInt64 coerces any JSON-decoded scalar form — number or decimal string — to an
// int64. This is the lenient (Jackson-style) input rule: a Long field accepts either
// wire form, so changing how Long is *emitted* never breaks a caller's *input*.
func longToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float64:
		return int64(x), true
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n, true
		}
		if f, err := x.Float64(); err == nil {
			return int64(f), true
		}
	case string:
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

// StandardScalars constructs the Long + JSON scalars used by every IR-rendered GraphQL
// schema with the legacy string Long encoding. Equivalent to StandardScalarsWith(false).
//
// Stability: stable
func StandardScalars() (long *graphql.Scalar, jsonValue *graphql.Scalar) {
	return StandardScalarsWith(false)
}

// StandardScalarsWith constructs the Long + JSON scalars. Per-source IRTypeBuilders share
// these so the final graphql.Schema sees one named instance — graphql-go rejects two
// scalars sharing a Name even when equivalently shaped.
//
// The Long scalar is lenient on input (accepts a JSON number or a decimal string,
// normalized to int64) regardless of longAsNumber. On output:
//   - longAsNumber == false (default): always a decimal string (legacy, precision-safe).
//   - longAsNumber == true: a JSON number when JS-safe (|v| <= 2^53-1), else a decimal
//     string. The wire form is then an honest promise — a number is exact, a string means
//     "too large for a JS number" — so clients type Long as `number | string`.
//
// Stability: stable
func StandardScalarsWith(longAsNumber bool) (long *graphql.Scalar, jsonValue *graphql.Scalar) {
	desc := "64-bit integer encoded as a decimal string to preserve precision past 2^53. " +
		"Accepts a JSON number or a decimal string as input."
	if longAsNumber {
		desc = "64-bit integer. Emitted as a JSON number when JS-safe (|v| <= 2^53-1) or a " +
			"decimal string when larger (to preserve precision) — type `number | string`. " +
			"Accepts either form as input."
	}
	long = graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Long",
		Description: desc,
		Serialize: func(v any) any {
			n, ok := longToInt64(v)
			if !ok {
				return nil
			}
			if longAsNumber && n <= maxSafeInteger && n >= -maxSafeInteger {
				return n
			}
			return strconv.FormatInt(n, 10)
		},
		// Normalize every accepted input form to the decimal-string the downstream
		// dispatch already binds to int64 (the stringified-Long contract).
		ParseValue: func(v any) any {
			if n, ok := longToInt64(v); ok {
				return strconv.FormatInt(n, 10)
			}
			return nil
		},
		ParseLiteral: func(v ast.Value) any {
			switch x := v.(type) {
			case *ast.IntValue:
				return x.Value
			case *ast.StringValue:
				if n, ok := longToInt64(x.Value); ok {
					return strconv.FormatInt(n, 10)
				}
			case *ast.FloatValue:
				if n, ok := longToInt64(x.Value); ok {
					return strconv.FormatInt(n, 10)
				}
			}
			return nil
		},
	})
	jsonValue = graphql.NewScalar(graphql.ScalarConfig{
		Name:         "JSON",
		Description:  "Untyped JSON value (used as a fallback for OpenAPI schemas that can't be mapped exactly).",
		Serialize:    func(v any) any { return v },
		ParseValue:   func(v any) any { return v },
		ParseLiteral: func(v ast.Value) any { return v },
	})
	return long, jsonValue
}
