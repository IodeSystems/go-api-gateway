package gateway

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/IodeSystems/graphql-go/v2"
	"github.com/IodeSystems/graphql-go/v2/language/ast"
)

// Upload is the Go-side value of a GraphQL Upload scalar. It carries
// the metadata + body of a file the gateway received via one of two
// wire formats:
//
//   - graphql-multipart-request-spec (single-request small files):
//     File holds the streaming reader for the part body; Filename /
//     ContentType / Size come from the multipart header.
//
//   - tus.io chunked upload (large / resumable files): TusID holds
//     the upload id; Filename / ContentType come from the tus
//     Upload-Metadata header at create time. File is nil until
//     Open(ctx, store) materialises it.
//
// Callers (dispatchers forwarding the upload upstream) use
// (*Upload).Open(ctx, gw.cfg.uploadStore) to get an io.ReadCloser
// regardless of which form delivered the file; the caller closes the
// returned reader when finished. Size is the wire-reported byte
// count; -1 means unknown.
//
// Stability: stable
type Upload struct {
	Filename    string
	ContentType string
	Size        int64
	File        io.ReadCloser
	// TusID is set when the value arrived as a string upload-id (the
	// client uploaded via the tus endpoint first and is referencing
	// the result). Mutually exclusive with File: parsers populate one
	// or the other.
	TusID string
}

// Open returns an io.ReadCloser over the upload body. Inline-uploaded
// values (File set) return File directly; tus-staged values (TusID
// set) materialise the body from store. The caller closes the
// returned reader.
//
// Calling Open on a tus-staged value with a nil store returns a
// configuration error so misconfigured gateways fail loud rather than
// silently failing the dispatch.
//
// Stability: stable
func (u *Upload) Open(ctx context.Context, store UploadStore) (io.ReadCloser, error) {
	if u == nil {
		return nil, fmt.Errorf("Upload.Open: nil receiver")
	}
	if u.File != nil {
		return u.File, nil
	}
	if u.TusID == "" {
		return nil, fmt.Errorf("Upload.Open: neither File nor TusID set")
	}
	if store == nil {
		return nil, fmt.Errorf("Upload.Open: tus upload-id %q referenced but no UploadStore configured (use WithUploadStore / WithUploadDataDir)", u.TusID)
	}
	staged, err := uploadFromStore(ctx, store, u.TusID)
	if err != nil {
		return nil, fmt.Errorf("Upload.Open: tus id %q: %w", u.TusID, err)
	}
	// Backfill metadata from the staged record so dispatchers see the
	// filename / content-type the client declared at tus POST time
	// (the *Upload from ParseValue had no way to know them).
	if u.Filename == "" {
		u.Filename = staged.Filename
	}
	if u.ContentType == "" {
		u.ContentType = staged.ContentType
	}
	if u.Size <= 0 {
		u.Size = staged.Size
	}
	return staged.File, nil
}

// uploadScalar is built once per process — graphql-go rejects two
// scalars with the same Name in a single Schema, so every schema
// build references this shared instance.
var (
	uploadScalarOnce sync.Once
	uploadScalarInst *graphql.Scalar
)

// UploadScalar returns the shared `Upload` GraphQL scalar. Mutations
// that accept files declare a variable of type `Upload!`; the
// multipart-request-spec parser substitutes a *Upload value at the
// corresponding path before execution.
//
// Stability: stable
func UploadScalar() *graphql.Scalar {
	uploadScalarOnce.Do(func() {
		uploadScalarInst = graphql.NewScalar(graphql.ScalarConfig{
			Name: "Upload",
			Description: "The `Upload` scalar represents a file streamed in via " +
				"the graphql-multipart-request-spec wire format " +
				"(https://github.com/jaydenseric/graphql-multipart-request-spec). " +
				"Input-only: serialization back to JSON returns null.",
			Serialize: func(any) any {
				// Output position is undefined for Upload — clients that
				// echo an Upload back in a response payload get null.
				return nil
			},
			ParseValue: func(v any) any {
				// Variable coercion path. Two wire shapes land here:
				//   1. *Upload from the multipart-spec parser (inline
				//      single-request file part) — identity pass-through.
				//   2. string from JSON variables — a tus upload-id the
				//      client staged via the tus endpoints. We wrap it
				//      in a *Upload{TusID: …} that defers store-side
				//      open() to dispatch time (graphql-go's ParseValue
				//      has no context for an in-line lookup).
				// Anything else is a hard error so a misconfigured
				// client doesn't silently drop the file.
				if v == nil {
					return nil
				}
				switch x := v.(type) {
				case *Upload:
					return x
				case string:
					if x == "" {
						return fmt.Errorf("Upload: empty string is not a valid tus upload-id")
					}
					return &Upload{TusID: x}
				}
				return fmt.Errorf("Upload: expected *Upload (multipart) or string (tus upload-id), got %T", v)
			},
			ParseLiteral: func(v ast.Value) any {
				// Upload literals in the query document are a hand-rolled
				// shape — `mutation { upload(file: "<tus-id>") }` —
				// rare but supported for the same string-id form
				// ParseValue accepts. Inline multipart can't appear in
				// a literal; that path always goes through variables.
				if sv, ok := v.(*ast.StringValue); ok && sv != nil && sv.Value != "" {
					return &Upload{TusID: sv.Value}
				}
				return nil
			},
		})
	})
	return uploadScalarInst
}
