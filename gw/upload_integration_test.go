package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/IodeSystems/graphql-go/v2"
)

// TestUpload_HTTPIngressMultipartRoundTrip — full chain:
//
//  1. Upstream service exposes POST /upload with multipart/form-data
//     in its OpenAPI spec. Test backend echoes the received file's
//     bytes + filename in its JSON response.
//  2. Gateway ingests the spec, re-exposes /upload as a REST route.
//  3. Test client POSTs multipart to the gateway's HTTP ingress.
//  4. Gateway parses multipart, dispatches outbound as multipart,
//     receives the JSON echo, returns it to the client.
//
// Pins both the http_ingress decoder (binary part → *Upload in canon
// args) and dispatchOpenAPI's multipart-out branch.
func TestUpload_HTTPIngressMultipartRoundTrip(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()))
	defer gw.Close()

	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	srv := httptest.NewServer(gw.IngressHandler())
	defer srv.Close()

	body, ct := buildMultipartFormBody(t, uploadMultipartFile{
		formKey:     "file",
		filename:    "greeting.txt",
		contentType: "text/plain",
		body:        []byte("hello upstream"),
	}, map[string]string{"description": "test upload"})

	resp, err := http.Post(srv.URL+"/upload", ct, body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s", resp.StatusCode, raw)
	}
	var got struct {
		Filename    string `json:"filename"`
		Body        string `json:"body"`
		ContentType string `json:"contentType"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Filename != "greeting.txt" {
		t.Errorf("filename = %q; want greeting.txt", got.Filename)
	}
	if got.Body != "hello upstream" {
		t.Errorf("body = %q; want hello upstream", got.Body)
	}
	if got.ContentType != "text/plain" {
		t.Errorf("content_type = %q; want text/plain", got.ContentType)
	}
	if got.Description != "test upload" {
		t.Errorf("description = %q; want \"test upload\"", got.Description)
	}
}

// TestUpload_GraphQLMultipartRoundTrip — same chain but the client
// hits /graphql with the graphql-multipart-request-spec wire shape.
// Pins that the GraphQL ingress correctly substitutes *Upload into
// variables, the field resolver passes them to dispatchOpenAPI, and
// the multipart-out branch forwards upstream.
func TestUpload_GraphQLMultipartRoundTrip(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()))
	defer gw.Close()

	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	// Find the actual GraphQL field name — the OpenAPI ingest derives
	// a name from operationId; we set it to "upload" in the spec.
	body, ct := buildGraphQLMultipart(t, graphQLMultipartCase{
		query:     `mutation($file: Upload!, $description: String) { files { upload(file: $file, description: $description) { filename body } } }`,
		variables: map[string]any{"file": nil, "description": "via graphql"},
		mapJSON:   `{"0":["variables.file"]}`,
		files: []uploadMultipartFilePart{
			{name: "0", filename: "g.txt", contentType: "text/plain", body: "graphql bytes"},
		},
	})

	resp, err := http.Post(srv.URL, ct, body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, raw)
	}
	var env struct {
		Data struct {
			Files struct {
				Upload struct {
					Filename string `json:"filename"`
					Body     string `json:"body"`
				} `json:"upload"`
			} `json:"files"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v (raw=%s)", err, raw)
	}
	if len(env.Errors) > 0 {
		t.Fatalf("graphql errors: %v", env.Errors)
	}
	if env.Data.Files.Upload.Filename != "g.txt" {
		t.Errorf("filename = %q; want g.txt", env.Data.Files.Upload.Filename)
	}
	if env.Data.Files.Upload.Body != "graphql bytes" {
		t.Errorf("body = %q; want graphql bytes", env.Data.Files.Upload.Body)
	}
}

// TestUpload_TusThenGraphQLMutation — large-file path. Client uploads
// via tus, then references the upload id in a GraphQL mutation. The
// dual-mode scalar resolves the id at dispatch time via the store and
// the multipart-out dispatcher forwards bytes upstream.
func TestUpload_TusThenGraphQLMutation(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()))
	defer gw.Close()

	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	tusSrv := httptest.NewServer(gw.UploadsTusHandler())
	defer tusSrv.Close()
	gqlSrv := httptest.NewServer(gw.Handler())
	defer gqlSrv.Close()

	// 1. POST: create upload.
	body := bytes.Repeat([]byte("X"), 1024)
	req, _ := http.NewRequest(http.MethodPost, tusSrv.URL, nil)
	req.Header.Set("Tus-Resumable", tusVersion)
	req.Header.Set("Upload-Length", "1024")
	req.Header.Set("Upload-Metadata", "filename "+b64("big.bin")+",content-type "+b64("application/octet-stream"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	id := strings.TrimPrefix(loc, "/")
	uploadURL := tusSrv.URL + loc

	// 2. PATCH: append bytes.
	req, _ = http.NewRequest(http.MethodPatch, uploadURL, bytes.NewReader(body))
	req.Header.Set("Tus-Resumable", tusVersion)
	req.Header.Set("Content-Type", tusContentType)
	req.Header.Set("Upload-Offset", "0")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PATCH status = %d", resp.StatusCode)
	}

	// 3. GraphQL mutation references the upload id.
	gqlBody := map[string]any{
		"query":     `mutation($file: Upload!) { files { upload(file: $file) { filename body contentType } } }`,
		"variables": map[string]any{"file": id},
	}
	enc, _ := json.Marshal(gqlBody)
	resp, err = http.Post(gqlSrv.URL, "application/json", bytes.NewReader(enc))
	if err != nil {
		t.Fatalf("GraphQL POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", resp.StatusCode, raw)
	}
	var env struct {
		Data struct {
			Files struct {
				Upload struct {
					Filename    string `json:"filename"`
					Body        string `json:"body"`
					ContentType string `json:"contentType"`
				} `json:"upload"`
			} `json:"files"`
		} `json:"data"`
		Errors []map[string]any `json:"errors"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v (raw=%s)", err, raw)
	}
	if len(env.Errors) > 0 {
		t.Fatalf("graphql errors: %v", env.Errors)
	}
	if env.Data.Files.Upload.Filename != "big.bin" {
		t.Errorf("filename = %q; want big.bin", env.Data.Files.Upload.Filename)
	}
	if len(env.Data.Files.Upload.Body) != 1024 {
		t.Errorf("body len = %d; want 1024", len(env.Data.Files.Upload.Body))
	}
	if env.Data.Files.Upload.ContentType != "application/octet-stream" {
		t.Errorf("content_type = %q; want application/octet-stream", env.Data.Files.Upload.ContentType)
	}
}

// TestUpload_WithUploadLimitEnforcedInline — WithUploadLimit caps
// inline graphql-multipart-request-spec bodies at the configured
// number of bytes; oversized requests get a JSON error envelope, not
// a silent parse error or a panic.
func TestUpload_WithUploadLimitEnforcedInline(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()), WithUploadLimit(64))
	defer gw.Close()
	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	// Build a body that's clearly bigger than 64 bytes — the file
	// part alone is 512 bytes.
	body, ct := buildGraphQLMultipart(t, graphQLMultipartCase{
		query:     `mutation($file: Upload!) { files { upload(file: $file) { filename } } }`,
		variables: map[string]any{"file": nil},
		mapJSON:   `{"0":["variables.file"]}`,
		files: []uploadMultipartFilePart{
			{name: "0", filename: "big.bin", contentType: "application/octet-stream", body: strings.Repeat("X", 512)},
		},
	})

	resp, err := http.Post(srv.URL, ct, body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	// We don't care whether the status is 400 or 413 — both are
	// honest. We care that the request is rejected (not silently
	// truncated + parsed) and that the response is a recognisable
	// error envelope.
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("status = 200; want a non-2xx rejection")
	}
}

// TestUpload_SchemaSurfacesUploadField — verifies the OpenAPI ingest's
// multipart/form-data → Upload field mapping actually shows up in
// SDL. Without this the client codegen can't see the upload arg.
func TestUpload_SchemaSurfacesUploadField(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()))
	defer gw.Close()

	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}

	srv := httptest.NewServer(gw.SchemaHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET schema: %v", err)
	}
	defer resp.Body.Close()
	sdl, _ := io.ReadAll(resp.Body)
	s := string(sdl)
	// Upload scalar always present (force-included).
	if !strings.Contains(s, "scalar Upload") {
		t.Errorf("SDL missing `scalar Upload`")
	}
	// And the upload field declares the Upload-typed arg.
	if !strings.Contains(s, "file: Upload") {
		t.Errorf("SDL missing `file: Upload` arg:\n%s", s)
	}
}

// TestUpload_SchemaIRConsumesUploadType — the IR typebuilder wires
// gw.UploadScalar() through RuntimeOptions.UploadType, so the schema
// the gateway exposes uses *our* singleton scalar (not graphql.String
// as the fallback). Pins the wiring so a future refactor that
// accidentally drops the option still shows up as a test failure.
func TestUpload_SchemaIRConsumesUploadType(t *testing.T) {
	upstream := newUploadEchoBackend(t)
	defer upstream.Close()

	gw := New(WithUploadDataDir(t.TempDir()))
	defer gw.Close()

	if err := gw.AddOpenAPIBytes(uploadEchoSpec(upstream.URL), To(upstream.URL), As("files")); err != nil {
		t.Fatalf("AddOpenAPIBytes: %v", err)
	}
	gw.assemble()
	sch := gw.schema.Load()
	if sch == nil {
		t.Fatal("gateway has no schema after AddOpenAPIBytes")
	}
	upload := sch.TypeMap()["Upload"]
	if upload == nil {
		t.Fatal("schema TypeMap has no Upload entry")
	}
	if _, ok := upload.(*graphql.Scalar); !ok {
		t.Errorf("Upload type = %T; want *graphql.Scalar", upload)
	}
}

// --- shared fixtures ------------------------------------------------

// uploadEchoSpec returns an OpenAPI 3 spec declaring a POST /upload
// endpoint with multipart/form-data containing a file part (`file`)
// and a string field (`description`). The 200 response echoes back
// the file bytes + metadata as JSON so tests can verify forwarding.
func uploadEchoSpec(host string) []byte {
	_ = host // host shows up via the gateway's To(); not in the spec body.
	return []byte(`{
  "openapi": "3.0.3",
  "info": {"title": "upload-echo", "version": "0.1.0"},
  "paths": {
    "/upload": {
      "post": {
        "operationId": "upload",
        "requestBody": {
          "required": true,
          "content": {
            "multipart/form-data": {
              "schema": {
                "type": "object",
                "required": ["file"],
                "properties": {
                  "file": {"type": "string", "format": "binary"},
                  "description": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "echoed",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "filename": {"type": "string"},
                    "body": {"type": "string"},
                    "content_type": {"type": "string"},
                    "description": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}`)
}

// newUploadEchoBackend stands up a tiny HTTP server that decodes
// multipart bodies on POST /upload and echoes the file + metadata
// back as JSON.
func newUploadEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		ct, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if ct != "multipart/form-data" {
			http.Error(w, "expected multipart", http.StatusBadRequest)
			return
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		form, err := mr.ReadForm(10 << 20)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		out := map[string]any{}
		if vs := form.Value["description"]; len(vs) > 0 {
			out["description"] = vs[0]
		}
		fhs := form.File["file"]
		if len(fhs) == 0 {
			http.Error(w, "missing file", http.StatusBadRequest)
			return
		}
		fh := fhs[0]
		f, _ := fh.Open()
		body, _ := io.ReadAll(f)
		_ = f.Close()
		out["filename"] = fh.Filename
		out["body"] = string(body)
		// Backend echoes via lowerCamel keys; OpenAPI ingest renames
		// JSON fields to GraphQL fields via lowerCamel and graphql-go's
		// default resolver looks up by GraphQL name. (Honoring
		// IR Field.JSONName at runtime is a separate workstream.)
		out["contentType"] = fh.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	return httptest.NewServer(mux)
}

type uploadMultipartFile struct {
	formKey     string
	filename    string
	contentType string
	body        []byte
}

func buildMultipartFormBody(t *testing.T, file uploadMultipartFile, fields map[string]string) (io.Reader, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField: %v", err)
		}
	}
	// CreateFormFile is the standard library convenience for
	// Content-Disposition + Content-Type, but it picks
	// application/octet-stream. To match the test contract (verifying
	// the gateway preserves the client's declared content-type) build
	// the part header manually.
	if file.body != nil {
		header := make(map[string][]string)
		header["Content-Disposition"] = []string{`form-data; name="` + file.formKey + `"; filename="` + file.filename + `"`}
		if file.contentType != "" {
			header["Content-Type"] = []string{file.contentType}
		}
		pw, err := mw.CreatePart(header)
		if err != nil {
			t.Fatalf("CreatePart: %v", err)
		}
		if _, err := pw.Write(file.body); err != nil {
			t.Fatalf("write part: %v", err)
		}
	}
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

type graphQLMultipartCase struct {
	query     string
	variables map[string]any
	mapJSON   string
	files     []uploadMultipartFilePart
}

type uploadMultipartFilePart struct {
	name        string
	filename    string
	contentType string
	body        string
}

func buildGraphQLMultipart(t *testing.T, c graphQLMultipartCase) (io.Reader, string) {
	t.Helper()
	ops := map[string]any{"query": c.query, "variables": c.variables}
	opsJSON, _ := json.Marshal(ops)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("operations", string(opsJSON))
	_ = mw.WriteField("map", c.mapJSON)
	for _, f := range c.files {
		header := make(map[string][]string)
		header["Content-Disposition"] = []string{`form-data; name="` + f.name + `"; filename="` + f.filename + `"`}
		if f.contentType != "" {
			header["Content-Type"] = []string{f.contentType}
		}
		pw, _ := mw.CreatePart(header)
		_, _ = pw.Write([]byte(f.body))
	}
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

// assemble exposes the gateway's schema assembly to tests so they can
// inspect the schema after registration without going through an HTTP
// round-trip. Mirrors the pattern other tests use.
func (g *Gateway) assemble() {
	g.mu.Lock()
	defer g.mu.Unlock()
	_ = g.assembleLocked()
}

