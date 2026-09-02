package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// ingressRoute is one resolved (METHOD, path) → dispatcher entry. The
// dispatcher pointer is captured at build time; the route table is
// rebuilt on every assembleLocked, so dispatcher churn doesn't strand
// stale entries.
//
// Three shapes today:
//
//   - ingressShapeProtoPost: exact-match path; the request JSON body
//     is the canonical args verbatim. Lookups go through
//     ingressTable.exact.
//   - ingressShapeOpenAPI: templated path (may contain {placeholders})
//     plus declared param/body locations; canonical args are
//     assembled from path segments, the URL query, and the JSON body.
//     Lookups walk ingressTable.templated until segs match.
//   - ingressShapeSubscription: GET /<pkg>.<Service>/<method> for a
//     server-streaming proto method. Args come from query params
//     (input fields); response is text/event-stream — one `data:`
//     frame per upstream event, terminating with `event: complete`
//     when the stream ends.
type ingressRoute struct {
	method     string
	path       string // exact match for proto-style; "" when templated
	schemaID   ir.SchemaID
	dispatcher ir.Dispatcher
	shape      ingressShape

	// OpenAPI-only fields. segs is the parsed pathTemplate split by
	// "/"; literal segments must equal the request, paramName segments
	// capture into args. queryParamNames lists the declared query
	// param names so single-valued matches land as strings (multi-
	// valued land as []string). hasBody is true when the op declares
	// a JSON request body — set args["body"] from the decoded payload.
	// multipartBody is true when the op's requestBody is
	// multipart/form-data; the ingress decodes form parts (Upload-typed
	// → *Upload, others → string) instead of JSON.
	segs            []routeSeg
	queryParamNames []string
	hasBody         bool
	multipartBody   bool
	formdataArgs    map[string]bool // arg name → true for OpenAPILocation="formdata" Upload-typed slots
}

type routeSeg struct {
	literal   string // empty when this segment captures
	paramName string // empty when this segment is literal
}

type ingressShape int

const (
	// ingressShapeProtoPost: POST /<pkg>.<Service>/<method> with a
	// JSON object whose top-level keys are the canonical args
	// (lowerCamel of proto fields).
	ingressShapeProtoPost ingressShape = iota

	// ingressShapeOpenAPI: route at the operation's declared
	// HTTPMethod + HTTPPath. Path placeholders, query params, and
	// the JSON body all flow into canonical args; the underlying
	// openAPIDispatcher already knows which to send where on egress.
	ingressShapeOpenAPI

	// ingressShapeSubscription: GET /<pkg>.<Service>/<method> on a
	// server-streaming proto method. Query params land in canonical
	// args (input fields). Dispatch returns a
	// chan any of decoded events that the handler streams as SSE.
	ingressShapeSubscription
)

// ingressTable holds the route set assembled for the current schema.
// Lookups are O(1) by (method, path) for exact-match routes (proto-
// style and any OpenAPI ops with no `{placeholders}`); templated
// routes are walked sequentially on exact-match miss. Per-method
// indexing keeps the templated walk small even for service-heavy
// gateways.
type ingressTable struct {
	exact     map[string]*ingressRoute   // key: METHOD + " " + path
	templated map[string][]*ingressRoute // key: METHOD; ordered list, longest-segment-prefix first
}

// rebuildIngressLocked walks every proto pool's RPCs and every
// OpenAPI source's operations, emitting one route per ingestible op
// pointing at the dispatcher already registered in g.dispatchers.
// Caller holds g.mu.
//
// Proto unary lands at POST /<pkg>.<Service>/<method>; proto server-
// streaming lands at GET on the same path (SSE response). Bidi /
// client-streaming are skipped — egress doesn't support them and
// ingress can't synthesise them. Internal namespaces (`_*` or
// AsInternal) are skipped just like the GraphQL surface skips them.
func (g *Gateway) rebuildIngressLocked() {
	t := &ingressTable{
		exact:     map[string]*ingressRoute{},
		templated: map[string][]*ingressRoute{},
	}

	// Proto-style: POST /<pkg>.<Service>/<method> for unary,
	// GET on the same path for server-streaming (text/event-stream).
	for _, slot := range g.slots {
		var fd protoreflect.FileDescriptor
		var key poolKey
		switch slot.kind {
		case slotKindProto:
			if slot.proto == nil {
				continue
			}
			fd = slot.proto.file
			key = slot.proto.key
		case slotKindInternalProto:
			if slot.internalProto == nil {
				continue
			}
			fd = slot.internalProto.file
			key = slot.key
		default:
			continue
		}
		if g.isInternal(key.namespace) {
			continue
		}
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			sd := services.Get(i)
			methods := sd.Methods()
			for j := 0; j < methods.Len(); j++ {
				md := methods.Get(j)
				if md.IsStreamingClient() {
					// bidi/client-streaming aren't routable — egress doesn't
					// support them, ingress can't synthesise them.
					continue
				}
				sid := ir.MakeSchemaID(key.namespace, key.version, string(md.Name()))
				d := g.dispatchers.Get(sid)
				if d == nil {
					continue
				}
				path := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
				if md.IsStreamingServer() {
					t.exact[http.MethodGet+" "+path] = &ingressRoute{
						method:     http.MethodGet,
						path:       path,
						schemaID:   sid,
						dispatcher: d,
						shape:      ingressShapeSubscription,
					}
					continue
				}
				t.exact[http.MethodPost+" "+path] = &ingressRoute{
					method:     http.MethodPost,
					path:       path,
					schemaID:   sid,
					dispatcher: d,
					shape:      ingressShapeProtoPost,
				}
			}
		}
	}

	// OpenAPI: route at each op's declared HTTPMethod/HTTPPath.
	for k, slot := range g.slots {
		if slot.kind != slotKindOpenAPI {
			continue
		}
		if g.isInternal(k.namespace) {
			continue
		}
		g.addOpenAPIDocRoutes(t, slot.openapi.doc, k)
	}

	// GraphQL: synthesize REST routes for stitched-graphql services
	// via the same IR→OpenAPI projection used for /api/schema/openapi.
	// Honors the IR-as-equalizer promise (any registered service is
	// reachable as REST), at the cost of routing through the
	// canonical-args dispatcher rather than the proto-style fast path
	// — fine for cross-kind which won't be hot.
	for k, slot := range g.slots {
		if slot.kind != slotKindGraphQL {
			continue
		}
		if g.isInternal(k.namespace) {
			continue
		}
		for _, svc := range slot.ir {
			doc, err := ir.RenderOpenAPI(svc)
			if err != nil || doc == nil {
				continue
			}
			g.addOpenAPIDocRoutes(t, doc, k)
		}
	}

	g.ingressRoutes.Store(t)
}

// addOpenAPIDocRoutes emits one ingressRoute per (method, path) in
// doc whose dispatcher is registered under MakeSchemaID(key, opName).
// Shared by the native-OpenAPI ingress pass and the cross-kind
// synthesis pass for stitched-graphql services.
func (g *Gateway) addOpenAPIDocRoutes(t *ingressTable, doc *openapi3.T, key poolKey) {
	if doc == nil || doc.Paths == nil {
		return
	}
	for path, item := range doc.Paths.Map() {
		if item == nil {
			continue
		}
		for _, mop := range openAPIOpsForPath(item) {
			// SchemaID is keyed on the same name IngestOpenAPI
			// uses: OperationID when set, otherwise "<METHOD><path>".
			// RenderOpenAPI sets OperationID=op.Name (post-flatten),
			// matching what PopulateSchemaIDs stamped, so the lookup
			// works for both native-OpenAPI and cross-kind synthesis.
			opName := mop.op.OperationID
			if opName == "" {
				opName = mop.method + path
			}
			sid := ir.MakeSchemaID(key.namespace, key.version, opName)
			d := g.dispatchers.Get(sid)
			if d == nil {
				continue
			}
			route := &ingressRoute{
				method:     strings.ToUpper(mop.method),
				path:       path,
				schemaID:   sid,
				dispatcher: d,
				shape:      ingressShapeOpenAPI,
			}
			route.segs = parseRouteTemplate(path)
			route.queryParamNames, route.hasBody, route.multipartBody, route.formdataArgs = openAPIArgPlan(mop.op)
			if hasParamSeg(route.segs) {
				t.templated[route.method] = append(t.templated[route.method], route)
			} else {
				t.exact[route.method+" "+path] = route
			}
		}
	}
}

// openAPIMethodOp pairs a normalized HTTP method with the OpenAPI
// operation found under it. Mirrors the verb table in
// ir.ingestOpenAPIPath so the route builder picks up exactly the
// ops IngestOpenAPI ingested.
type openAPIMethodOp struct {
	method string
	op     *openapi3.Operation
}

func openAPIOpsForPath(item *openapi3.PathItem) []openAPIMethodOp {
	out := make([]openAPIMethodOp, 0, 5)
	if item.Get != nil {
		out = append(out, openAPIMethodOp{"GET", item.Get})
	}
	if item.Post != nil {
		out = append(out, openAPIMethodOp{"POST", item.Post})
	}
	if item.Put != nil {
		out = append(out, openAPIMethodOp{"PUT", item.Put})
	}
	if item.Patch != nil {
		out = append(out, openAPIMethodOp{"PATCH", item.Patch})
	}
	if item.Delete != nil {
		out = append(out, openAPIMethodOp{"DELETE", item.Delete})
	}
	return out
}

// parseRouteTemplate splits "/things/{id}/items" into segments,
// distinguishing literal vs placeholder. Trailing/leading slashes
// are normalised away. An empty path yields a single empty literal
// segment so it matches "/" but nothing else.
func parseRouteTemplate(template string) []routeSeg {
	parts := strings.Split(strings.Trim(template, "/"), "/")
	out := make([]routeSeg, len(parts))
	for i, p := range parts {
		if len(p) >= 2 && p[0] == '{' && p[len(p)-1] == '}' {
			out[i] = routeSeg{paramName: p[1 : len(p)-1]}
		} else {
			out[i] = routeSeg{literal: p}
		}
	}
	return out
}

func hasParamSeg(segs []routeSeg) bool {
	for _, s := range segs {
		if s.paramName != "" {
			return true
		}
	}
	return false
}

// openAPIArgPlan summarises the bits of an OpenAPI operation the
// ingress arg extractor needs at request time: declared query param
// names, whether the op accepts a JSON or multipart request body, and
// which multipart properties land as Upload-typed *Upload values
// (string/format:binary). Header / cookie params are out of scope
// today — clients send the values, the egress dispatcher doesn't yet
// read them off canonical args, and adding the round-trip can wait
// for a real use case.
//
// multipart/form-data is checked before application/json: when both
// are declared the ingress prefers the multipart shape (matching IR
// ingestOpenAPIFormDataBody, so the wire format stays consistent
// across schema + dispatch + ingress).
func openAPIArgPlan(op *openapi3.Operation) (queryParams []string, hasBody, multipartBody bool, formdataUploads map[string]bool) {
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if paramRef.Value.In == "query" {
			queryParams = append(queryParams, paramRef.Value.Name)
		}
	}
	if op.RequestBody != nil && op.RequestBody.Value != nil {
		body := op.RequestBody.Value
		if mt, ok := body.Content["multipart/form-data"]; ok && mt != nil && mt.Schema != nil && mt.Schema.Value != nil {
			multipartBody = true
			formdataUploads = map[string]bool{}
			for name, propRef := range mt.Schema.Value.Properties {
				if propRef == nil || propRef.Value == nil {
					continue
				}
				if isOpenAPIBinaryProp(propRef.Value) {
					formdataUploads[name] = true
				}
			}
			return queryParams, false, true, formdataUploads
		}
		if mt, ok := body.Content["application/json"]; ok && mt != nil {
			hasBody = true
		}
	}
	return queryParams, hasBody, false, nil
}

// isOpenAPIBinaryProp reports whether a schema property describes an
// uploaded file (type:string + format:binary or :byte), matching the
// IR ingest's mapping to ScalarUpload. Array-of-binary returns true
// too — the parser substitutes []any of *Upload at that path.
func isOpenAPIBinaryProp(s *openapi3.Schema) bool {
	if s == nil {
		return false
	}
	if s.Type != nil && s.Type.Includes("string") && (s.Format == "binary" || s.Format == "byte") {
		return true
	}
	if s.Type != nil && s.Type.Includes("array") && s.Items != nil && s.Items.Value != nil {
		return isOpenAPIBinaryProp(s.Items.Value)
	}
	return false
}

// IngressHandler returns an http.Handler that accepts inbound
// requests in the registered services' native shapes:
//
//   - Proto unary: POST /<pkg>.<Service>/<method> with a JSON body
//     whose top-level fields are the canonical args.
//   - Proto server-streaming: GET /<pkg>.<Service>/<method> with
//     query params for the input fields plus hmac/timestamp/kid;
//     response is text/event-stream — one `data:` frame per event.
//   - OpenAPI: each operation's declared HTTPMethod + HTTPPath, with
//     path / query / body decoded into canonical args.
//
// All shapes go through the same ir.Dispatcher chain the GraphQL
// resolver uses, so runtime middleware (HideType, InjectType, user
// Transforms) and per-pool backpressure apply identically.
//
// Unmatched paths return JSON 404. The handler is safe to call
// before Handler() — schema assembly triggers the same way.
//
// Stability: stable
func (g *Gateway) IngressHandler() http.Handler {
	g.mu.Lock()
	if g.schema.Load() == nil {
		if err := g.assembleLocked(); err != nil {
			g.mu.Unlock()
			return errorHandler(err)
		}
	}
	g.mu.Unlock()
	return http.HandlerFunc(g.serveIngress)
}

func (g *Gateway) serveIngress(w http.ResponseWriter, r *http.Request) {
	if g.draining.Load() {
		writeIngressError(w, http.StatusServiceUnavailable, "draining")
		return
	}
	t := g.ingressRoutes.Load()
	if t == nil {
		writeIngressError(w, http.StatusNotFound, "no routes")
		return
	}

	route, pathParams := lookupIngressRoute(t, r.Method, r.URL.Path)
	if route == nil {
		writeIngressError(w, http.StatusNotFound, "no route for "+r.Method+" "+r.URL.Path)
		return
	}

	args, err := g.buildIngressArgs(r, route, pathParams)
	if err != nil {
		writeIngressError(w, http.StatusBadRequest, err.Error())
		return
	}

	ns, ver, op := route.schemaID.Parts()
	if route.shape == ingressShapeSubscription {
		// SSE subscription lifetime is open-ended; request_*_seconds is
		// not meaningful here. Skip the accumulator + recording.
		ctx := g.tracer.extractHTTP(r.Context(), r.Header)
		ctx, span := g.tracer.startIngressSpan(ctx, "gateway.http.subscription",
			ingressAttr("http"),
			namespaceAttr(ns),
			versionAttr(ver),
			methodAttr(op),
			httpMethodAttr(r.Method),
			httpTargetAttr(r.URL.Path),
			httpRouteAttr(route.path),
		)
		defer span.End()
		ctx = withTracer(ctx, g.tracer)
		ctx = withInjectCache(ctx)
		ctx = withHTTPRequest(ctx, r)
		streamSSE(ctx, w, route, args)
		return
	}

	ctx := g.tracer.extractHTTP(r.Context(), r.Header)
	ctx, span := g.tracer.startIngressSpan(ctx, "gateway.http",
		ingressAttr("http"),
		namespaceAttr(ns),
		versionAttr(ver),
		methodAttr(op),
		httpMethodAttr(r.Method),
		httpTargetAttr(r.URL.Path),
		httpRouteAttr(route.path),
	)
	defer span.End()
	ctx = withTracer(ctx, g.tracer)
	ctx, accum := withDispatchAccumulator(ctx)
	ctx = withInjectCache(ctx)
	ctx = withHTTPRequest(ctx, r)
	start := time.Now()
	defer func() {
		total := time.Since(start)
		dispatchSum := time.Duration(accum.Sum.Load())
		g.cfg.metrics.RecordRequest("http", total, total-dispatchSum)
		g.logRequestLine("http", r.URL.Path, total, dispatchSum, int(accum.Count.Load()))
	}()

	out, err := route.dispatcher.Dispatch(ctx, args)
	if err != nil {
		writeIngressDispatchError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		return
	}
}

// streamSSE dispatches a subscription and pumps the resulting event
// channel out to the client as text/event-stream. One `data:` frame
// per event (JSON-encoded payload). When the upstream channel closes,
// the handler emits `event: complete\ndata: {}` and returns. ctx
// cancels propagate down to the dispatcher, which releases stream
// slots and closes the upstream.
//
// Pre-dispatch errors (HMAC verify fail, slot-acquire timeout) come
// back as Reject — written as a regular JSON error envelope, no SSE
// frame, so HTTP status carries the failure code. Post-stream-open
// errors are vanishingly rare and already filtered by the broker;
// when they do occur, the connection just closes.
func streamSSE(ctx context.Context, w http.ResponseWriter, route *ingressRoute, args map[string]any) {
	out, err := route.dispatcher.Dispatch(ctx, args)
	if err != nil {
		writeIngressDispatchError(w, err)
		return
	}
	ch, ok := out.(chan any)
	if !ok {
		writeIngressError(w, http.StatusInternalServerError, "subscription dispatcher returned non-channel")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeIngressError(w, http.StatusInternalServerError, "streaming not supported by ResponseWriter")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				_, _ = io.WriteString(w, "event: complete\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			b, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// lookupIngressRoute returns the matching route and any captured
// path params. Exact-match wins over templated; templated routes
// are walked in registration order.
func lookupIngressRoute(t *ingressTable, method, path string) (*ingressRoute, map[string]string) {
	if r, ok := t.exact[method+" "+path]; ok {
		return r, nil
	}
	candidates := t.templated[method]
	if len(candidates) == 0 {
		return nil, nil
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for _, route := range candidates {
		if len(route.segs) != len(parts) {
			continue
		}
		params, ok := matchSegs(route.segs, parts)
		if !ok {
			continue
		}
		return route, params
	}
	return nil, nil
}

// matchSegs matches a parsed request path against a route's
// segments. Returns the captured params on match, or (nil, false)
// when a literal segment differs.
func matchSegs(segs []routeSeg, parts []string) (map[string]string, bool) {
	var params map[string]string
	for i, seg := range segs {
		if seg.paramName != "" {
			v, err := url.PathUnescape(parts[i])
			if err != nil {
				return nil, false
			}
			if params == nil {
				params = make(map[string]string, len(segs))
			}
			params[seg.paramName] = v
			continue
		}
		if seg.literal != parts[i] {
			return nil, false
		}
	}
	return params, true
}

// buildIngressArgs assembles the canonical args map for one
// dispatch. Proto-style routes pull the body in verbatim; OpenAPI
// routes mix path / query / body; subscription routes (SSE) take
// every query param as-is — input fields plus hmac/timestamp/kid.
//
// The gateway receiver gives the parser access to upload-related
// config (WithUploadLimit caps the multipart body read at the
// network layer) — proto / subscription paths ignore it.
func (g *Gateway) buildIngressArgs(r *http.Request, route *ingressRoute, pathParams map[string]string) (map[string]any, error) {
	switch route.shape {
	case ingressShapeProtoPost:
		return decodeJSONObject(r.Body)
	case ingressShapeSubscription:
		args := map[string]any{}
		for name, vs := range r.URL.Query() {
			if len(vs) == 0 {
				continue
			}
			if len(vs) == 1 {
				args[name] = vs[0]
			} else {
				ifs := make([]any, len(vs))
				for i, s := range vs {
					ifs[i] = s
				}
				args[name] = ifs
			}
		}
		return args, nil
	case ingressShapeOpenAPI:
		args := map[string]any{}
		for k, v := range pathParams {
			args[k] = v
		}
		// Only declared query params land in args — the egress
		// dispatcher only encodes declared ones, so passing extras
		// through just bloats the canonical map. Multi-valued params
		// are forwarded as []any (graphql-go's list shape).
		q := r.URL.Query()
		for _, name := range route.queryParamNames {
			vs := q[name]
			if len(vs) == 0 {
				continue
			}
			if len(vs) == 1 {
				args[name] = vs[0]
			} else {
				ifs := make([]any, len(vs))
				for i, s := range vs {
					ifs[i] = s
				}
				args[name] = ifs
			}
		}

		switch {
		case route.multipartBody:
			// Multipart/form-data ingress: each declared form property
			// becomes a top-level canonical arg; binary properties land
			// as *Upload (single) or []any of *Upload (array). Other
			// properties pass through as strings; non-declared parts are
			// dropped silently — same approach as queryParamNames above.
			if err := decodeMultipartFormArgs(r, route, args, g.cfg.uploadLimit); err != nil {
				return nil, err
			}
		case route.hasBody && r.ContentLength != 0:
			body, err := decodeJSONAny(r.Body)
			if err != nil {
				return nil, err
			}
			if body != nil {
				args["body"] = body
			}
		}
		return args, nil
	}
	return nil, fmt.Errorf("unsupported ingress shape %d", route.shape)
}

// decodeMultipartFormArgs parses an inbound multipart/form-data POST
// at the HTTP ingress (gateway's re-exposed REST surface for ingested
// OpenAPI services). Binary parts substitute into args as *Upload;
// non-binary parts pass through as strings (or []any for repeated
// keys). Arrays of binary parts (e.g. `files[]`) substitute as
// []any of *Upload at the same arg name.
//
// uploadLimit > 0 caps the total parsed body at that many bytes via
// http.MaxBytesReader; the response on overshoot is the multipart
// reader's natural error, surfaced as 400. The dispatcher closes any
// captured File readers when done.
func decodeMultipartFormArgs(r *http.Request, route *ingressRoute, args map[string]any, uploadLimit int64) error {
	contentType, ctParams, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if contentType != "multipart/form-data" {
		return fmt.Errorf("multipart: expected multipart/form-data, got %q", contentType)
	}
	boundary := ctParams["boundary"]
	if boundary == "" {
		return fmt.Errorf("multipart: missing boundary")
	}

	body := io.Reader(r.Body)
	if uploadLimit > 0 {
		// MaxBytesReader sets a 413 on the writer when exceeded, but
		// since we surface our own JSON error envelope downstream we
		// pass a nil writer — the limit is enforced via the reader
		// returning an error on overshoot.
		body = http.MaxBytesReader(nil, r.Body, uploadLimit)
	}
	mr := multipart.NewReader(body, boundary)
	const inMemory = 32 << 20 // per-part in-memory threshold; bigger spills to tempfile.
	form, err := mr.ReadForm(inMemory)
	if err != nil {
		return fmt.Errorf("multipart: read form: %w", err)
	}
	// Non-file form values pass through as strings; multi-valued
	// fields land as []any to preserve repeated-key semantics.
	for k, vs := range form.Value {
		if len(vs) == 0 {
			continue
		}
		if len(vs) == 1 {
			args[k] = vs[0]
		} else {
			arr := make([]any, len(vs))
			for i, v := range vs {
				arr[i] = v
			}
			args[k] = arr
		}
	}
	// File parts substitute into *Upload values. Only parts whose name
	// appears in route.formdataArgs are recognised as Upload-typed; an
	// unexpected file part for a non-binary slot is rejected so the
	// adopter sees the mismatch immediately rather than silently
	// dropping the bytes.
	for k, fhs := range form.File {
		if !route.formdataArgs[k] {
			return fmt.Errorf("multipart: file part %q does not correspond to an Upload-typed property", k)
		}
		ups := make([]*Upload, 0, len(fhs))
		for _, fh := range fhs {
			f, err := fh.Open()
			if err != nil {
				return fmt.Errorf("multipart: open file %q: %w", k, err)
			}
			ups = append(ups, &Upload{
				Filename:    fh.Filename,
				ContentType: fh.Header.Get("Content-Type"),
				Size:        fh.Size,
				File:        f,
			})
		}
		if len(ups) == 1 {
			args[k] = ups[0]
		} else {
			arr := make([]any, len(ups))
			for i, u := range ups {
				arr[i] = u
			}
			args[k] = arr
		}
	}
	return nil
}

// decodeJSONObject reads up to 10 MiB of body and decodes a JSON
// object into a map. Empty body / EOF returns an empty map. Bodies
// that decode to a non-object are rejected.
func decodeJSONObject(r io.Reader) (map[string]any, error) {
	const limit = 10 << 20
	dec := json.NewDecoder(io.LimitReader(r, limit+1))
	dec.UseNumber()
	var args map[string]any
	if err := dec.Decode(&args); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("decode body: %w", err)
	}
	if args == nil {
		args = map[string]any{}
	}
	return args, nil
}

// decodeJSONAny reads up to 10 MiB and decodes into any JSON value
// (object, array, primitive). Empty body returns nil.
func decodeJSONAny(r io.Reader) (any, error) {
	const limit = 10 << 20
	dec := json.NewDecoder(io.LimitReader(r, limit+1))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("decode body: %w", err)
	}
	return v, nil
}

// writeIngressError writes a JSON error envelope and the given status.
// Mirrors what the GraphQL handler returns on rejections so codegen
// clients can share their error-decode path.
func writeIngressError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

// writeIngressDispatchError maps a Reject (or arbitrary error) onto an
// HTTP status. Uses the gateway's Code enum the same way the OpenAPI
// dispatcher's classification does — keeps ingress + egress error
// codes symmetric. Surfaces RetryAfter as the standard HTTP header
// when set on the rejection (today only WithQuota does so).
func writeIngressDispatchError(w http.ResponseWriter, err error) {
	var rej *rejection
	if errors.As(err, &rej) {
		if rej.RetryAfter > 0 {
			secs := int(rej.RetryAfter / time.Second)
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", fmt.Sprintf("%d", secs))
		}
		writeIngressError(w, codeToHTTPStatus(rej.Code), rej.Msg)
		return
	}
	writeIngressError(w, http.StatusInternalServerError, err.Error())
}

// codeToHTTPStatus is the inverse of httpStatusToCode in openapi.go —
// classifies a gateway Code onto the most appropriate HTTP status for
// ingress error envelopes.
func codeToHTTPStatus(c Code) int {
	switch c {
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodePermissionDenied:
		return http.StatusForbidden
	case CodeResourceExhausted:
		return http.StatusTooManyRequests
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}
