package gat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// outboundIdleConnsPerHost is how many keep-alive connections gat
// holds open per upstream. Go's http.DefaultClient ships
// MaxIdleConnsPerHost=2, which collapses into a fresh TCP connection
// (and handshake) per request the moment concurrency exceeds two —
// the upstream stops being the bottleneck and connection churn takes
// over. gw/ pre-bakes the same default for exactly this reason.
const outboundIdleConnsPerHost = 1024

// outboundHTTP is the client every OpenAPI dispatch forwards through.
// Package-level so all dispatchers share one connection pool: a
// per-dispatcher client would partition keep-alives by operation and
// defeat the point. Adopters needing a custom transport (proxy, mTLS)
// supply their own dispatcher via ServiceRegistration.Dispatchers.
var outboundHTTP = newOutboundHTTPClient()

func newOutboundHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = outboundIdleConnsPerHost * 4
	tr.MaxIdleConnsPerHost = outboundIdleConnsPerHost
	tr.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: tr}
}

// openAPIDispatcher implements ir.Dispatcher for one OpenAPI operation.
type openAPIDispatcher struct {
	baseURL   string
	method    string
	pathTmpl  string
	operation *openapi3.Operation
}

func newOpenAPIDispatcher(baseURL string, svc *ir.Service, op *ir.Operation) *openAPIDispatcher {
	var (
		pathTmpl  string
		method    string
		operation *openapi3.Operation
	)
	if svc.Origin != nil {
		doc := svc.Origin.(*openapi3.T)
		for p, pi := range doc.Paths.Map() {
			for m, opVal := range pi.Operations() {
				if opVal.OperationID == op.Name {
					pathTmpl = p
					method = m
					operation = opVal
					break
				}
			}
			if operation != nil {
				break
			}
		}
	}
	return &openAPIDispatcher{
		baseURL:   baseURL,
		method:    method,
		pathTmpl:  pathTmpl,
		operation: operation,
	}
}

func (d *openAPIDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	if d.operation == nil || d.pathTmpl == "" {
		return nil, fmt.Errorf("gat: openapi dispatcher not configured")
	}

	resolvedPath := d.pathTmpl
	queryArgs := url.Values{}
	for _, paramRef := range d.operation.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		p := paramRef.Value
		v, ok := args[p.Name]
		if !ok {
			continue
		}
		strVal := fmt.Sprintf("%v", v)
		switch p.In {
		case "path":
			resolvedPath = strings.ReplaceAll(resolvedPath, "{"+p.Name+"}", url.PathEscape(strVal))
		case "query":
			queryArgs.Add(p.Name, strVal)
		}
	}

	full := d.baseURL + resolvedPath
	if len(queryArgs) > 0 {
		full += "?" + queryArgs.Encode()
	}

	var body io.Reader
	if bv, ok := args["body"]; ok && bv != nil {
		b, err := json.Marshal(bv)
		if err != nil {
			return nil, fmt.Errorf("gat: marshal body: %w", err)
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, d.method, full, body)
	if err != nil {
		return nil, fmt.Errorf("gat: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	// Forward Authorization header from inbound request.
	if inbound := HTTPRequestFromContext(ctx); inbound != nil {
		if v := inbound.Header.Get("Authorization"); v != "" {
			req.Header.Set("Authorization", v)
		}
	}

	resp, err := outboundHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gat: %s %s: %w", d.method, full, err)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gat: %s %s: read body: %w", d.method, full, err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("gat: %s %s: %s: %s", d.method, full, resp.Status, strings.TrimSpace(string(respBytes)))
	}
	if len(respBytes) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return nil, fmt.Errorf("gat: decode response: %w", err)
	}
	return out, nil
}
