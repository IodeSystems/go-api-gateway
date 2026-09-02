package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/iodesystems/gwag/v2/bench/cmd/traffic/runner"
)

// runOpenAPI parses openapi-adapter flags, fetches the gateway-rendered
// OpenAPI spec for --service, locates the operation by --operation
// (operationId), constructs an HTTP request whose path/query/body is
// built from --args, and fires it under /api/ingress/.
func runOpenAPI(args []string) error {
	fs := flag.NewFlagSet("openapi", flag.ExitOnError)
	rps := fs.Int("rps", 100, "requests per second per target")
	duration := fs.Duration("duration", 30*time.Second, "test duration")
	concurrency := fs.Int("concurrency", 0, "max concurrent in-flight per target (extras are dropped); 0 = auto = max(64, rps/20)")
	shards := fs.Int("shards", 0, "driver goroutines per target; 0 = auto = ceil(rps/1500)")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request HTTP timeout")
	serverSide := fs.Bool("server-metrics", true, "snapshot gateway metrics before+after for the per-backend table")
	metricsPath := fs.String("metrics-path", "/api/metrics", "path the gateway exposes Prometheus metrics at; use /metrics for a raw gateway, empty to skip server-side capture")
	jsonOut := fs.String("json", "", "write the gateway-pass summary to PATH as JSON; '-' for stdout")
	service := fs.String("service", "", "registered namespace (e.g. greeter or greeter:v1); required")
	operation := fs.String("operation", "", "operationId to invoke; required")
	argsJSON := fs.String("args", "{}", "JSON object: keys map to path/query parameters and the request body")
	ingressPrefix := fs.String("ingress-prefix", "/api/ingress", "URL prefix where IngressHandler is mounted on the gateway")
	var targetsRaw runner.StringFlag
	fs.Var(&targetsRaw, "target", "gateway HTTP base URL (repeat or comma-separate)")
	var directTargetsRaw runner.StringFlag
	fs.Var(&directTargetsRaw, "direct", "upstream HTTP base URL (e.g. http://localhost:9000) to dial directly, bypassing the gateway. The path resolved from --service+--operation is appended verbatim with no ingress prefix. When set, runs a second pass and prints a side-by-side compare.")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *service == "" {
		return errors.New("--service is required")
	}
	if *operation == "" {
		return errors.New("--operation is required")
	}

	httpTargets := runner.SplitCSV(targetsRaw)
	if len(httpTargets) == 0 {
		return errors.New("at least one --target is required")
	}

	ns, ver := splitServiceVer(*service)
	plan, err := fetchOpenAPIPlan(httpTargets[0], ns, ver, *operation)
	if err != nil {
		return fmt.Errorf("resolve operation: %w", err)
	}

	var argMap map[string]any
	if err := json.Unmarshal([]byte(*argsJSON), &argMap); err != nil {
		return fmt.Errorf("parse --args: %w", err)
	}

	targets := make([]runner.Target, 0, len(httpTargets))
	for _, ht := range httpTargets {
		fire, err := makeOpenAPIFire(*timeout, runner.ResolveConcurrency(*rps, *concurrency), ht, *ingressPrefix, plan, argMap)
		if err != nil {
			return err
		}
		fullURL := strings.TrimRight(ht, "/") + *ingressPrefix + plan.path
		targets = append(targets, runner.Target{
			Label:      fmt.Sprintf("%s %s", plan.method, fullURL),
			MetricsURL: runner.MetricsURLWithPath(ht, *metricsPath),
			Fire:       fire,
		})
	}

	directTargets := runner.SplitCSV(directTargetsRaw)
	var directTargetsBuilt []runner.Target
	for _, dt := range directTargets {
		// Direct dial: the upstream's own HTTP server. No ingress prefix —
		// the path comes from the OpenAPI spec verbatim.
		fire, err := makeOpenAPIFire(*timeout, runner.ResolveConcurrency(*rps, *concurrency), dt, "", plan, argMap)
		if err != nil {
			return err
		}
		fullURL := strings.TrimRight(dt, "/") + plan.path
		directTargetsBuilt = append(directTargetsBuilt, runner.Target{
			Label: fmt.Sprintf("direct %s %s", plan.method, fullURL),
			// MetricsURL empty: the gateway is not in the path on this pass.
			Fire: fire,
		})
	}

	opts := runner.Options{
		RPS:           *rps,
		Duration:      *duration,
		Concurrency:   *concurrency,
		Shards:        *shards,
		ServerMetrics: *serverSide,
	}

	fmt.Fprintf(os.Stdout, "running %d req/s for %s against %d openapi target(s); %s %s\n", *rps, duration.String(), len(targets), plan.method, plan.path)
	gwRes, err := runner.Run(opts, ternaryStr(len(directTargetsBuilt) > 0, "gateway", ""), targets)
	if err != nil {
		return err
	}
	runner.PrintPass(opts, gwRes)
	if err := writeJSONIfRequested(*jsonOut, opts, gwRes); err != nil {
		return err
	}

	if len(directTargetsBuilt) == 0 {
		return nil
	}
	fmt.Fprintf(os.Stdout, "\nrunning direct pass: %d req/s for %s against %d direct target(s); bypassing gateway\n", *rps, duration.String(), len(directTargetsBuilt))
	directOpts := opts
	directOpts.ServerMetrics = false
	dRes, err := runner.Run(directOpts, "direct", directTargetsBuilt)
	if err != nil {
		return err
	}
	runner.PrintPass(directOpts, dRes)
	runner.PrintCompare(gwRes, dRes)
	return nil
}

// opPlan describes how to construct a request from --args for a
// single operation. PathParams + QueryParams are arg-map keys consumed
// by the URL; remaining keys go in the body when hasBody.
type opPlan struct {
	method      string
	path        string // template, e.g. "/things/{id}"
	pathParams  []string
	queryParams []string
	hasBody     bool
}

// fetchOpenAPIPlan GETs /api/schema/openapi?service=<ns>(:<ver>),
// walks the response object for the operationId, and returns a plan.
func fetchOpenAPIPlan(httpBase, ns, ver, opID string) (opPlan, error) {
	u := strings.TrimRight(httpBase, "/") + "/api/schema/openapi?service=" + ns
	if ver != "" {
		u += ":" + ver
	}
	resp, err := http.Get(u)
	if err != nil {
		return opPlan{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return opPlan{}, fmt.Errorf("schema/openapi status %d", resp.StatusCode)
	}
	var byNS map[string]map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&byNS); err != nil {
		return opPlan{}, fmt.Errorf("decode openapi index: %w", err)
	}
	versions, ok := byNS[ns]
	if !ok {
		return opPlan{}, fmt.Errorf("namespace %q not in /api/schema/openapi response", ns)
	}
	var specRaw json.RawMessage
	switch {
	case ver != "":
		specRaw, ok = versions[ver]
		if !ok {
			return opPlan{}, fmt.Errorf("version %q not in /api/schema/openapi[%q]", ver, ns)
		}
	case len(versions) == 1:
		for _, v := range versions {
			specRaw = v
		}
	default:
		keys := make([]string, 0, len(versions))
		for k := range versions {
			keys = append(keys, k)
		}
		return opPlan{}, fmt.Errorf("namespace %q has multiple versions %v; pin via --service %s:vN", ns, keys, ns)
	}

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(specRaw)
	if err != nil {
		return opPlan{}, fmt.Errorf("load openapi spec: %w", err)
	}
	if doc.Paths == nil {
		return opPlan{}, fmt.Errorf("spec has no paths")
	}
	for path, item := range doc.Paths.Map() {
		if item == nil {
			continue
		}
		for method, op := range pathOperations(item) {
			if op == nil || op.OperationID != opID {
				continue
			}
			return planFromOperation(method, path, op), nil
		}
	}
	return opPlan{}, fmt.Errorf("operationId %q not found in service %q", opID, ns)
}

func pathOperations(item *openapi3.PathItem) map[string]*openapi3.Operation {
	return map[string]*openapi3.Operation{
		"GET":    item.Get,
		"POST":   item.Post,
		"PUT":    item.Put,
		"PATCH":  item.Patch,
		"DELETE": item.Delete,
	}
}

func planFromOperation(method, path string, op *openapi3.Operation) opPlan {
	plan := opPlan{method: method, path: path}
	for _, pref := range op.Parameters {
		if pref == nil || pref.Value == nil {
			continue
		}
		switch pref.Value.In {
		case "path":
			plan.pathParams = append(plan.pathParams, pref.Value.Name)
		case "query":
			plan.queryParams = append(plan.queryParams, pref.Value.Name)
		}
	}
	if op.RequestBody != nil && op.RequestBody.Value != nil {
		if _, ok := op.RequestBody.Value.Content["application/json"]; ok {
			plan.hasBody = true
		}
	}
	return plan
}

func makeOpenAPIFire(timeout time.Duration, concurrency int, httpBase, ingressPrefix string, plan opPlan, argMap map[string]any) (func(context.Context, *runner.Stats), error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = concurrency * 4
	tr.MaxIdleConnsPerHost = concurrency * 4
	tr.IdleConnTimeout = 90 * time.Second
	client := &http.Client{Timeout: timeout, Transport: tr}

	pathConsumed := map[string]bool{}
	for _, p := range plan.pathParams {
		pathConsumed[p] = true
	}

	resolvedPath := plan.path
	for _, p := range plan.pathParams {
		v, ok := argMap[p]
		if !ok {
			return nil, fmt.Errorf("--args missing required path param %q", p)
		}
		resolvedPath = strings.ReplaceAll(resolvedPath, "{"+p+"}", url.PathEscape(fmt.Sprint(v)))
	}

	// Honour the spec verbatim: declared query params come from
	// argMap; if the op declares a JSON requestBody, every remaining
	// non-path arg lands in the body. The synthesised OpenAPI now
	// emits proto-unary args as a body schema (matching IngressHandler's
	// ingressShapeProtoPost decode), so no double-send fallback.
	bodyShaped := plan.method == "POST" || plan.method == "PUT" || plan.method == "PATCH"
	q := url.Values{}
	for _, qp := range plan.queryParams {
		if v, ok := argMap[qp]; ok {
			q.Set(qp, fmt.Sprint(v))
		}
	}

	var bodyBytes []byte
	sendBody := false
	if bodyShaped && plan.hasBody {
		bodyArgs := map[string]any{}
		for k, v := range argMap {
			if pathConsumed[k] {
				continue
			}
			if q.Has(k) {
				continue
			}
			bodyArgs[k] = v
		}
		if len(bodyArgs) > 0 {
			var err error
			bodyBytes, err = json.Marshal(bodyArgs)
			if err != nil {
				return nil, fmt.Errorf("marshal body: %w", err)
			}
			sendBody = true
		}
	}

	full := strings.TrimRight(httpBase, "/") + ingressPrefix + resolvedPath
	if encoded := q.Encode(); encoded != "" {
		full += "?" + encoded
	}

	return func(ctx context.Context, s *runner.Stats) {
		var body io.Reader
		if sendBody {
			body = bytes.NewReader(bodyBytes)
		}
		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, plan.method, full, body)
		if err != nil {
			s.RecordErr(runner.ErrTransport, "build request: "+err.Error())
			return
		}
		if sendBody {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.RecordErr(runner.ErrTransport, err.Error())
			return
		}
		defer resp.Body.Close()

		statusLabel := fmt.Sprintf("%d", resp.StatusCode)
		s.RecordCode(statusLabel)
		respBody, _ := io.ReadAll(resp.Body)
		excerpt := runner.Truncate(string(respBody), 200)
		if resp.StatusCode >= 400 {
			s.RecordErr(runner.ErrHTTP, fmt.Sprintf("status=%d body=%s", resp.StatusCode, excerpt))
			s.RecordBody(statusLabel, excerpt)
			return
		}
		s.RecordBody(statusLabel, excerpt)
		s.RecordOK(elapsed)
	}, nil
}
