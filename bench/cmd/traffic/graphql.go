package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/iodesystems/gwag/v2/bench/cmd/traffic/runner"
)

// runGraphQL parses graphql-adapter flags, builds one Target per
// --target URL with a JSON-POST Fire closure, and hands off to
// runner.Run.
func runGraphQL(args []string) error {
	fs := flag.NewFlagSet("graphql", flag.ExitOnError)
	rps := fs.Int("rps", 100, "requests per second per target")
	duration := fs.Duration("duration", 30*time.Second, "test duration")
	concurrency := fs.Int("concurrency", 0, "max concurrent in-flight per target (extras are dropped); 0 = auto = max(64, rps/20)")
	shards := fs.Int("shards", 0, "driver goroutines per target; 0 = auto = ceil(rps/1500). Sub-millisecond Go tickers cap at ~3k Hz per goroutine, so anything above ~3k RPS must be sharded.")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request HTTP timeout")
	serverSide := fs.Bool("server-metrics", true, "snapshot gateway metrics before+after for the per-backend table")
	metricsPath := fs.String("metrics-path", "/api/metrics", "path the gateway exposes Prometheus metrics at; use /metrics for a raw gateway, empty to skip server-side capture")
	jsonOut := fs.String("json", "", "write the gateway-pass summary (target_rps, achieved RPS, p50/p95/p99, gateway dispatch + ingress) to PATH as JSON. Direct-pass results are not exported. PATH '-' writes to stdout.")
	query := fs.String("query", `{ greeter { hello(name: "world") { greeting } } }`, "GraphQL query string")
	directQuery := fs.String("direct-query", "", "GraphQL query string for the --direct pass. Defaults to --query — but the upstream's schema is usually unprefixed, so override (e.g. '{ hello(name:\"world\") { greeting } }') when the gateway adds a namespace.")
	var targetsRaw runner.StringFlag
	fs.Var(&targetsRaw, "target", "GraphQL endpoint URL (repeat or comma-separate for multiple)")
	var directTargetsRaw runner.StringFlag
	fs.Var(&directTargetsRaw, "direct", "upstream GraphQL endpoint URL (e.g. http://localhost:50054/graphql) to dial directly, bypassing the gateway. When set, runs a second pass and prints a side-by-side compare. Repeat or comma-separate for multiple direct targets.")
	if err := fs.Parse(args); err != nil {
		return err
	}

	targetURLs := runner.SplitCSV(targetsRaw)
	if len(targetURLs) == 0 {
		return errors.New("at least one --target is required")
	}

	body, err := json.Marshal(map[string]any{"query": *query})
	if err != nil {
		return fmt.Errorf("marshal query: %w", err)
	}

	targets := make([]runner.Target, 0, len(targetURLs))
	for _, u := range targetURLs {
		fire := runner.MakeGraphQLFire(*timeout, runner.ResolveConcurrency(*rps, *concurrency), u, body)
		targets = append(targets, runner.Target{
			Label:      u,
			MetricsURL: runner.MetricsURLWithPath(u, *metricsPath),
			Fire:       fire,
		})
	}

	directURLs := runner.SplitCSV(directTargetsRaw)
	directBody := body
	if len(directURLs) > 0 && *directQuery != "" {
		directBody, err = json.Marshal(map[string]any{"query": *directQuery})
		if err != nil {
			return fmt.Errorf("marshal direct-query: %w", err)
		}
	}
	directTargets := make([]runner.Target, 0, len(directURLs))
	for _, u := range directURLs {
		fire := runner.MakeGraphQLFire(*timeout, runner.ResolveConcurrency(*rps, *concurrency), u, directBody)
		directTargets = append(directTargets, runner.Target{
			Label: "direct " + u,
			// MetricsURL empty: gateway not in path on this pass.
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

	fmt.Fprintf(os.Stdout, "running %d req/s for %s against %d target(s)\n", *rps, duration.String(), len(targetURLs))
	gwRes, err := runner.Run(opts, ternaryStr(len(directTargets) > 0, "gateway", ""), targets)
	if err != nil {
		return err
	}
	runner.PrintPass(opts, gwRes)
	if err := writeJSONIfRequested(*jsonOut, opts, gwRes); err != nil {
		return err
	}

	if len(directTargets) == 0 {
		return nil
	}
	fmt.Fprintf(os.Stdout, "\nrunning direct pass: %d req/s for %s against %d direct target(s); bypassing gateway\n", *rps, duration.String(), len(directTargets))
	directOpts := opts
	directOpts.ServerMetrics = false
	dRes, err := runner.Run(directOpts, "direct", directTargets)
	if err != nil {
		return err
	}
	runner.PrintPass(directOpts, dRes)
	runner.PrintCompare(gwRes, dRes)
	return nil
}
