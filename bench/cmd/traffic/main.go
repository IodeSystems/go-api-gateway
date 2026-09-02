// Traffic generator for the gateway bench stack.
//
// Subcommands pick the wire format that hits the gateway:
//
//	traffic graphql --target http://gw/api/graphql --query '{...}' ...
//	traffic grpc    --target http://gw         --grpc-target gw:50090 --service ns[:vN] --method M --args '{...}' ...
//	traffic openapi --target http://gw         --service ns[:vN] --operation OP --args '{...}' ...
//
// Each adapter shares a runner that drives the rate loop, snapshots
// /api/metrics before+after for the per-backend table, and prints
// client + server summary blocks. Errors are categorized so the
// summary explains *what* failed rather than just how many.
//
// All three formats target the same registered service through
// different ingresses on the gateway: GraphQL via gw.Handler(), gRPC
// via gw.GRPCUnknownHandler(), HTTP/JSON via gw.IngressHandler() —
// all dispatching through the same IR. Apples-to-apples per-format
// gateway cost.
package main

import (
	"fmt"
	"os"

	"github.com/iodesystems/gwag/v2/bench/cmd/traffic/runner"
)

// writeJSONIfRequested writes runner.WriteJSON to the path (or stdout
// when path is "-"). No-op when path is empty. Wraps the file
// handling so each adapter (graphql/grpc/openapi) is one line.
func writeJSONIfRequested(path string, opts runner.Options, res runner.PassResult) error {
	if path == "" {
		return nil
	}
	if path == "-" {
		return runner.WriteJSON(os.Stdout, opts, res)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("--json: %w", err)
	}
	defer f.Close()
	if err := runner.WriteJSON(f, opts, res); err != nil {
		return fmt.Errorf("--json encode: %w", err)
	}
	return nil
}

const usage = `usage: traffic <graphql|grpc|openapi> [flags]

Subcommands:
  graphql  POST {query} to /api/graphql
  grpc     unary RPC via gateway gRPC ingress (gw.GRPCUnknownHandler)
  openapi  HTTP/JSON via gateway IngressHandler (typically /api/ingress/...)

Each subcommand shares: --rps, --duration, --concurrency, --timeout,
--target, --server-metrics, --metrics-path. Run 'traffic <sub> --help'
for details.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	sub := os.Args[1]
	rest := os.Args[2:]
	var err error
	switch sub {
	case "graphql":
		err = runGraphQL(rest)
	case "grpc":
		err = runGRPC(rest)
	case "openapi":
		err = runOpenAPI(rest)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", sub, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
