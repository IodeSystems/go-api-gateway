# gwag — Agent Instructions

A Go library + binary that fronts four kinds of upstream services
under a single typed GraphQL surface:

- **gRPC services** described by `.proto`, registered via
  `AddProto[Descriptor]` or the gRPC control plane.
- **OpenAPI 3.x services** described by a JSON/YAML spec, registered
  via `AddOpenAPI[Bytes]` or the same control plane (proto field
  `ServiceBinding.openapi_spec`).
- **Existing GraphQL services** stitched in via `AddGraphQL`
  (boot-time introspection + namespace-prefixed type mirror).
- **MCP servers** ingested via `AddMCP` (boot-time `tools/list`
  introspection; each tool → a GraphQL Mutation). Tools-only;
  stdio / Streamable-HTTP / SSE transports.

Multi-gateway clustering via embedded NATS; subscriptions via NATS
pub/sub with HMAC channel auth.

## Build

```
bin/build              # full build: UI codegen + Vite + Go (idempotent)
bin/build --no-ui      # Go only (UI unchanged)
bin/build --no-go      # UI only
bin/build --no-codegen # skip GraphQL schema/codegen step
```

`bin/build` is the single entry point. Do NOT run `go build ./...` directly
without first ensuring `ui/dist/` exists — `ui/embed.go` has
`//go:embed all:dist` and will fail. `bin/build` seeds `ui/dist/` from
`ui/fallback/index.html` if the Vite build hasn't run or fails.

## Verify

```
go vet ./...
go test ./gw/...       # main library + tests (~22s full suite)
go test ./gw/ir/...    # IR package only
go test -run TestName  # single test
```

No linter config (.golangci, etc.) — just `go vet`. No Makefile.

## Proto Generation

Generated files in `gw/proto/*/v1/*.pb.go` and `examples/*/gen/**`.
Never edit. Regenerate with:

```
PATH=".bin:$PATH" protoc \
  --proto_path=. --proto_path=gw/proto \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  <proto_file>
```

protoc and go-grpc plugin binaries live in `.bin/`.

## UI (React Admin)

- Located in `ui/`, uses pnpm (v11.0.8), Vite, TypeScript, MUI.
- `cd ui && pnpm run gen` — fetches GraphQL schema from gateway `--gen`
  mode, then runs graphql-codegen → `src/api/gateway.ts`.
- `cd ui && pnpm run build` — Vite build + tsc check → `ui/dist/`.
- `cd ui && pnpm dev` — dev server (separate from gateway).
- `ui/dist/` is gitignored; embedded into Go binary via `ui/embed.go`.

## Run Examples

```
cd examples/multi && ./run.sh          # single gateway + greeter + library
cd examples/multi && ./run-cluster.sh  # 3-node cluster
```

The example gateway (`examples/multi/cmd/gateway/main.go`) is the
canonical wiring reference: HTTP mux, control plane, admin huma router,
UI embed, self-ingested OpenAPI, NATS cluster, graceful drain.

## Binary

```
bin/gwag --proto foo.proto=foo-svc:50051 --addr :8080
bin/gwag up                            # zero-config: NATS + admin + UI
REBUILD=1 bin/gwag up                  # force a fresh build first
```

`bin/gwag` runs a per-arch cached build under `bin/.run/`; first run
(or `REBUILD=1`) compiles `./gw/cmd/gwag`. Equivalent to
`go run ./gw/cmd/gwag` but skips recompilation on every invocation.
For the real admin UI, run `bin/build` first — `bin/gwag` only seeds
the `ui/fallback/` placeholder when `ui/dist/` is empty.

Subcommands: `peer list/forget`, `services list`, `schema fetch/diff`,
`sign`, `login/logout/context/use`.

## Architecture Essentials

- **Library**: `gw/` — import as `gateway "github.com/iodesystems/gwag/v2/gw"`.
- **IR layer**: `gw/ir/` — format-agnostic intermediate representation.
  Ingest (proto/OpenAPI/GraphQL) happens once at registration; schema
  rebuild reads `slot.ir` directly.
- **Slots**: single occupant per `(namespace, version)`. `registerSlotLocked`
  enforces tier policy: `unstable` swaps, `vN` locked, cross-kind rejects.
- **Admin operations**: defined via huma → OpenAPI → self-ingested.
  See `gw/admin_huma.go`. Dogfoods the same OpenAPI path external services take.
- **Subscriptions**: NATS pub/sub, not server-streaming gRPC dispatch.
  AsyncAPI export was considered and dropped — GraphQL SDL +
  Subscription types is the client-facing schema for events.
- **Auto-internal**: namespaces starting with `_` are hidden from public schema.
- **Cluster**: embedded NATS + JetStream KV. Reconcilers sync slots across nodes.
- **Per-pool isolation is sacred** — no gateway-wide caps on unary dispatch
  beyond per-pool `MaxInflight`.
- **Proto/gRPC is canonical** for service-to-service. GraphQL is client-facing;
  OpenAPI and downstream-GraphQL ingestion are bridges for legacy/external services.
- **Admin auth != service auth** — the boot-token + AdminAuthorizer delegate is
  *only* for gateway admin endpoints. Nothing to do with inter-service auth or
  outbound auth to upstreams (`WithOpenAPIClient` / `ForwardHeaders`).
- **Proto ingest is raw-source only** — `protocompile` pipeline with
  `SourceInfoStandard` so comments survive into SDL and MCP corpus.
  Control plane ships raw `.proto` bytes, not compiled FileDescriptorSet.
- **ServiceOption** applies to every registration entry point
  (`AddProto`, `AddOpenAPI`, `AddGraphQL`, `AddMCP`). Options: `To`,
  `As`, `Version`, `AsInternal`, `ForwardHeaders`, `OpenAPIClient`,
  `ProtoImports`. (`AddMCP` reads `As` / `Version` / `AsInternal`;
  transport + target are positional.)
- **MCP ingest is boot-time, tools-only** — `AddMCP` runs `tools/list`
  once at registration via the `mark3labs/mcp-go` client; `slot.kind`
  is `mcp`, the slot owns the live client (a subprocess for stdio).
  No control-plane path. `ir.IngestMCP` takes raw JSON so `gw/ir`
  carries no mcp-go dependency.

## Test Gotchas

- Cluster tests use `StartCluster` with `127.0.0.1:0` ports.
- `startClusterTracking` captures its ctx as parent of watch + reconciler
  goroutines — pass `context.Background()`, NOT `WithTimeout`, or goroutines
  die mid-test. See `cluster_dispatch_test.go`.
- Fixtures: httptest for OpenAPI/GraphQL forwarding; in-process `grpc.Server`
  for gRPC; in-process `grpc.ClientConnInterface` fakes for delegate testing.

## Measuring

Two directories, two questions. The names used to be `bench/` and
`compare/`, which are synonyms and told you nothing:

- **`bench/`** — gwag *against itself*. Load generator, RPS sweep with
  knee detection, Prometheus + Grafana, pprof capture. Renders
  `docs/perf.md`. Driven by `bin/bench`.
- **`compare/`** — gwag and gat *against other tools*. Renders
  `compare/comparison.md` (Apollo Router, graphql-mesh, gat vs gwag,
  process to process) and `compare/gatbench/results.md` (in-process
  microbenchmarks vs gqlgen, connect-go, grpc-gateway). Driven by
  `compare/run.sh`.

`compare/` drives its sweeps with `bench/cmd/traffic`, so the load
generator is shared and the two sets of numbers line up.

```
bin/bench up [--build] [--raw]    # boot benchmark stack
bin/bench down                    # tear down
bin/bench gw add                  # add cluster node
bin/bench service add greeter     # register managed service
bin/bench traffic                 # run load generator
bin/bench perf                    # self sweep — regenerates docs/perf.md

compare/run.sh local              # competitor sweep — regenerates comparison.md
compare/run.sh local --only gat   # one gateway
compare/gatbench/report.sh        # microbenchmark matrix
```

State lives under `bench/.run/` and `compare/.run/` (both gitignored).

`compare/gatbench/` is its own Go module: gat's pitch includes a small
dependency tree, so gqlgen and grpc-gateway must not land in the root
`go.mod`. It is unreachable from `go build ./...` at the root.

## Don't Commit

- `.gw/` — contains `credentials.json` with admin bearer tokens
- `*.test` — test binaries
- `ui/dist/` — build artifacts
- `docs/plan.md`, `docs/todo.md` — maintainer working docs

## Commit Messages

Follow the style of recent commits: `type(scope): summary` on the first
line (50 chars or less), blank line, then a body that explains the why.

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`.

Example:

    feat(ps): cross-slot channel-binding pattern uniqueness

    Extract ChannelBindings into slot.ir at bake time so the
    reconciler can route deletes without walking per-kind maps.

## Changelog

If your change touches a public surface (exported symbols marked
`// Stability: stable`, wire-format fields, HTTP routes, metric
names, build / release artefacts), add a one-line entry to
`CHANGELOG.md` under `## Unreleased`. Section by intent: `Added` /
`Changed` / `Deprecated` / `Removed` / `Fixed` / `Security` /
`Migration`. Skip the file entirely for internal refactors, tests,
docs that don't promise anything new.

Stability contract: [`docs/stability.md`](./docs/stability.md).
Release flow: [`RELEASE.md`](./RELEASE.md).
