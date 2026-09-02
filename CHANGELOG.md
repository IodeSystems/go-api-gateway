# Changelog

Notable changes to gwag, conventional-commits-derived but human-edited.
One section per release; `Unreleased` at the top while main moves.

The version contract is in [`docs/stability.md`](./docs/stability.md):
SemVer over the symbols marked `// Stability: stable`, additive
changes on MINOR, drops on MAJOR.

## Unreleased

## v2.0.0 — 2026-09-01

### Changed
- **Import paths gain a `/v2` suffix.**
  `github.com/iodesystems/gwag` → `github.com/iodesystems/gwag/v2`.
  Required, not cosmetic: gwag's exported API is typed in graphql-go
  values — `*graphql.Scalar`, `graphql.Output`, `*graphql.Schema`,
  `graphql.Fields`, `*graphql.ResolveInfo` — and those types now come
  from `github.com/IodeSystems/graphql-go/v2`. To Go, a type from the
  v1 path and the same type from the v2 path are different types, so a
  caller holding a v1 `*graphql.Scalar` can no longer pass it in. That
  is a MAJOR break under `docs/stability.md`, and v1.4.1 consumers stay
  frozen where they are until they choose to move.

- **graphql-go v1.2.0 → v2.0.0.** `ExecuteParams.ConcurrentThunks` was
  removed upstream; gwag never set it, so nothing here changed but the
  path.

### Fixed
- **Thunked resolvers no longer serialize under append-mode execution.**
  graphql-go's append walker awaited each thunk as its own resolver
  returned, so resolvers that start a goroutine and return a thunk
  waiting on it ran end to end instead of overlapping. A selection set
  now resolves every field before awaiting any thunk. Measured on 20
  fields sleeping 1 ms each: 21.8 ms → 1.49 ms.

### Added
- **`graphql.DoWriter` / `graphql.DoAppend`** are available from
  graphql-go v2 for writing a response body without building the
  `map[string]interface{}` result tree. gwag does not use them yet.

### Performance
- Plan-time allocation in graphql-go dropped: response-key JSON is
  built once per selection set on first append-mode use rather than
  once per field at plan time. `PlanQuery` on a 100-field query went
  325 → 224 allocs/op.

## v1.4.1 — 2026-08-18

### Security
- **The GraphQL parser now caps nesting depth.** Via
  `github.com/IodeSystems/graphql-go` v1.1.0 → v1.2.0. The parser
  descended one Go stack frame per nesting level with no limit, and a
  stack overflow in Go is a fatal runtime error that `recover` cannot
  catch — so a deeply enough nested document killed the gateway process
  rather than failing the request. Roughly three million levels (a ~6 MB
  body) were needed to reach it.

  `WithUploadLimit` did not stand in the way: it defaults to unlimited,
  and it gates the multipart and tus paths, not a plain
  `application/json` GraphQL POST. A stock gateway was exposed.

  The cap is `parser.DefaultMaxDepth` (1000), and gwag picks it up with
  no configuration — it parses with zero-valued `ParseOptions`, which
  selects the default. The limit counts *descent* only: sibling fields,
  list elements and object fields cost no depth, so wide documents are
  unaffected, and 1000 sits far above any real query. Selection sets,
  list values, input-object values and list types each recurse
  independently and all four are capped. Set `ParseOptions.MaxDepth`
  negative to disable the check for trusted input.
- **Three more hostile-query blow-ups are closed** in the same bump:
  syntax-error formatting is linear rather than quadratic,
  abstract-field selections plan lazily instead of exponentially, and
  the overlapping-fields validator terminates on cyclic fragment
  spreads. Each is driven by the query document alone.

## v1.4.0 — 2026-08-17

### Changed
- **gat's GraphQL ingress answers `200` for a query that resolved to
  errors**, where it previously answered `400`. An errors envelope is a
  successful transport exchange under GraphQL-over-HTTP, and this is
  what the full `gw/` gateway has always done — gat was the outlier.
  Transport-level rejections are unaffected: a body that isn't JSON, or
  one carrying no query, is still a `400`. Clients that branch on the
  HTTP status to detect a failed query must read the `errors` key
  instead.
- **gat's GraphQL ingress runs on a plan cache + append-mode executor.**
  It served every request through `graphql.Do` — re-parsing, re-validating
  and re-planning the query each time, then building a `map[string]any`
  result tree to encode. It now looks the query up in a
  `graphql.PlanCache` and runs `ExecutePlanAppend`, writing response JSON
  straight into a pooled buffer, the same path `gw/` moved to in v1.0.0.
  Measured on a 25-row list: 535µs → 119µs, 2180 → 462 allocs; a point
  lookup goes 271µs → 25µs, 1041 → 89 allocs. GraphQL error `extensions`
  (`status` / `code`) still reach the response envelope.
- **gat's Connect/gRPC ingress no longer routes through JSON or an
  intermediate map.** Requests were marshalled to JSON text with
  `protojson` and parsed back into a `map[string]any`, which was then
  bound into the handler's input struct by tag; responses went the other
  way, `json.Marshal` then `protojson.Unmarshal`. Three mount-time plans
  replace all of it: `bindPlan` writes proto fields straight into the
  input struct through cached field paths, `messageToArgs` walks a
  message directly when the bind plan declines, and an encode plan
  writes the response.

  Dropping the map also drops the repair work it required — `assignValue`
  existed largely to undo protojson's encoding, parsing a 64-bit integer
  back out of the string `"5"`. Reading the message directly means an
  int64 is an int64 the whole way.

  Each plan refuses to form for anything it cannot prove it handles
  identically — enums, proto maps, and any Go type with a custom
  JSON/text marshaler or unmarshaler (`time.Time`, `json.RawMessage`) —
  and that operation keeps the older path. Correctness never depends on
  the analysis being clever; only speed does. All three are held against
  their predecessors by differential tests, because `bindInput` silently
  skips what it cannot map and a divergence would surface as a handler
  reading a zero value rather than as an error.

  The per-request scan for the body argument also moved to mount time.
- **gat writes Connect responses straight to proto wire bytes.** The
  response was assembled as a `dynamicpb` message and handed to
  `proto.Marshal`. A memory profile of a 25-row response put roughly
  four fifths of the request's allocations in that arrangement:
  `dynamicpb.NewMessage` once per row, `protoreflect.Value` boxing per
  field, and proto's reflective `marshalMessageSlow`, which is the only
  marshaller available because `dynamicpb` carries no generated fast
  path. The encode plan already knows which Go field feeds which proto
  field, so the bytes are now emitted directly and no message is built.

  gat also mounts its own binary codec, which passes `AllowPartial` to
  `proto.Marshal` on the paths that still need it. proto3 has no
  required fields, so the initialization check it skips could only ever
  succeed while walking the whole message to find that out.

  Emitting the wire format means owning its rules — implicit presence,
  packed versus unpacked repeated fields, length-prefixed nested
  messages, sign-extended negative 32-bit varints. Each is asserted
  byte-for-byte against `proto.Marshal` rather than reasoned about,
  since a mistake yields output that still parses but means something
  else. Responses whose encode plan declines still take the old path.

  On a 25-row list: **Connect binary 100µs → 30µs, 429 → 129 allocs;
  gRPC-Web 87µs → 32µs, 390 → 137 allocs.** That moves gat from ~4.8×
  a hand-written connect-go service to ~1.4×. The JSON codec is
  unchanged — protojson needs a real message, so that path still
  materialises one.

- **The maintainer benchmark directory `perf/` is now `compare/`.**
  `bench/` and `perf/` are synonyms and neither name said which was
  which. `bench/` measures gwag against itself (renders `docs/perf.md`,
  driven by `bin/bench`); `compare/` measures gwag and gat against other
  tools (renders `compare/comparison.md` and
  `compare/gatbench/results.md`, driven by `compare/run.sh`). No shipped
  artefact moves — `docs/perf.md`, `bench/perf-scenarios.yaml` and the
  `bin/bench perf` subcommand keep their names.

### Fixed
- **gat's OpenAPI dispatch reused Go's `http.DefaultClient`**, whose
  `MaxIdleConnsPerHost` is 2. Past two concurrent in-flight requests it
  fell back to a fresh connection and TCP handshake per dispatch, so
  connection churn — not the upstream — became the ceiling. In the
  competitor sweep gat's OpenAPI scenario walled at 20k RPS with p99
  74ms where gwag, which has always pre-baked a tuned client for this
  exact reason, reached 50k. gat now uses the same defaults
  (`MaxIdleConnsPerHost` 1024, 90s idle timeout), shared package-wide
  so every dispatcher draws on one pool.

### Added
- `compare/gatbench/` — a separate Go module benchmarking gat's three
  surfaces against the tools you'd otherwise hand-write each with
  (gqlgen, connect-go, grpc-gateway) on one shared workload. Kept out
  of the root module so those dependencies stay out of every downstream
  `go.sum`. Results are spliced into `compare/README.md` and the root
  README by `compare/gatbench/report.sh`, so the published tables can't
  drift from the numbers.

## v1.3.2 — 2026-07-26

### Fixed
- gat now binds request parameters promoted out of an **exported embedded
  struct**. huma documents and binds them, and gat rendered them into the
  GraphQL schema and the proto request message — but `bindInput` matched
  tags against top-level field indices only, so the argument arrived and
  was silently dropped. Same zero-value failure as the unexported-embed
  trap, one layer down and on the GraphQL/gRPC path specifically. Tag
  lookup now carries a field path and descends exported embeds (matching
  huma), allocating pointer-embedded structs on the way.

## v1.3.1 — 2026-07-26

### Fixed
- **The v1.3.0 embedded-parameter check was over-broad and rejected
  working code.** The real huma rule is *exportedness*, not embedding:
  `_findInType` skips `!f.IsExported()` before it reads a tag, and an
  embedded field takes its name from its type — so embedding an
  **unexported** type (`scope`) hides every parameter inside it, while an
  **exported** embed (`Scope`) is documented and bound correctly. v1.3.0
  refused to mount on both. It now reports only what huma genuinely
  cannot reach: a parameter on an unexported field, or one sitting inside
  an unexported embedded struct (exportedness does not recover below an
  unexported embed). Error text and `docs/gat.md` corrected — the fix is
  to export the type, not to flatten the struct.

### Added
- `gat.CheckEmbeddedParams(types...)` — the same detector, exported, for
  inputs gat never captures: operations registered with plain
  `huma.Register`, or a huma codebase not using gat at all. gat cannot
  recover those Go types from the OpenAPI document precisely because the
  parameter is missing from it, so this is a test assertion you call
  yourself.

## v1.3.0 — 2026-07-26

### Added
- gat now **refuses to mount** when an operation declares request
  parameters on an anonymous embedded struct, naming every lost
  parameter and the fix. huma does not read `query` / `path` / `header`
  / `cookie` tags from an embedded struct — they are absent from the
  OpenAPI document *and* are never bound at runtime, so the handler
  silently receives zero values and any default it falls back to looks
  like correct behaviour. gat cannot repair it (huma owns parameter
  discovery) but it is the layer that notices, because GraphQL and proto
  are built from that same document. `g.AllowEmbeddedParams(true)`
  downgrades the check to a log warning while an existing codebase is
  flattened. Documented in `docs/gat.md` with a pointer from the README.

  This can newly fail a gateway that previously started — hence a minor,
  not a patch. If it fires, those parameters were already being dropped.

### Fixed
- UI build (`bin/build`) no longer falls back to `ui/fallback/`. Two
  generated-type aliases indexed `[number]` into a nullable array
  (`NonNullable<X['services'][number]>` instead of
  `NonNullable<X['services']>[number]`), which failed `tsc` and cascaded
  into implicit-`any` errors; fixing the types then surfaced a genuinely
  unguarded `inj.landings` (nullable in the schema) on the injectors
  page, which now renders the same placeholder for null as for empty.

## v1.2.1 — 2026-07-26

### Fixed
- Enum values now survive a round-trip into a dynamic proto message.
  `renderProtoEnum` prefixes the descriptor spelling with the enum type
  (`ADMIN` → `Role_ADMIN`) because proto scopes enum *value* names to
  the enclosing package, so two enums sharing a value would collide at
  files-registry build. Every other surface speaks the bare name, but
  both value converters looked the bare name up directly against the
  prefixed descriptor and failed with `unknown enum value "ADMIN" for
  pets.v1.Role` — any cross-format call carrying an enum into gRPC
  errored. New `ir.EnumValueByExternalName` bridges the two spellings
  (proto-origin enums keep verbatim names, so both are tried) and both
  converters go through it.
- Proto rendering of map fields. `map<K,V>` has no standalone type in
  proto3 — it is sugar for a nested `<Field>Entry` message marked
  `map_entry` plus a `repeated` field — and the canonical render path
  left that arm unimplemented. Because the empty `case` shadowed the
  `default` arm, a map produced a field with **no type at all** and
  `FileDescriptorSet` construction failed with `cannot resolve type:
  invalid name reference: ""`, so a gat server with a Go `map` anywhere
  in a request or response body would not start. Every other shape
  proto cannot express degrades to `string`; maps were the one that
  turned that into a hard failure.

  Maps now render properly, including **arbitrary nesting**:
  `map<string, map<string, map<string, T>>>` boxes each inner map in a
  synthesised `<Field>Value` wrapper, since proto3 forbids a map as a
  map value. Non-integral map keys degrade to `string` (proto restricts
  keys to integral and string types), and a value shape proto still
  cannot express degrades to `string` rather than to nothing.

## v1.2.0 — 2026-07-13

### Fixed
- OpenAPI / MCP ingest now numbers synthesised enum values 0-based in
  declaration order instead of leaving every value at `0`. The proto
  renderer reads `EnumValue.Number` verbatim, so the old all-zero
  numbering failed `FileDescriptorSet` construction ("conflicting
  non-aliased values on number 0") and kept a gat server from starting
  whenever an ingested schema had a string enum field.
- OpenAPI `nullable: true` (and the 3.1 `type: [..., "null"]` spelling)
  now maps onto GraphQL nullability: a field is rendered non-null only
  when it is required *and* not nullable. A required-but-nullable field
  previously rendered `T!` and errored at query time when its value was
  null.
- `{prefix}/schema/graphql` now answers introspection `POST` queries by
  delegating to the GraphQL execution handler (the `GET` view still
  serves SDL / `?format=json`). graphql-codegen and similar tools
  introspect via `POST`, so they can target the documented schema URL
  instead of getting a 405.

## v1.1.0 — 2026-05-31

### Added
- Admin UI "Sign Token" panel — a GUI for `gwag sign`. A form (channel,
  TTL, optional kid) calls the existing `signSubscriptionToken` admin op
  and shows the returned hmac / timestamp / kid for an operator to hand
  to a subscription client. Bearer-gated like the other admin writes.
- `gateway.WithDestructiveReads(paths...)` — opt specific GET/HEAD paths
  into the admin-auth gate (for reads that have side effects or expose
  sensitive data), instead of `AdminMiddleware` treating every GET as a
  public read. Exact match, or prefix with a trailing `*`. The gate
  applies wherever the path is reached, including the `admin_*` GraphQL
  field that dispatches a GET to it. See
  [`docs/admin-auth.md`](./docs/admin-auth.md).
- Global `gwag` context fallback. When a directory has no project-local
  `./.gw/credentials.json`, login resolution falls back to
  `~/.config/gwag/credentials.json` (XDG_CONFIG_HOME-aware) — kubectl-
  style, so a login set once globally applies in every directory unless
  a project overrides it. `gwag login` / `logout` / `use` take `--global`
  to manage that file; `gwag context` prints the resolved source path;
  `gwag up` still writes the project-local context.
- OAuth/JWT token-exchange outbound auth. `gateway.TokenExchangeClient`
  / `TokenExchangeConfig` / `NewTokenExchangeTransport` perform RFC 8693
  token exchange: the inbound caller's bearer (read from the HTTP request
  or the gRPC ingress metadata) is swapped at a configurable issuer for
  an upstream-scoped token, cached until shortly before expiry (bounded
  by `MaxCachedTokens`; concurrent misses coalesce via singleflight
  without holding the cache lock during the network call) and attached
  outbound.
  Preserves caller identity while re-minting for the upstream's
  audience — the third outbound-identity mode next to `ForwardHeaders`
  (passthrough) and `ServiceAccountTransport` (gateway's own identity).
  Install via `WithOpenAPIClient` / `OpenAPIClient`. See
  [`docs/admin-auth.md`](./docs/admin-auth.md).
- Service-account outbound auth helper. `gateway.ServiceAccountTransport`
  (an `http.RoundTripper`) + the `ServiceAccountClient` shortcut and
  `TokenSource` / `StaticToken` attach the gateway's *own* credential to
  outbound OpenAPI / downstream-GraphQL dispatches — distinct from
  forwarding the caller's token. Install via `WithOpenAPIClient` /
  `OpenAPIClient`. Defaults to `Authorization: Bearer <token>`; header +
  scheme are configurable. See [`docs/admin-auth.md`](./docs/admin-auth.md).
- `gwag --admin-data-dir DIR` flag. Persists (and reloads) the boot
  admin token to `<DIR>/admin-token` for ad-hoc `gwag` startups, mirroring
  the example gateway and `gwag up`. A stable token means
  `gwag sign --gateway --bearer $(cat <DIR>/admin-token)` works against a
  local gateway instead of falling back to local `--secret HEX` signing.
- Static `--openapi` / `--graphql` registration flags for the `gwag`
  command, alongside the existing `--proto`. `--openapi SPEC=[NS@]URL`
  ingests a spec (file or http URL) and forwards to the upstream base;
  `--graphql [NS@]URL` introspects a downstream GraphQL endpoint at
  boot. Both repeatable; compose with `--control-plane` runtime
  registration. A CLI-driven static pipeline no longer needs the
  control plane for those two kinds.
- SDL-visible annotation carriage. An upstream author can attach
  custom directives to an operation / type / field — `x-gwag-annotations`
  on an OpenAPI (incl. huma) operation or schema, or a `@gql Name(args)`
  proto leading comment — and gwag carries them verbatim into every
  egress: real GraphQL SDL directives (with synthesized `directive`
  declarations so the document stays valid), `x-gwag-annotations` on the
  rendered OpenAPI, and `@gql` comments in the proto SDL view. Metadata
  only; runtime policy stays in transforms. New surface: `ir.Annotation`
  / `ir.AnnotationArg`, `ir.Operation/Type/Field.Annotations`,
  `ir.AnnotationIndex` + `ir.NewAnnotationIndex`,
  `ir.RuntimeOptions.AnnotationSink`, and `ir.PrintSchemaSDL` now takes
  an optional `*AnnotationIndex`. Two limits: GraphQL is a destination
  only (introspection can't carry applied directives, so downstream
  GraphQL directives can't be read), and the directives appear in the
  served SDL but not the introspection JSON. See
  [`docs/directives.md`](./docs/directives.md).
- `@ref` source-of-truth markers. A `@ref
  <workspace-relative-path>[:<symbol>]` Doxygen tag an upstream author
  attaches to an operation / type / field is now captured at ingest
  (proto leading comment, OpenAPI `x-ref` extension, or GraphQL
  description line) into the new `ir.Operation/Type/Field.Ref` field and
  re-emitted across every format the gateway serves: a `@ref` line in
  the GraphQL SDL + introspection descriptions, `x-ref` on OpenAPI
  operations, and a method leading comment in the proto SDL view. The
  marker is propagated, never synthesized — it carries through unchanged
  so [tslsmcp](https://github.com/iodesystems/tslsmcp) can link
  generated artifacts back to source. Purely informational; runtime
  behavior unchanged. First marker wins (multi-source deferred).
- gat pubsub. The embedded `gat` translator gains a publish/subscribe
  primitive — `Gateway.PubSub()` with `Publish` / `Subscribe` and
  NATS-style channel patterns (`*` one segment, `>` the rest),
  best-effort buffered delivery. `EnablePeerMesh(PeerMesh{Peers,
  Auth})` adds best-effort cross-node fanout: `Publish` fire-and-
  forgets the event to every peer gat over HTTP (HMAC-signed), and
  each peer fans it out local-only — one hop, no re-broadcast.
  Per-peer bounded queue + circuit breaker so a dead peer never
  stalls the publisher. `RegisterHTTP` / `RegisterHuma` mount a
  WebSocket subscribe stream at
  `{prefix}/_gat/subscribe?channel=PATTERN`, plus the internal
  peer-receive endpoint. `EnableSubscribeAuth(secret)` gates the
  subscribe endpoint behind HMAC tokens minted by
  `gat.SignSubscribeToken` — a key independent of the peer-mesh
  `Auth`. `(*gat.Gateway).SignSubscribeToken(channel)` is the
  import-boundary method form: server-side code holding the gateway
  signs against the configured secret without threading it (errors if
  `EnableSubscribeAuth` is unset). `gat.Gateway` gains `Close()`. See
  [`docs/gat-pubsub.md`](./docs/gat-pubsub.md). Static peer list
  only — a dynamic `PeerProvider` is a followup.

### Changed
- OpenAPI ingest now maps inline (non-`$ref`) object variants in a
  `oneOf` / `anyOf` to real GraphQL union members instead of collapsing
  the union to a JSON scalar. Each inline object variant is synthesised
  as a named type (`<hint>VariantN`), so it also round-trips back out as
  a named OpenAPI component. Non-object variants (scalars) still fall
  back, since a GraphQL union can only hold objects.
- Outbound dispatches now propagate distributed-tracing / correlation
  headers (W3C `traceparent` / `tracestate` / `baggage`, B3,
  `x-request-id`, AWS / GCP trace headers) from the inbound request, so
  a trace survives the gateway hop without configuring OpenTelemetry.
  OpenAPI / downstream-GraphQL forward them as HTTP headers alongside
  the auth allowlist; proto / gRPC dispatch bridges them onto the
  outbound gRPC metadata (inbound source is the HTTP request, or the
  incoming gRPC metadata on the gRPC ingress). An OTel `TracerProvider`,
  when set, still overwrites `traceparent` with the gateway's span
  context. An empty `ForwardHeaders()` opts out of HTTP forwarding,
  trace headers included.

### Removed
- **BREAKING (SemVer deviation):** `ir.RenderGraphQL(svcs) string` is
  removed. It shipped `// Stability: stable` in v1.0.0, so strictly this
  is a 2.0 change — we are making a deliberate exception in this minor
  because it had **zero non-test callers** and silently disagreed with
  the served schema (flat output on `Query`, skipping the
  namespace/version projection and descriptions the runtime path
  applies). The served `/schema/graphql` SDL has always come from the
  runtime path, not this function. **Migration:** build the schema with
  `ir.RenderGraphQLRuntime(svcs, registry, opts)` and serialize with
  `ir.PrintSchemaSDL(schema)`. If you need the old flat renderer, pin to
  `v1.0.x`.

### Fixed
- gat embedded dispatch — in-process huma handler results are now
  JSON-normalized before GraphQL resolution. Previously the raw Go
  value was handed to the graphql-go runtime, so custom
  `json.Marshaler` types and sealed-union responses (`oneOf` +
  discriminator) could not resolve. (`gw/gat/dispatch_inproc.go`)
- gat OpenAPI-origin enums — the graphql-go runtime value for an enum
  member now binds to the enum name (string) rather than the default
  ordinal number, so enum fields in JSON-shaped data resolve instead
  of returning null. (`gw/ir/render_graphql_runtime.go`)
- gat bind — protojson-encoded numbers (int64/uint64 land as JSON
  strings to preserve precision past `Number.MAX_SAFE_INTEGER`) now
  coerce into numeric Go fields with bounds-checked `strconv` parsing,
  both at top-level args and recursively through nested body structs
  and slices. Consumer huma input structs can declare `int64` /
  `uint64` fields directly without ad-hoc `string` shims at every
  call site. (`gw/gat/dispatch_inproc.go`)

## v1.0.0 — 2026-05-16

### Added
- Downstream MCP server ingestion — a fourth ingest kind alongside
  proto / OpenAPI / GraphQL. `gw.AddMCP(transport, target, opts...)`
  connects to an MCP server, runs `tools/list`, and re-exposes each
  tool as a GraphQL Mutation; `tools/call` dispatches at request
  time. Transports: `MCPStdio` (subprocess), `MCPHTTP` (Streamable
  HTTP), `MCPSSE`. CLI: `gwag --mcp-upstream NS:TRANSPORT:TARGET`
  (repeatable, also on `gwag up`). New public symbols `gw.AddMCP`,
  `gw.MCPTransport` + `MCPStdio` / `MCPHTTP` / `MCPSSE`, `ir.IngestMCP`,
  `ir.KindMCP`, `ir.MCPToolOrigin`. Tools-only — resources and
  prompts are not ingested. See [`docs/mcp.md`](./docs/mcp.md).
- `gwag --mcp` mounts the outbound MCP server at `/mcp` on the bare
  `gwag` command (previously only `gwag serve` / `gwag up`).
- Proto `bytes` field → `Upload` arg binding. Mark a `bytes` field
  with `[(gwag.upload.v1.upload) = true]` (extension declared at
  `gwag/upload/v1/options.proto`) and the gateway exposes it as a
  GraphQL `Upload` arg; the proto dispatcher reads the upload body
  (inline graphql-multipart-spec or tus-staged) into the field,
  capped by `WithUploadLimit`. Closes the proto-side gap in the
  upload story — OpenAPI `multipart/form-data` already mapped to
  `Upload`. New IR helper `ir.IsUploadField`; new public symbol
  `gw/proto/upload/v1.E_Upload`. Worked end-to-end in
  `gw/upload_proto_test.go`.
- `ir.AppendDispatcher` (gw/ir/dispatch.go) — optional capability
  interface for dispatchers that can emit their result as JSON bytes
  directly. The runtime renderer prefers `DispatchAppend` over
  `Dispatch` when a registered dispatcher implements it, routing
  through the fork's `ExecutePlanAppend` walker's `ResolveAppend`
  fast path (no per-field resolver tree, no full-tree map
  allocation, no leaf-emitter coercion round-trip). The built-in
  `protoDispatcher`, `openAPIDispatcher`, `graphQLDispatcher` (flat-
  upstream forwarder), and `graphqlGroupDispatcher` (nested-
  namespace forwarder) all implement it; the middleware chain
  (`backpressureMiddleware`, `quotaMiddleware`,
  `callerIDEnforceMiddleware`) passes the capability through. Adopters
  with BYO dispatchers (`gat.ServiceRegistration.Dispatchers`) opt in
  by implementing the method; plain `Dispatcher` continues to work
  unchanged.

  Measured impact (Ryzen 9 3900X, loopback, -benchtime=2s, single
  greeter / pets-style schema):

  | Bench | Before (Dispatch) | After (DispatchAppend) | Speedup |
  |---|---|---|---|
  | `BenchmarkProtoSchemaExec*`   | 436 µs / 68 KB / 937 allocs | 201 µs / 11.5 KB / 187 allocs | 2.2× / 5.9× / 5.0× |
  | `BenchmarkOpenAPISchemaExec*` | 391 µs / 68 KB / 937 allocs | 151 µs / 8.9 KB / 102 allocs | 2.6× / 7.6× / 9.2× |

  Fields whose return type contains a Union or Interface take the
  append path on graphql-origin (upstream resolves `__typename`,
  `prefixResponseTypenames` rewrites to the local prefix) and on
  openAPI-origin (the walker carries IR Service + type prefix to
  discriminate variants via `DiscriminatorProperty` / `Mapping`, then
  a required-fields heuristic, mirroring `IRTypeBuilder.unionFor`'s
  `ResolveType`). Proto-origin abstract returns stay on the Dispatch
  path until proto ingest synthesizes `TypeUnion` and the
  dispatcher carries the discriminator closure.

  New helper files: `gw/proto_append.go` (proto-message → JSON walker
  with selection projection + snake_case→camelCase renaming),
  `gw/openapi_append.go` (JSON-byte projector walking the local
  selection AST, type-aware for Union dispatch).
- `gwag serve --graphql URL [--mcp] [--mcp-include GLOB]`
  (gw/cmd/gwag/serve.go) fronts one downstream GraphQL service with
  the full gateway — metrics, backpressure, subscription proxy, and
  an optional `/mcp` mount. `--openapi` and `--proto` keep using the
  lighter gat translator. README "How to use it" lists all four
  shapes (standalone, gat, MCP exposure, CLI shortcut).
- `gw.WithMCPInclude(globs...)` / `gw.WithMCPExclude(globs...)` /
  `gw.WithMCPAutoInclude()` (gw/gateway.go) seed the MCP allowlist at
  construction. Operators declare which operations agents can call
  before any runtime admin edit, so the `/mcp` endpoint serves a
  meaningful surface from the first request without an out-of-band
  admin POST. Globs are dot-segmented (`*` matches one segment, `**`
  matches any number). Internal `_*` namespaces remain filtered first;
  neither Include nor AutoInclude can override that. Runtime
  `/api/admin/mcp/*` edits still override the seed.
- `gw.MountMCP(mux, opts...)` and `gateway.MCPPath(path)`
  (gw/mcp_server.go) wrap `MCPHandler()` in `AdminMiddleware` and
  register it on a `*http.ServeMux` in one call. Default path
  `/mcp`; override with `MCPPath`. Used by `gwag up` and the multi
  example so the standard gwag binary exposes MCP out of the box.
- `mcpIncludeRemove` and `mcpExcludeRemove` admin huma operations
  (`POST /api/admin/mcp/include/remove`, `POST /api/admin/mcp/exclude/remove`)
  — explicit removal sibling to the existing include / exclude
  endpoints. Needed so the UI can manage entries without overwriting
  the full list.
- `docs/mcp.md` — full MCP surface reference (tools, allowlist
  semantics, mounting, cluster behavior, runtime control). Linked
  from the README and from `docs/admin-auth.md`.
- `gat.RegisterHTTP(mux, g, prefix)` (gw/gat/http_register.go) — the
  huma-free counterpart of `RegisterHuma`. Mounts gat's four HTTP
  endpoints (`/graphql` + three schema views) on any `HandleMux` after
  `gat.New(regs...)` has built the gateway. `gwag serve` uses this
  path internally.
- `gwag serve` subcommand (gw/cmd/gwag/serve.go) boots the embedded
  gat translator against one upstream service described by an OpenAPI
  spec or `.proto` file. `gwag serve --openapi spec.yaml --to
  http://localhost:8081` or `gwag serve --proto greeter.proto --to
  localhost:50051`. No NATS, no admin, no cluster — for those use
  `gwag up`. `--addr`, `--prefix`, `--namespace`, `--version`
  overrides. Mounts `/graphql` + `/schema/{graphql,proto,openapi}`
  on a plain `http.ServeMux`. Doc: `docs/gat.md` "gwag serve — CLI
  shortcut".
- `gat.RegisterHTTP(mux, g, prefix)` (gw/gat/http_register.go) — the
  huma-free counterpart of `RegisterHuma`. Mounts gat's four HTTP
  endpoints (`/graphql` + three schema views) on any `HandleMux` after
  `gat.New(regs...)` has built the gateway. `gwag serve` uses this
  path internally.
- `gat.ProtoFile(path, target)` and `gat.ProtoSource(entry, body, imports, target)`
  (gw/gat/proto.go) compile a `.proto` via `protocompile`, ingest via
  `ir.IngestProto`, dial the target gRPC server, and return
  `[]ServiceRegistration` ready for `gat.New(...)`. The returned
  dispatchers do `grpc.ClientConn.Invoke` per call with `dynamicpb`
  marshal/unmarshal — no pool, no replicas, no backpressure (gat is
  the simple-start variant; pull full gwag for those). Default
  transport is insecure; mTLS or custom dials route through the
  existing `ServiceRegistration.Dispatchers` BYO path. Namespace and
  version derive from the proto package: `greeter.v1` →
  `greeter`/`v1` (trailing `vN` is the version), `pets` →
  `pets`/`v1`. Unary-only; server-streaming is not in gat's scope.
  Doc: `docs/gat.md` "Front a remote gRPC service from a .proto".
- `gw.WithWSLimit(gw.WSLimitOptions{MaxPerIP, RatePerSec, Burst, TrustedIPs})`
  (gw/ws_limit.go) caps the graphql-transport-ws Upgrade path per
  peer IP — a concurrent-connection semaphore plus a token-bucket
  rate limit on the handshake itself. Operators running gwag at the
  edge (no upstream reverse proxy / CDN terminating WebSockets) get
  the minimum DoS guard; an unset option leaves the upgrade path
  uncapped, preserving the pre-v1 behaviour. Rejected upgrades
  return HTTP 429 and increment
  `go_api_gateway_ws_rejected_total{reason}` with reason
  `max_per_ip` or `rate_limit`. Peer IP comes from `r.RemoteAddr`
  (port stripped); spoofable headers like `X-Forwarded-For` are
  not honoured — operators behind a trusted proxy pin its address
  in `TrustedIPs` and rely on the proxy for per-origin limits.
  Doc: `docs/operations.md` "WebSocket upgrade caps".
- `gw.WithTracer(trace.TracerProvider)` (gw/gateway.go) installs an
  OpenTelemetry TracerProvider for distributed tracing. Per-request
  server-kind spans land at every ingress shape: GraphQL
  (`gateway.graphql`), HTTP/JSON (`gateway.http`), gRPC
  (`gateway.grpc`), plus `.subscription` variants for SSE and gRPC
  server-streaming. Each upstream call opens a nested client-kind
  span (`gateway.dispatch.proto`, `gateway.dispatch.openapi`,
  `gateway.dispatch.graphql`, `gateway.dispatch.internal`,
  `gateway.dispatch.proto.subscription`) and injects W3C TraceContext
  on the outbound side — `traceparent` rides HTTP headers
  (OpenAPI / downstream GraphQL) and gRPC metadata (proto unary +
  server-streaming) so downstream services join the same trace. Spans
  carry the canonical `gateway.ingress` / `gateway.namespace` /
  `gateway.method` / `gateway.version` attribute set plus
  ingress-specific `http.*` / `rpc.*` keys from the OpenTelemetry
  semantic conventions. Inbound `traceparent` on the request is
  honoured so the gateway joins the caller's trace. When the option is
  unset, a noop tracer is wired and per-request cost stays near zero.
  Default builds depend on `go.opentelemetry.io/otel` directly.
- `Upload` GraphQL scalar (`gw.UploadScalar`, `*gw.Upload`) — exposed
  in every assembled schema so clients can declare `mutation
  ($f: Upload!)` against upload-capable fields. End-to-end multipart
  uploads via two complementary wire shapes: the
  graphql-multipart-request-spec parser handles inline single-request
  files at `POST /api/graphql`; tus.io v1.0 chunked uploads land at
  `gw.UploadsTusHandler()` (`/api/uploads/tus`) and the upload-id
  rides in GraphQL variables. The `Upload` scalar accepts both forms
  at `ParseValue`; the dispatcher opens the body lazily via
  `(*Upload).Open(ctx, store)` regardless of which wire shape
  delivered it.
- OpenAPI ingest recognises `multipart/form-data` request bodies and
  flattens `type: string, format: binary` schema properties into
  top-level `Upload!` (or `[Upload!]!`) GraphQL args. The
  `openAPIDispatcher` forwards as `multipart/form-data` upstream
  preserving the captured `Filename` and `Content-Type`. The
  gateway's re-exposed REST surface (`gw.IngressHandler`) accepts
  multipart inbound too. Proto-side binding deferred.
- `gw.UploadStore` interface (`Create` / `Append` / `Info` / `Open`
  / `Delete`) backing both wire shapes; default
  `gw.FilesystemUploadStore` impl with 24-hour TTL eviction.
- `gw.WithUploadStore(UploadStore)` plugs a custom store;
  `gw.WithUploadDataDir(string)` shorthand installs the default
  filesystem store (gateway owns lifecycle, `Close` shuts it down);
  `gw.WithUploadLimit(int64)` caps per-upload total bytes at both
  parsers.
- `docs/uploads.md` — adopter-facing docs covering both wire shapes,
  configuration knobs, error shapes, and outbound dispatch
  semantics.
- `docs/stability.md` — SemVer contract for 1.x. Defines what's
  locked, what's allowed to change, and what's experimental.
- `// Stability: stable` and `// Stability: experimental` godoc
  markers on every exported symbol in `gw/`, `gw/ir`, `gw/gat`,
  `gw/controlclient`. 187 stable + 14 experimental.
- `docs/api-audit.md` — pre-v1 export classification with per-symbol
  rationale.
- `CHANGELOG.md` (this file) and `RELEASE.md` (release process).
- `ir.OperationGroup.OriginKind` and `ir.OperationGroup.SchemaID`
  (gw/ir/ir.go) — group-level metadata so the runtime renderer can
  distinguish graphql-origin groups (which now install a single
  forwarding resolver at the top of the chain) from passthroughs.
  Group SchemaIDs use a `_group_<path>` suffix to avoid colliding
  with leaf Operation SchemaIDs. `PopulateSchemaIDs` populates them.

### Fixed
- `gw.AddGraphQL` against nested-namespace upstreams. Previously the
  per-leaf forwarder dropped the parent group chain: a local
  `{ ns { greeter { hello(...) } } }` produced an upstream
  `{ hello(...) }` that any sane GraphQL server rejects ("Cannot
  query field 'hello' on type 'Query'"). Now a single
  `graphqlGroupDispatcher` registers at each top-level graphql
  group; it captures the local sub-selection, forwards once with
  the container path intact (`{ greeter { hello(...) } }`), and a
  new `graphqlGroupChildResolver` demuxes the response map back
  into per-leaf field positions keyed by alias-or-name. Sibling
  leaves under one group ride a single upstream round trip —
  preserves GraphQL's batching across fields, no longer
  N round-trips. Aliases (including same-field-different-args, the
  case where aliasing is structurally required) round-trip
  correctly. Affected schemas: gwag-fronting-gwag, Hot Chocolate's
  grouped operations, some Apollo subgraph layouts. Pinned by
  `TestGraphQLIngest_NestedNamespaceForwarding`,
  `TestGraphQLIngest_DeepNestedNamespaceForwarding`,
  `TestGraphQLIngest_InlineFragmentInGroup`,
  `TestGraphQLIngest_GroupDispatcherIsolatedAcrossVersions`, and
  `TestGraphQLIngest_AliasInGroupSelection`.

### Changed
- OpenAPI namespace derivation now **preserves the spec title's
  case** — a spec titled `Pets` yields the GraphQL namespace `Pets`,
  not `pets`. Previously the library `AddOpenAPI` path preserved
  case while the `gat` translator and `gwag serve --openapi` lower-
  cased it, so the same spec produced a different schema depending
  on how it was registered. All three paths now route through one
  shared rule, `ir.SanitizeNamespace` (case preserved, spaces /
  dashes → `_`, leading digit guarded). This also fixes a latent
  bug where a digit-leading title produced an invalid GraphQL
  identifier.
- `gw.MCPConfig` and `gw.MCPHandler` promoted from `Stability:
  experimental` to `Stability: stable`. Same for the
  `/api/admin/mcp/*` huma operations. The four MCP tools
  (`schema_list` / `schema_search` / `schema_expand` / `query`) and
  the allowlist shape (AutoInclude / Include / Exclude with
  dot-segmented globs) are part of the 1.x contract.
- `gw.AddAdminEvents()` no longer requires a configured cluster — it
  registers the schema fragment unconditionally so SDL-driven codegen
  consumers pick up the `admin_events_watchServices` subscription
  field. Runtime subscriptions return a clean "subscription broker
  not available" error when no cluster resolves; cluster-mode
  behaviour is unchanged.
- Cluster-mode MCP allowlist: the first runtime mutation now merges
  into the local `WithMCPInclude(...)` seed instead of replacing it.
  Previously `tryMutateMCPConfig` started from `MCPConfig{}` when
  the JetStream KV bucket had no record, so the first
  `/api/admin/mcp/include` POST blew the seed away. The CAS path
  now falls back to the local snapshot on `ErrKeyNotFound`; after
  the first Put, KV stays authoritative as before.
- `gwag serve --mcp` now works with `--openapi` and `--proto`, not
  only `--graphql`. When `--mcp` (or `--mcp-include`) is set, the run
  promotes from the lite gat path onto the full `*gateway.Gateway`
  so `/mcp` shares dispatcher, plan cache, and metrics with every
  other ingress. The `/api/graphql` + `/api/schema/*` URL shape
  matches the existing `--graphql` flow; `--prefix` is ignored when
  `--mcp` is on.
- `examples/multi`: MCP mount moved from `/api/mcp` to `/mcp`, and
  the example seeds the allowlist with `WithMCPInclude("greeter.**",
  "library.**", "admin.**")`. Operators previously needed to POST to
  `/api/admin/mcp/include` before any tool returned useful results;
  the seed makes the demo work end-to-end on first boot.
- `gw/ir` SDL renderer emits one block-string per description (rather
  than one block per source line). Resolves a "consecutive
  triple-quoted strings" parse error from GraphQL lexers (including
  graphql-codegen) when descriptions contained blank or multi-line
  text. The rendered SDL is semantically equivalent.
- Wire-level identifiers renamed `go-api-gateway` → `gwag` across
  JetStream KV bucket names (`gwag-{registry,peers,stable,
  deprecated,mcp-config}`), the default NATS cluster name
  (`gwag`), the MCP server-info string returned by `MCPHandler`,
  and the UI's `localStorage` keys (`gwag:admin-token`,
  `gwag:admin-token-changed`). Pre-1.0 cleanup so the project
  ships with one consistent identifier.
- `github.com/IodeSystems/graphql-go` bumped 0.8.1 → 1.0.0 — first
  tagged release of the fork. `ExecutePlanAppend` walker available;
  gateway-side wiring is a follow-on perf step.

### Removed (pre-1.0 cleanup; no SemVer contract yet)
- `gw/`: unexported every admin-internal helper that escaped:
  - Stats / history wire types (`StatsSnapshot`, `MethodStatsSnapshot`,
    `HistoryBucket`, `ServiceHistory`, `SubjectInfo`) and their
    methods (`Snapshot`, `History`, `ActiveSubjects`).
  - MCP admin wire types (`Schema{Expand,List,Search}*`,
    `MCP{QueryInput,ResponseWithEvents,EventsBundle,ChannelEvent}`)
    and admin-only methods (`MCPAllows`, `MCPConfigSnapshot`,
    `MCPQuery`, `MCPSchema{Expand,List,Search}`, `SetMCPConfig`).
    `MCPHandler` stays public (the canonical wiring uses it).
  - Injector inventory types (`InjectorEntry`, `InjectorFrame`,
    `InjectorLanding`, `InjectorRecord`, `InjectorKind`, `InjectorState`
    + their consts) and the `InjectorInventory()` method.
  - `BackpressureConfig` + `BackpressureMiddleware` (internal
    plumbing; `BackpressureOptions` + `WithBackpressure` remain
    the public surface).
  - `Hide{Type,Path}Rewrite` / `Nullable{Type,Path}Rewrite` —
    concrete `SchemaRewrite` impls type-asserted inside `gw/` only.
  - `HeaderInjector` and `Transform.{Headers,Inventory}` — `Transform`
    is now opaque; construct via `InjectHeader` / `InjectType` /
    `InjectPath`.
  - `InternalProto{,Subscription}Handler` callback types.
  - `With{Admin,HTTP}Auth`, `IsAdminAuth`, `WithHTTPRequest` — set
    on ctx inside the gateway, never called from adopter code.
- `gw/`: dropped IR re-export shims (`RenderGraphQLRuntime`,
  `RenderGraphQLRuntimeFields`, `RuntimeOptions`, `IRTypeBuilder`,
  `IRTypeBuilderOptions`, `IRTypeNaming`, `NewIRTypeBuilder`).
  Callers import `gw/ir` directly.
- `gw/gat`: dropped six dead exports (`Option`, `config`, `As`,
  `Version`, `ServiceOption`, `ServiceConfig`) — declared but never
  applied anywhere. Per-registration knobs would go on
  `ServiceRegistration` as plain fields if/when needed.

## v0 — pre-1.0 development

Capsule history of what landed before the 1.0 surface lock. Granular
log lives in `git log`; this is the orientation for adopters reading
the changelog cold.

### Ingest pipeline
- **Proto ingest** via `protocompile` with `SourceInfoStandard` so
  comments survive into SDL + the MCP corpus. Three entry points:
  `AddProto(path)`, `AddProtoFS(fs, entry)`, `AddProtoBytes(entry, body)`.
- **OpenAPI 3.x ingest** via `kin-openapi`. Mirror entry points:
  `AddOpenAPI(spec)`, `AddOpenAPIBytes(spec)`.
- **GraphQL stitching** via boot-time introspection with a
  namespace-prefixed type mirror: `AddGraphQL(endpoint)`.
- Symmetric raw-source wire format across the control plane:
  `ServiceBinding.proto_source` + `proto_imports`, `openapi_spec`.
  The compiled-FileDescriptorSet path was retired pre-1.0.

### Schema rebuild + IR
- Format-agnostic intermediate representation (`gw/ir`) that lifts
  proto / OpenAPI / GraphQL into a common shape. Ingest happens once
  at registration; schema rebuild reads `slot.ir` directly.
- `RenderGraphQL{,Runtime,RuntimeFields}` for client-facing SDL,
  `RenderProtoFiles` for FDS export, `RenderOpenAPI` for the
  OpenAPI re-emit. Each format honours `FlatOperations` if the
  upstream format doesn't natively support nesting.
- Auto-internal namespaces: any namespace prefixed `_` is hidden
  from the public schema (used internally for admin / quota /
  pubsub-auth in-process services).
- Tier model: `unstable` (single overwrite slot), `vN` (locked
  once registered; differing schema-hash → `AlreadyExists`),
  `stable` (computed alias to highest-ever `vN`, monotonic; only
  `RetractStable` walks it back). `--allow-tier` per-deploy policy
  replaces the older `--environment` knob.

### Dispatch
- Reflection-based unary dispatch is the canonical path. Codegen +
  plugin paths are roadmap, not shipped.
- Per-pool backpressure (`BackpressureOptions`) with separate
  unary in-flight caps, per-replica caps, and gateway-wide stream
  caps. No gateway-wide unary cap — pools are the isolation primitive.
- Outbound transport pluggability: `WithOpenAPIClient(c)` / per-
  source `OpenAPIClient(c)` for HTTP; `LoadMTLSConfig` + `WithTLS`
  for mTLS on outbound gRPC.

### Pub/sub
- `gwag.ps.v1.PubSub` proto rides NATS for multi-listener fan-out
  with channel-named topics and a typed payload registry.
- Server-streaming gRPC at egress: each subscriber opens its own
  upstream stream. The earlier "auto-transform every `stream Resp`
  into a NATS subscription" path was retired pre-1.0 — the schema
  no longer lies about an upstream stream that didn't open.
- HMAC + per-pattern auth tiers (`WithChannelAuth(pattern, tier)`):
  `ChannelAuthOpen`, `ChannelAuthHMAC`, `ChannelAuthDelegate`.
  Strictest-tier-wins for wildcard subs. Sign-side: bearer-gated
  `SignSubscribeToken{,WithKid}`.

### Caller identity
- Three extractor styles via `WithCallerID{Public,HMAC,Delegated}`
  + the `WithCallerIDExtractor` seam for custom shapes.
- `WithCallerIDMetricsTopK` caps Prometheus label cardinality;
  overflow folds into the `OtherCallerID = "__other__"` bucket.
- Kid-aware HMAC rotation via `SignCallerIDTokenWithKid`.

### Admin surface
- Operations defined via huma (`gw/admin_huma.go`), mounted at
  `/admin/*`, and self-ingested as OpenAPI so the SDL gains nested
  `Query.admin.*` / `Mutation.admin.*` fields. The same path any
  external huma service takes.
- `AdminMiddleware`: bearer-gated mutations, public reads. Pluggable
  `AdminAuthorizer` delegate registered under `_admin_auth/v1`;
  boot token underneath as the always-works hatch.
- `WithAdminDataDir` persists the boot token under
  `<dir>/admin-token`; pair with `gwag login`/`use`/`context` for
  multi-cluster credential management.

### Multi-gateway clustering
- Embedded NATS via `StartCluster(ClusterOptions)`. JetStream KV
  buckets hold the registry, peer roster, stable map, deprecation
  state, and MCP config.
- Reconcilers on every node sync local slot state from KV watches —
  proto, OpenAPI, and GraphQL bindings ride in the same value
  shape; `slot.kind` picks the install / remove path.
- The gateway publishes its own service-change events on
  `admin_events_watchServices` so UIs can react to pool churn
  without polling.

### Embedded translator (`gw/gat`)
- `gat.New()` + `gat.Register{,Huma,HTTP,GRPC}` ship a single-server
  in-process translator that turns huma operations into both a
  GraphQL surface and connect-go gRPC endpoints. No NATS, no
  admin, no MCP — the minimum-cost entry for a huma app that wants
  typed multi-format clients.
- Proto ingest via `gat.ProtoFile` / `gat.ProtoSource` and the
  `gwag serve` subcommand for single-upstream embedded boot.
- Package classified `experimental`; surface may shift on a minor
  release.

### MCP integration
- `MCPHandler()` mounts an MCP-streamable HTTP endpoint with four
  tools: `schema_list`, `schema_search`, `schema_expand`,
  `gateway_query`. Operator-curated allowlist lives in the
  cluster `gwag-mcp-config` KV bucket; `SetMCPConfig` writes
  through.

### Operability
- Prometheus metrics under the `gateway_*` namespace: dispatch
  histograms, queue depth (per-pool + per-replica), backpressure
  rejects, admin-auth outcomes, caller-id label budgets, MCP
  audit counters.
- `WithPprof` mounts the runtime profiler under
  `AdminMiddleware`.
- Health endpoint (`HealthHandler`) flips to 503 during
  `Drain(ctx)`. Graceful drain waits for in-flight unary +
  active subscriptions before close.
- `WithRequestLog(io.Writer)` opt-in JSON-per-request sink with
  total time, self time, and per-dispatch counts.

### Benchmarking
- `bench/` self-perf harness with `bench traffic` (escalating-
  target-RPS sweep) + `bench perf` (knee detection, p95/p99
  reports → `docs/perf.md`).
- `perf/` competitor matrix scaffolding for head-to-head against
  graphql-mesh, Apollo Router, WunderGraph. Hermetic Docker
  build; scenarios declared in `perf/competitors.yaml`.

### Tooling / DX
- `gwag` binary (`gw/cmd/gwag`) with subcommands: `up` (zero-config
  NATS + admin + UI), `peer {list,forget}`, `services list`,
  `schema {fetch,diff}`, `sign`, `login`, `logout`, `context`, `use`.
- Embedded React + MUI v6 admin console at `ui/` with
  TanStack Router. `ui/embed.go` bundles the built SPA into the
  Go binary.
- `bin/build` single-entry build wrapper handling UI codegen +
  Vite build + Go build with fallback seeding so
  `go:embed all:dist` never breaks an unprepared tree.

### Won't fix (called out intentionally)
- **No Apollo Federation.** Stitching is correct for the common
  case; federation's entity-merging is overkill for most teams.
  See `docs/federation.md`.
- **No AsyncAPI export.** GraphQL SDL with Subscription types
  covers TS codegen; AsyncAPI's TS tooling is patchier.
- **One Register call = one address contributing to N pools, not
  N independent bindings.** Bindings share lifetime. Run two
  binaries for independent lifecycles.
