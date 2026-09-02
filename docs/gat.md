# gat — embedded GraphQL + gRPC for huma services

`gat` (GraphQL API Translator) is gwag's in-process variant: a
single-server translator that turns a [huma](https://huma.rocks/)
HTTP service into REST + GraphQL + gRPC on **one** port. No NATS,
no cluster, no admin endpoints, no MCP — just typed surfaces
emitted from the same handlers huma already runs.

Import:

```go
import "github.com/iodesystems/gwag/v2/gw/gat"
```

A runnable end-to-end demo with React/TypeScript codegen lives at
[`examples/gat/`](../examples/gat/README.md).

## When to reach for gat (vs full gwag)

gat fits one Go binary serving its own huma operations — single
process, no NATS, no cluster, no runtime control-plane registration,
no admin UI. ~250 deps vs ~498.

Use full gwag when multiple service binaries register at runtime,
when you need NATS-backed pub/sub or cluster KV, or when the admin
UI / MCP tools / self-introspecting admin OpenAPI is load-bearing.

## What it costs

Each surface, measured against the tool you'd otherwise hand-write it
with, on the same huma handler and the same payload:
[`compare/gatbench/`](../compare/gatbench/README.md).

Short version: the REST path is huma unmodified, so it costs nothing
extra. GraphQL lands ahead of a hand-written gqlgen server — gat runs
the same plan cache + append-mode executor `gw/` uses, which appends
response JSON straight into a pooled buffer. Connect/gRPC is the
surface that costs: ~1.7-3× a hand-written connect-go service,
because gat binds through dynamicpb and reflection where connect-go
uses generated message types.

The GraphQL ingress answers **200** for any query that reached
execution, including one that resolved to errors — an errors envelope
is a successful exchange, per GraphQL-over-HTTP. A malformed request
(body isn't JSON, or carries no query) is still a 400.

## Mental model

Huma is the source of truth. You register each operation with huma
as you always have — except you call `gat.Register` instead of
`huma.Register` for the operations you want surfaced via GraphQL
and/or gRPC. `gat.Register` is a paired drop-in:

```go
func Register[I, O any](
    api huma.API,
    g *gat.Gateway,
    op huma.Operation,
    handler func(context.Context, *I) (*O, error),
)
```

It calls `huma.Register(api, op, handler)` (so REST keeps working
exactly as before) AND captures the handler reference on `g` so gat
can dispatch GraphQL and gRPC requests to it in-process. No loopback
HTTP, no network hops, no second handler implementation.

After all operations are registered, you wire gat's two mount points:

```go
gat.RegisterHuma(api, g, "/api")    // GraphQL + 3 schema views
gat.RegisterGRPC(mux, g, "/api/grpc") // connect-go handlers
```

`RegisterHuma` finalizes the gateway: it reads huma's accumulated
`OpenAPI` document, ingests it via the same IR pipeline as full
gwag, wires the captured handlers as in-process dispatchers, builds
the GraphQL schema, and mounts the GraphQL + schema-view endpoints
as huma operations.

`RegisterGRPC` adds [connect-go](https://connectrpc.com/) handlers
per operation. Each operation gets a path of the canonical Connect /
gRPC shape: `{prefix}/{Service.FullName}/{MethodName}`. Wire-protocol
coverage is what connect-go gives: grpc-go clients, connect-go
clients, and grpc-web clients all hit the same handler.

## Routes

For a `gat.RegisterHuma(api, g, "/api")` + `gat.RegisterGRPC(mux, g, "/api/grpc")`
setup, the endpoints are:

| URL                                            | What                                        |
| ---------------------------------------------- | ------------------------------------------- |
| `POST /api/graphql`                            | GraphQL queries + mutations                 |
| `GET  /api/schema/graphql`                     | SDL (default), `?format=json` introspection |
| `GET  /api/schema/proto`                       | `descriptorpb.FileDescriptorSet` (binary)   |
| `GET  /api/schema/openapi`                     | Re-emitted OpenAPI document                 |
| `POST /api/grpc/{Service.FullName}/{Method}`   | Connect / gRPC / gRPC-Web                   |

Plus huma's own routes — REST per the operation paths, and huma's
default `/openapi.json` for huma's spec.

## Incremental adoption

`gat.Register` is opt-in per operation. Plain `huma.Register`
operations keep serving REST and don't appear in the GraphQL schema
or proto descriptor set, so you can migrate handler-by-handler.

## `gwag serve` — CLI shortcut

For the single-upstream case (one OpenAPI spec or one `.proto`, one
backend), the `gwag` CLI ships a `serve` subcommand that wires this
package without any Go scaffolding:

```bash
# Front an OpenAPI spec.
gwag serve --openapi spec.yaml --to http://localhost:8081

# Front a remote gRPC service.
gwag serve --proto greeter.proto --to localhost:50051

# Mount under a prefix, override the GraphQL namespace, change listen addr.
gwag serve --openapi spec.yaml --to http://localhost:8081 \
           --addr :9090 --prefix /api --namespace pets
```

The serve command boots gat (no NATS, no admin, no cluster) and
mounts `/graphql` plus `/schema/{graphql,proto,openapi}` on a plain
`http.ServeMux`. For richer needs (auth, metrics, multiple sources)
write Go directly against this package — `gwag serve` is just the
zero-friction entry point.

## Mounting on a plain mux

`gat.RegisterHTTP(mux, g, prefix)` is the huma-free counterpart of
`RegisterHuma`. Mount on any `*http.ServeMux` (or anything satisfying
`gat.HandleMux`) after `gat.New(regs...)` has built the gateway:

```go
g, _ := gat.New(regs...)
mux := http.NewServeMux()
if err := gat.RegisterHTTP(mux, g, "/api"); err != nil {
    log.Fatal(err)
}
http.ListenAndServe(":8080", mux)
```

Endpoints: `POST /api/graphql`, `GET /api/schema/graphql` (SDL or
`?format=json` introspection), `GET /api/schema/proto` (FDS),
`GET /api/schema/openapi`. `gwag serve` uses exactly this path.

## Front a remote gRPC service from a .proto

The huma-paired flow above hosts handlers in the same Go binary as
gat. The companion `gat.ProtoFile` / `gat.ProtoSource` helpers point
gat at a *remote* gRPC service instead — gat compiles the `.proto`,
dials the target, and dispatches GraphQL queries to the upstream via
`grpc.ClientConn.Invoke` per call.

```go
regs, err := gat.ProtoFile("greeter.proto", "localhost:50051")
if err != nil { log.Fatal(err) }
g, err := gat.New(regs...)
if err != nil { log.Fatal(err) }
http.Handle("/graphql", g.Handler())
```

The bytes variant accepts an embedded `.proto`:

```go
//go:embed greeter.proto
var greeterProto []byte

regs, _ := gat.ProtoSource("greeter.proto", greeterProto, nil, "localhost:50051")
g, _ := gat.New(regs...)
```

Namespace / version derive from the proto package by default —
`greeter.v1` → namespace `greeter` + version `v1` (the trailing `vN`
segment is recognised); `pets` → namespace `pets` + version `v1`.
Override per-service after the helper returns:

```go
regs[0].Service.Namespace = "external"
regs[0].Service.Version = "v2"
```

The default transport is insecure (no TLS). For mTLS or other dial
credentials, dial yourself and supply custom dispatchers via
`ServiceRegistration.Dispatchers` — see `gat.New`'s BYO-IR pattern.

This path is unary-only. Server-streaming RPCs (GraphQL
subscriptions) aren't supported by gat; bring full gwag if you need
them.

## Dispatch internals

Each captured operation gets an `ir.Dispatcher` that:

1. Receives the canonical args map (`map[string]any` keyed by IR Arg
   name) from the graphql-go runtime or from a Connect/gRPC request
   (after `protojson` round-trip).
2. Allocates a fresh `*I` via `reflect.New(inputType)`.
3. Walks the IR's `op.Args` and binds each arg's value into the
   matching huma input field by tag (`path:"id"`, `query:"limit"`,
   `header:"X-Foo"`, `cookie:"sid"`) or, for body args, JSON-
   unmarshals into the `Body` field.
4. Calls the captured handler.
5. Extracts `Body` from the typed output (or returns the whole
   struct if there is no `Body` field).

The reflection-based binding is JSON-shaped, so anything huma's
codec already accepts at the REST boundary also works at the GraphQL
and gRPC boundaries.

## Schema endpoints

Three views over the same IR projection:

- `/api/schema/graphql` → `graphql-codegen`, typed-document-node,
  gql.tada
- `/api/schema/proto` → `buf`, `ts-proto`, `protobuf-es`
- `/api/schema/openapi` (or huma's `/openapi.json`) →
  `openapi-typescript`, `openapi-fetch`, `kubb`, `orval`

## Trap: parameters on an unexported field

**huma skips unexported fields when it discovers request parameters.**
An embedded field takes its name from its type, so embedding an
*unexported* type hides every parameter inside it:

```go
type scope struct {                // lowercase type name ⇒
    Dir string `query:"dir"`       // unexported FIELD name ⇒
}                                  // invisible to huma

type statusInput struct {
    scope                          // ← dir is dropped
    Spec string `query:"spec"`     // ← this one is fine
}
```

**Export the type and it works** — huma walks exported embedded structs
normally:

```go
type Scope struct {
    Dir string `query:"dir"`
}

type statusInput struct {
    Scope                          // ← dir is documented and bound
    Spec string `query:"spec"`
}
```

One character. Embedding is not the problem; **exportedness** is. The
same rule silently drops an ordinary unexported field carrying a
parameter tag.

gat binds parameters promoted out of an exported embed correctly as of
v1.3.2. Before that, the argument appeared in the GraphQL schema and the
proto request but was never written to the field — the same silent
zero-value failure one layer down.

### Why it is easy to misread

The failure is quiet in the worst way. `?dir=/some/path` is accepted and
ignored, the handler reads `""`, and whatever default it falls back to
looks like correct behaviour. The OpenAPI document omits the parameter
entirely, so a generated client cannot send it even if it wants to.
Nothing errors, nothing logs.

Because it is silent, it is easy to conclude the wrong thing — that
embedded params are unsupported and every input struct must be flattened.
They are supported. Check the case of the type name first.

### How you find out

gat refuses to mount, naming every unreachable parameter:

```
gat: 2 request parameter(s) unreachable by huma:
  status: statusInput.scope.Dir has `query:"dir"` — it sits inside an unexported embedded struct, so huma will not see it
  status: statusInput.scope.Now has `query:"now"` — it sits inside an unexported embedded struct, so huma will not see it

huma skips unexported fields when it discovers parameters, and an embedded field
takes its name from its type — so embedding an unexported type hides everything
inside it. The parameter is absent from the OpenAPI document and is never bound,
so the handler silently reads the zero value.

Fix: export the type or field (`scope` -> `Scope`). Exported embeds work fine.
```

gat cannot apply the fix — huma owns parameter discovery, and Go has no
way to promote a field's visibility — but gat is the layer that notices,
because it builds GraphQL and proto from that same document and the
parameter is simply absent. Plain huma gives you no warning at all.

There is related upstream discussion, though it is about *generic* /
interface-typed inputs rather than this visibility rule:

- [huma#617 — Allow anonymous fields to be used as request input
  parameters](https://github.com/danielgtaylor/huma/issues/617)
- [huma#870 — Enabling recursive process structs for
  parameters](https://github.com/danielgtaylor/huma/issues/870)

To unblock an existing codebase while types are being exported:

```go
g.AllowEmbeddedParams(true)   // before RegisterHuma; logs instead of failing
```

**Only parameter tags are affected.** A struct with no `query` / `path` /
`header` / `cookie` tags embeds normally whatever its case, and `Body`
fields are unaffected.

### Checking inputs gat does not capture

The mount-time check only sees operations registered through
`gat.Register`. Anything registered with plain `huma.Register` — the
incremental-adoption path above — is invisible to gat, and gat cannot
recover the Go type from the OpenAPI document precisely because the
parameter is missing from it.

`gat.CheckEmbeddedParams` is the same detector, exported, so those
inputs can be asserted in your own tests:

```go
func TestNoHiddenParams(t *testing.T) {
    if err := gat.CheckEmbeddedParams(
        reflect.TypeFor[listThingsInput](),
        reflect.TypeFor[getThingInput](),
    ); err != nil {
        t.Fatal(err)
    }
}
```

Worth doing even in a codebase with no gat at all: it is a five-line
test against a failure mode that otherwise ships silently.

## Constraints (current shape)

- One huma API per gat gateway. Multi-huma is doable (BYO-IR path)
  but not the recommended path.
- GraphQL subscriptions (server-streaming over WebSocket) are not in
  gat — bring full gwag for those. gat does have a separate
  publish/subscribe primitive with best-effort cross-node fanout and
  an SSE delivery stream; see [`gat-pubsub.md`](./gat-pubsub.md). The
  NATS-backed, durable pub/sub stack remains gwag-only.
- gat doesn't manage auth. Whatever middleware the adopter wraps
  the huma router with applies to gat's GraphQL/gRPC routes too,
  since they ride on the same handler chain.
