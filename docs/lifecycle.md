# Service lifecycle

Services don't just appear and vanish — they're version-tagged,
deprecated with warnings, and retired only after their callers move
off. The gateway's job is to make every step of that visible.

## Register

Services self-register over the gRPC control plane and heartbeat
to stay alive. One Register call can carry many services on one
address (multiple RPCs in one binary). Heartbeats every TTL/3;
missed heartbeats past TTL evict.

```go
import (
    _ "embed"

    "github.com/iodesystems/gwag/v2/controlclient"
)

//go:embed greeter.proto
var greeterProto []byte

reg, _ := controlclient.SelfRegister(ctx, controlclient.Options{
    GatewayAddr: "gateway:50090",
    ServiceAddr: "greeter:50051",
    Services: []controlclient.Service{
        {Namespace: "greeter", ProtoSource: greeterProto},
    },
})
defer reg.Close(ctx) // graceful deregister
```

Services ship raw `.proto` bytes; the gateway compiles them via
`protocompile` so leading / trailing comments survive into the
GraphQL SDL. Multi-file `.proto` layouts (one entrypoint with
`import "..."` statements) use `ProtoFS fs.FS` + `ProtoEntry string`
with any `fs.FS` (`embed.FS`, `os.DirFS(...)`, archives), or pass
the imports map explicitly via `ProtoImports map[string][]byte`.

OpenAPI services use the same `Service` struct with `OpenAPISpec`
instead of `ProtoSource`; same shape, both formats ship raw source.
The control-plane API is in
[`gw/proto/controlplane/v1/control.proto`](../gw/proto/controlplane/v1/control.proto).

## Version

A service can register at one of three **tiers** per namespace:

| Tier | What it is | Mutability | Deprecation |
|---|---|---|---|
| `unstable` | Trunk-tip; published on every push from CI | Always overwrites the prior `unstable` | Pinning to it lights up `@deprecated(reason: "unstable — pin to stable or vN for releases")` so codegen flags it in IDE/lint |
| `stable` | Alias to the most-recent numbered cut | Auto-rolls when a new `vN` is registered | Never deprecated |
| `vN` (`v1`, `v2`, …) | Pinned historical cuts | Immutable | Auto-`@deprecated` when a newer `vN` exists |

```graphql
type UserQuery {
  unstable: UserUnstableQuery @deprecated(reason: "unstable — pin to stable or vN for releases")
  stable:   UserV2Query        # alias to the latest cut (currently v2)
  v1:       UserV1Query @deprecated(reason: "use v2")
  v2:       UserV2Query
  # newest fields hoisted to the top
  profile(id: ID!): Profile     # from stable / v2
}
```

Flow:

1. **Trunk publishes to `unstable`.** Every CI green re-registers
   the service at `unstable`. The slot is mutable — no `vN` churn,
   no deprecated noise. Teams building against a dep can opt into
   its `unstable` for fast iteration.
2. **Cut a release.** Freeze `unstable` into a new `vN` and roll
   `stable` forward. Older `vN-1` and earlier flip to
   `@deprecated` automatically.
3. **Caller-side lint forces dependency negotiation.** Set
   `controlclient.Options.BuildTag` and the client refuses to
   register `unstable` from a release-tagged binary. A service that
   cuts a release must pin `stable` or a numbered `vN` on its deps
   — if a dep's `unstable` has diverged with breaking changes, you
   can't cut until that dep cuts (or you adopt the break).

Generated clients propagate `@deprecated` through normal codegen
channels: `protoc-gen-go` emits `// Deprecated: ...`,
graphql-codegen emits JSDoc `@deprecated` on TS hooks,
openapi-generator marks operations deprecated.

## Per-tier policy

Each gateway boots with `--allow-tier` controlling which tiers can
register against it:

```bash
# trunk-friendly gateway: anything goes
$ gwag --allow-tier unstable,stable,vN

# release-track gateway: no unstable
$ gwag --allow-tier stable,vN

# locked-down gateway: pinned cuts only (or stable+vN if you want evergreen)
$ gwag --allow-tier vN
```

A service that tries to register a disallowed tier is rejected at
the control plane with a clear error code. Operators who need
physical isolation between deployments (PCI/HIPAA, blast-radius
separation) run multiple gateways with distinct cluster names — that
side of the picture is a deployment concern, not something the
gateway needs to model.

## Deprecate

Two paths:

1. **Auto-deprecation on version fold.** Older `vN` get `@deprecated` automatically when newer cuts register; `unstable` carries a fixed deprecation reason any time it's queried. Free.
2. **Manual deprecate / undeprecate.** Operators flip a `deprecated` bit on any `(namespace, version)` from the admin UI or RPC, with a reason string. Useful for sunsetting a still-current `vN` ahead of cutting `vN+1`, or for un-deprecating in a rollback.

CI gate with `schema diff --strict`:

```bash
$ gwag schema diff \
    --from https://gw.internal/schema \
    --to   ./candidate.graphql --strict
```

`--strict` fails on changes that can break a working client query.
The rules:

- **Breaking** — exit non-zero: required arg / required input field
  removed, output field removed (any nullability), output field type
  changed, default value changed or removed, required arg / required
  input field added without a default, type / enum value removed.
- **Info** — printed but not a failure: optional arg / optional input
  field removed (callers who didn't pass it are unaffected; callers
  who did get a recoverable validation error), default value added,
  optional arg / field added, deprecation flipped on, new types /
  enums / fields.

The conservative "any removal is breaking" policy will relax once
caller-side usage tracking can answer "is anyone passing this
optional arg?" directly. Until then, the relaxation matches the
asymmetric reality that *adding* fields is always safe but
*removing* fields is mostly safe for the optional ones.

## Retire

Stop heartbeating; the gateway evicts after TTL. The schema
rebuilds; the field disappears.

## Promotion path

The tier flow above is the per-namespace story. Two extra gates wire
the same axis into CI:

1. **`services list`** exposes per-pool hashes — CI diffs hash sets
   across clusters to confirm the bytes match for every
   `(namespace, tier|version)` you intend to promote.
2. **`/schema` + `schema diff --strict`** is the client-perspective
   gate — additions are fine, removals or required-arg changes fail
   the build (rules in [Deprecate](#deprecate)).

Per-cluster drift is prevented by the canonical hash gate in the
pool: a replica with a mismatched proto can't join an existing
`(ns, version)` pool.

## What this *doesn't* (yet) tell you

The `dispatch_duration_seconds` Prometheus histogram already carries
a `caller` label (extracted via the caller-identity seams documented
in [`caller-identity.md`](./caller-identity.md) — forgeable-by-design
"public" mode, HMAC for production, delegated when you want richer
attrs alongside), so "who's calling this method?" is queryable today.

What's still missing is the *deprecated-operation* UI panel — a
per-(namespace, method) view of which callers are still hitting an
`@deprecated` op so you know who to nudge before retiring it. Until
that lands, deprecation is "announce, wait, retire," with the
schema-side warnings emitted automatically and Prometheus answering
"who's still on it?" via the caller label.
