# Walkthrough: one registry, three client ecosystems

Clone the repo, run the example, and generate typed clients in TS,
Go, and Python off the same live registry. Then edit a service,
re-codegen, and watch the change propagate without restarting the
gateway.

Time: 10-15 minutes if you have the codegen tools already installed.

## What you'll need

- Go 1.21+ (to run the gateway and the example services)
- `curl` (to hit the schema endpoints)
- For codegen — install only the ecosystems you care about:
  - **TS / GraphQL**: `pnpm` (or `npm`) + `graphql-codegen` (pulled in via the example's package.json)
  - **Go / proto**: `buf` ([install](https://buf.build/docs/installation))
  - **Python / OpenAPI**: `openapi-generator-cli` ([install](https://openapi-generator.tech/docs/installation))

You don't need all three. Pick the ones your team uses. Each is
independent — adding a new ecosystem is curl + one tool.

## Step 1: Boot the example

```bash
git clone https://github.com/iodesystems/gwag && cd gwag
cd examples/multi && ./run.sh
```

That brings up a gateway on `:8080` (HTTP) + `:50090` (control
plane), plus two example services that self-register over the
control plane:

- `greeter` — a proto service with a single `Hello` RPC.
- `library` — a proto service with `ListBooks` / `GetBook` /
  `AddBook` RPCs.

Both registered as proto, but the wedge below shows them re-emitted
in all three formats simultaneously.

In another terminal, sanity-check the consolidated GraphQL surface:

```bash
curl -s -X POST localhost:8080/api/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ greeter { hello(name:\"world\") { greeting } } }"}'
# → {"data":{"greeter":{"hello":{"greeting":"Hello, world!"}}}}
```

Open `http://localhost:8080/` in a browser for the admin UI's
schema explorer. (The boot token is logged at gateway startup and
written to `/tmp/gwag-multi/admin-token`; paste it when prompted.)

## Step 2: Codegen three typed-client packages

### GraphQL → TypeScript (graphql-codegen)

Browser / mobile path. One SDL, typed React hooks (or Apollo Client
or urql — pick the flavor).

```bash
mkdir -p /tmp/walkthrough/ts && cd /tmp/walkthrough/ts
curl 'http://localhost:8080/api/schema/graphql' > schema.graphql

cat > codegen.yml <<'EOF'
schema: ./schema.graphql
documents: ./src/**/*.graphql
generates:
  src/gateway.ts:
    plugins:
      - typescript
      - typescript-operations
EOF

pnpm init -y
pnpm add -D @graphql-codegen/cli @graphql-codegen/typescript @graphql-codegen/typescript-operations
mkdir -p src && echo 'query Hello { greeter { hello(name: "world") { greeting } } }' > src/hello.graphql
pnpm exec graphql-codegen --config codegen.yml
# → src/gateway.ts contains typed Hello query + result types
```

Now your TS service code uses `HelloQuery` / `HelloQueryVariables`
from the generated file — type-safe, refactor-safe, IDE-aware.

### Proto → Go (buf)

The Go service-to-service path. One FileDescriptorSet → idiomatic
gRPC stubs, no JSON in the middle.

```bash
mkdir -p /tmp/walkthrough/go && cd /tmp/walkthrough/go
curl 'http://localhost:8080/api/schema/proto?service=greeter' > greeter.fds

cat > buf.gen.yaml <<'EOF'
version: v1
plugins:
  - plugin: buf.build/protocolbuffers/go
    out: gen
    opt: paths=source_relative
  - plugin: buf.build/grpc/go
    out: gen
    opt: paths=source_relative
EOF

buf generate greeter.fds
# → gen/greeter/v1/greeter.pb.go + greeter_grpc.pb.go
```

Wire it into a client:

```go
import (
    greeterv1 "yourmodule/gen/greeter/v1"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

// Note: gRPC ingress runs on the control-plane port (:50090), not :8080.
// In examples/multi the same gRPC server hosts ControlPlane + the
// gateway's GRPCUnknownHandler — see cmd/gateway/main.go.
conn, _ := grpc.NewClient("localhost:50090", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := greeterv1.NewGreeterServiceClient(conn)
resp, _ := client.Hello(ctx, &greeterv1.HelloRequest{Name: "world"})
// resp.Greeting == "Hello, world!"
```

The gateway's `gw.GRPCUnknownHandler()` translates the call through
the IR layer to whichever upstream serves the namespace — your
client doesn't care that the gateway is in the middle.

### OpenAPI → Python (openapi-generator)

The data team's path, or "any language openapi-generator supports"
(40+ targets — Java, C#, Ruby, Rust, Kotlin…).

```bash
mkdir -p /tmp/walkthrough/py && cd /tmp/walkthrough/py
curl 'http://localhost:8080/api/schema/openapi?service=greeter' > greeter.json

openapi-generator-cli generate \
  -i greeter.json -g python -o ./client \
  --additional-properties=packageName=greeter_client
# → ./client/greeter_client/ — a pip-installable Python package
```

Use it per the generated `client/README.md` — `openapi-generator`
produces a `Configuration` + `ApiClient` + one `*Api` class per
tag, with methods named after each `operationId`. The synthesized
spec's `servers` entry points at the gateway's HTTP ingress
(`http://localhost:8080/api/ingress/...`).

The gateway synthesizes OpenAPI for proto-origin services — same
IR, different renderer. The Python team gets a pip-installable REST
client regardless of the upstream wire format.

## Step 3: Edit a service, re-codegen

```bash
# Add a field to greeter.proto:
$EDITOR examples/multi/protos/greeter.proto
# (add: string locale = 2; to HelloRequest)
```

Restart just the greeter service:

```bash
# Ctrl-C the greeter process in the run.sh output (or kill it by PID)
# Then re-run from examples/multi:
go run ./cmd/greeter &
```

The greeter service re-registers over the control plane. The
gateway's schema rebuilds. **No gateway restart needed.**

Re-run any of the codegen steps from Step 2 — the new field is
there. Your TS / Go / Python clients regenerate in seconds.

For the Go side specifically, you can verify without re-codegenning
first:

```bash
curl -s 'http://localhost:8080/api/schema/proto?service=greeter' | \
  protoc --decode_raw 2>&1 | grep -A1 'locale'
```

Edit once; every typed client picks it up on the next codegen.

## Step 4: Add a new service

Write a tiny self-registering service. The control-plane client
takes `.proto` bytes (or `fs.FS` for multi-file layouts):

```go
package main

import (
    "context"
    _ "embed"
    "log"

    "github.com/iodesystems/gwag/v2/controlclient"
)

//go:embed weather.proto
var weatherProto []byte

func main() {
    ctx := context.Background()
    reg, err := controlclient.SelfRegister(ctx, controlclient.Options{
        GatewayAddr: "localhost:50090",
        ServiceAddr: "localhost:50053",
        Services: []controlclient.Service{
            {Namespace: "weather", ProtoSource: weatherProto},
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer reg.Close(ctx)

    // ... start your gRPC server on :50053 ...
}
```

The moment your service heartbeats in, `Query.weather` appears in
GraphQL, `?service=weather` works on all three schema endpoints,
and your codegen pipelines see it next time they run. No gateway
deploy, no static config edit, no schema review checkout.

Full lifecycle reference (version pinning, `@deprecated`
propagation, retirement): [`lifecycle.md`](./lifecycle.md).

## Step 5: When your upstream isn't proto

The example uses proto services because proto's the canonical
service-to-service format, but the gateway ingests OpenAPI and
downstream GraphQL on equal footing:

```go
// OpenAPI service — point at the spec + the upstream base URL.
gw.AddOpenAPI("./billing-openapi.json",
    gateway.As("billing"),
    gateway.To("https://billing.internal"))

// Downstream GraphQL — stitched into the consolidated schema with
// a namespace prefix.
gw.AddGraphQL("https://legacy-graphql.internal/graphql",
    gateway.As("legacy"))
```

Both produce the same three-format codegen surface. Your TS team
can run graphql-codegen against `?service=billing` even though
billing speaks OpenAPI 3.x upstream; the gateway translates.

Control-plane (self-register) variants exist for both — see
`ServiceBinding.openapi_spec` and `ServiceBinding.graphql_endpoint`
in `gw/proto/controlplane/v1/control.proto`.

Other docs cover operational concerns, auth, versioning, cluster,
pub/sub, embedded mode, and Federation positioning — see
[`../README.md`](../README.md) for the index.
