# examples/auth

The canonical case the library API is shaped around: hide auth fields
globally, fill them from a registered auth service, hide that service
from the external schema.

## What's here

- `protos/auth.proto` — `AuthService.Resolve(token) → Context`.
- `protos/user.proto` — `UserService.GetMe(GetMeRequest)` where
  `GetMeRequest` embeds `auth.v1.Context`.
- `gen/` — generated Go bindings (`protoc-gen-go` +
  `protoc-gen-go-grpc`).
- `main.go` — wires both services in-process via bufconn, configures
  the gateway, declares `InjectType[*authv1.Context]`.

## Run it

```
$ cd examples/auth
$ go run .
2026/05/05 16:47:14 listening on :8080
```

In another terminal:

```
$ curl -sS -H 'Authorization: Bearer alice' \
       -H 'Content-Type: application/json' \
       -d '{"query": "query { user { getMe { id name tenantId } } }"}' \
       http://localhost:8080/graphql
{"data":{"user":{"getMe":{"id":"u_alice","name":"Demo User","tenantId":"t_demo"}}}}
```

Without the bearer header, the auth resolver short-circuits with
`Reject(CodeUnauthenticated, ...)`:

```
$ curl -sS -H 'Content-Type: application/json' \
       -d '{"query": "query { user { getMe { id name tenantId } } }"}' \
       http://localhost:8080/graphql
{"data":{"user":{"getMe":null}},"errors":[{"message":"missing bearer token","extensions":{"code":"UNAUTHENTICATED"},...}]}
```

## What's hidden

External schema introspection does *not* show:

- the `auth` namespace (registered with `AsInternal()`)
- the `auth` argument on `getMe` (stripped by `InjectType`)

```
$ curl -sS -H 'Content-Type: application/json' \
       -d '{"query": "{ __schema { queryType { fields { name } } } }"}' \
       http://localhost:8080/graphql
{"data":{"__schema":{"queryType":{"fields":[{"name":"user"}]}}}}

$ curl -sS -H 'Content-Type: application/json' \
       -d '{"query": "{ __type(name: \"UserNamespace\") { fields { name args { name } } } }"}' \
       http://localhost:8080/graphql
{"data":{"__type":{"fields":[{"args":[],"name":"getMe"}]}}}
```

`AuthService.Resolve` is called once per HTTP request and cached on the
request context — multiple RPCs in one GraphQL query share the result.

## Regenerating bindings

```
protoc -I protos \
  --go_out=. --go_opt=module=github.com/iodesystems/gwag/v2/examples/auth \
  --go-grpc_out=. --go-grpc_opt=module=github.com/iodesystems/gwag/v2/examples/auth \
  protos/auth.proto protos/user.proto
```

Requires `protoc-gen-go` and `protoc-gen-go-grpc` on `PATH`.
