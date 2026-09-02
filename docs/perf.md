<!--
DO NOT EDIT — this file regenerates from sweep JSONs via 'bin/bench perf report'.
Run 'bin/bench perf all' to refresh the inputs under bench/.run/perf/.
-->

# Performance

> _Generated 2026-08-18T21:56:56Z from 3 scenario sweeps via `bin/bench perf report`._

**Headline (proto scenario, last healthy rung):** **49263 RPS** at p95 **23.02ms** with gateway self-time mean **672µs**.

> **Looking for "how does gwag compare to X?"** This page is gwag's
> own throughput on your hardware. For a head-to-head against
> graphql-mesh, Apollo Router and gat on the same backends, see
> [`compare/comparison.md`](../compare/comparison.md); for
> gat against gqlgen / connect-go / grpc-gateway in-process, see
> [`compare/gatbench/`](../compare/gatbench/README.md).

## Machine

| Field | Value |
|---|---|
| Captured at | 2026-08-18T21:54:46Z |
| CPU | AMD Ryzen 9 3900X 12-Core Processor |
| Cores (logical) | 24 |
| RAM | 125.7 GiB |
| OS | Ubuntu 24.04 |
| Kernel | 6.8.0-137-generic |
| Arch | amd64 |
| Go | go1.26.2 |
| Gateway rev | d7af8fe |


## Scenario: `graphql`

- Endpoint: `http://localhost:18080/api/graphql`
- Duration per rep: `5.0s` × 3 reps (rep 1 discarded as warm-up)

| Target RPS | Achieved | Client mean | p50 | p95 | p99 | Gateway self (mean) | Dispatch (mean) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 999 | 466µs | 465µs | 537µs | 687µs | 37µs | 231µs |
| 5000 | 4991 | 584µs | 551µs | 819µs | 1.30ms | 41µs | 298µs |
| 10000 | 9978 | 763µs | 680µs | 1.30ms | 2.42ms | 41µs | 385µs |
| 20000 | 19964 | 775µs | 684µs | 1.43ms | 2.91ms | 31µs | 333µs |
| 30000 | 29969 | 1.16ms | 962µs | 2.74ms | 4.43ms | 38µs | 415µs |
| 40000 | 39581 | 2.62ms | 1.54ms | 8.07ms | 18.04ms | 173µs | 959µs |
| 50000 | 45552 | 34.12ms | 36.94ms | 64.99ms | 85.48ms | 12.03ms | 4.17ms |

**Knee detected at 50000 RPS** (latency_above_50ms): p99 85484µs (85.5ms) exceeds 50ms SLA ceiling. Recommended ceiling: **40000 RPS** on this host.

### Interpretation

**~1649 RPS / core** across 24 logical cores at the recommended ceiling. Gateway self-time mean is **173µs** at the recommended ceiling — this is the per-request overhead the gateway adds on top of whatever the upstream takes.

### Where time + allocs go (40000 RPS, 20s CPU window)

**Top CPU (cumulative):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: cpu
Time: 2026-08-18 14:56:34 PDT
Duration: 20s, Total samples = 159.51s (797.51%)
Showing nodes accounting for 100.78s, 63.18% of 159.51s total
Dropped 1099 nodes (cum <= 0.80s)
      flat  flat%   sum%        cum   cum%
     0.32s   0.2%   0.2%     93.01s 58.31%  net/http.(*conn).serve
     0.05s 0.031%  0.23%     58.90s 36.93%  net/http.serverHandler.ServeHTTP
     0.03s 0.019%  0.25%     58.85s 36.89%  net/http.(*ServeMux).ServeHTTP
     0.04s 0.025%  0.28%     57.84s 36.26%  net/http.HandlerFunc.ServeHTTP
     0.41s  0.26%  0.53%     57.80s 36.24%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
     0.36s  0.23%  0.76%     55.20s 34.61%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
     0.17s  0.11%  0.87%     46.83s 29.36%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
     0.01s 0.0063%  0.87%     46.65s 29.25%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
     0.29s  0.18%  1.05%     46.60s 29.21%  github.com/IodeSystems/graphql-go.writePlannedSelection
     1.14s  0.71%  1.77%     46.26s 29.00%  github.com/IodeSystems/graphql-go.writePlannedField
```

**Top allocs (cumulative alloc_space):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: alloc_space
Time: 2026-08-18 14:56:54 PDT
Showing nodes accounting for 132144.97MB, 89.22% of 148103.31MB total
Dropped 665 nodes (cum <= 740.52MB)
      flat  flat%   sum%        cum   cum%
     167MB  0.11%  0.11% 130552.72MB 88.15%  net/http.(*conn).serve
         0     0%  0.11% 112583.07MB 76.02%  net/http.(*ServeMux).ServeHTTP
         0     0%  0.11% 112583.07MB 76.02%  net/http.HandlerFunc.ServeHTTP
         0     0%  0.11% 112583.07MB 76.02%  net/http.serverHandler.ServeHTTP
 2180.90MB  1.47%  1.59% 112396.71MB 75.89%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
         0     0%  1.59% 106837.66MB 72.14%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
         0     0%  1.59% 91233.41MB 61.60%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
         0     0%  1.59% 91232.91MB 61.60%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
  514.52MB  0.35%  1.93% 91232.91MB 61.60%  github.com/IodeSystems/graphql-go.writePlannedSelection
 1563.61MB  1.06%  2.99% 90718.39MB 61.25%  github.com/IodeSystems/graphql-go.writePlannedField
```

Raw pprof files: `profile-graphql.cpu.pprof` + `profile-graphql.allocs.pprof` under the sweep out-dir; inspect interactively with `go tool pprof`.

## Scenario: `openapi`
pure OpenAPI/HTTP backend (hello_openapi); same Hello shape via HTTP/JSON.


- Endpoint: `http://localhost:18080/api/graphql`
- Duration per rep: `5.0s` × 3 reps (rep 1 discarded as warm-up)

| Target RPS | Achieved | Client mean | p50 | p95 | p99 | Gateway self (mean) | Dispatch (mean) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 999 | 444µs | 445µs | 509µs | 660µs | 38µs | 209µs |
| 5000 | 4995 | 542µs | 520µs | 691µs | 1.11ms | 43µs | 254µs |
| 10000 | 9986 | 711µs | 653µs | 1.12ms | 2.19ms | 45µs | 320µs |
| 20000 | 19938 | 684µs | 620µs | 1.21ms | 2.56ms | 32µs | 258µs |
| 30000 | 29984 | 1.00ms | 872µs | 2.22ms | 3.44ms | 37µs | 315µs |
| 40000 | 39951 | 1.53ms | 1.22ms | 3.85ms | 5.93ms | 41µs | 434µs |
| 50000 | 48885 | 5.05ms | 3.20ms | 16.42ms | 25.33ms | 228µs | 978µs |
| 60000 | 56461 | 34.07ms | 46.37ms | 69.49ms | 110.53ms | 1.05ms | 1.69ms |

**Knee detected at 60000 RPS** (latency_above_50ms): p99 110533µs (110.5ms) exceeds 50ms SLA ceiling. Recommended ceiling: **50000 RPS** on this host.

### Interpretation

**~2037 RPS / core** across 24 logical cores at the recommended ceiling. Gateway self-time mean is **228µs** at the recommended ceiling — this is the per-request overhead the gateway adds on top of whatever the upstream takes.

### Where time + allocs go (50000 RPS, 20s CPU window)

**Top CPU (cumulative):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: cpu
Time: 2026-08-18 14:54:23 PDT
Duration: 20s, Total samples = 183.95s (919.73%)
Showing nodes accounting for 118.30s, 64.31% of 183.95s total
Dropped 1170 nodes (cum <= 0.92s)
      flat  flat%   sum%        cum   cum%
     0.58s  0.32%  0.32%    108.58s 59.03%  net/http.(*conn).serve
     0.07s 0.038%  0.35%     65.91s 35.83%  net/http.serverHandler.ServeHTTP
     0.07s 0.038%  0.39%     65.84s 35.79%  net/http.(*ServeMux).ServeHTTP
     0.07s 0.038%  0.43%     64.99s 35.33%  net/http.HandlerFunc.ServeHTTP
     0.38s  0.21%  0.64%     64.91s 35.29%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
     0.26s  0.14%  0.78%     62.34s 33.89%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
     0.20s  0.11%  0.89%        54s 29.36%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
     0.09s 0.049%  0.94%     53.78s 29.24%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
     0.45s  0.24%  1.18%     53.65s 29.17%  github.com/IodeSystems/graphql-go.writePlannedSelection
     1.44s  0.78%  1.96%     53.28s 28.96%  github.com/IodeSystems/graphql-go.writePlannedField
```

**Top allocs (cumulative alloc_space):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: alloc_space
Time: 2026-08-18 14:54:43 PDT
Showing nodes accounting for 94050.80MB, 90.11% of 104378.34MB total
Dropped 604 nodes (cum <= 521.89MB)
      flat  flat%   sum%        cum   cum%
     123MB  0.12%  0.12% 91178.74MB 87.35%  net/http.(*conn).serve
         0     0%  0.12% 78136.61MB 74.86%  net/http.(*ServeMux).ServeHTTP
         0     0%  0.12% 78136.61MB 74.86%  net/http.HandlerFunc.ServeHTTP
         0     0%  0.12% 78136.61MB 74.86%  net/http.serverHandler.ServeHTTP
 1564.29MB  1.50%  1.62% 78009.30MB 74.74%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
         0     0%  1.62% 74040.40MB 70.93%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
         0     0%  1.62% 62779.20MB 60.15%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
         0     0%  1.62% 62778.70MB 60.15%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
  374.02MB  0.36%  1.97% 62778.70MB 60.15%  github.com/IodeSystems/graphql-go.writePlannedSelection
 1022.57MB  0.98%  2.95% 62404.68MB 59.79%  github.com/IodeSystems/graphql-go.writePlannedField
```

Raw pprof files: `profile-openapi.cpu.pprof` + `profile-openapi.allocs.pprof` under the sweep out-dir; inspect interactively with `go tool pprof`.

## Scenario: `proto`
pure proto/gRPC backend (greeter); baseline for native-format dispatch cost.


- Endpoint: `http://localhost:18080/api/graphql`
- Duration per rep: `5.0s` × 3 reps (rep 1 discarded as warm-up)

| Target RPS | Achieved | Client mean | p50 | p95 | p99 | Gateway self (mean) | Dispatch (mean) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 999 | 507µs | 508µs | 590µs | 808µs | 37µs | 256µs |
| 5000 | 4997 | 671µs | 626µs | 1.08ms | 1.34ms | 41µs | 342µs |
| 10000 | 9995 | 906µs | 789µs | 1.66ms | 2.25ms | 41µs | 389µs |
| 20000 | 19990 | 770µs | 708µs | 1.42ms | 2.05ms | 29µs | 346µs |
| 30000 | 29983 | 1.11ms | 986µs | 2.28ms | 3.21ms | 32µs | 436µs |
| 40000 | 39952 | 1.69ms | 1.37ms | 4.05ms | 6.11ms | 34µs | 608µs |
| 50000 | 49263 | 6.37ms | 3.20ms | 23.02ms | 39.36ms | 672µs | 1.38ms |
| 60000 | 57636 | 33.76ms | 45.77ms | 67.72ms | 107.41ms | 1.34ms | 2.10ms |

**Knee detected at 60000 RPS** (latency_above_50ms): p99 107407µs (107.4ms) exceeds 50ms SLA ceiling. Recommended ceiling: **50000 RPS** on this host.

### Interpretation

**~2053 RPS / core** across 24 logical cores at the recommended ceiling. Gateway self-time mean is **672µs** at the recommended ceiling — this is the per-request overhead the gateway adds on top of whatever the upstream takes.

### Where time + allocs go (50000 RPS, 20s CPU window)

**Top CPU (cumulative):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: cpu
Time: 2026-08-18 14:51:57 PDT
Duration: 20s, Total samples = 173.29s (866.39%)
Showing nodes accounting for 110.70s, 63.88% of 173.29s total
Dropped 1188 nodes (cum <= 0.87s)
      flat  flat%   sum%        cum   cum%
     0.38s  0.22%  0.22%    108.19s 62.43%  net/http.(*conn).serve
     0.05s 0.029%  0.25%     64.18s 37.04%  net/http.serverHandler.ServeHTTP
     0.05s 0.029%  0.28%     64.13s 37.01%  net/http.(*ServeMux).ServeHTTP
     0.07s  0.04%  0.32%     63.21s 36.48%  net/http.HandlerFunc.ServeHTTP
     0.36s  0.21%  0.53%     63.13s 36.43%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
     0.32s  0.18%  0.71%     59.83s 34.53%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
     0.20s  0.12%  0.83%     51.77s 29.87%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
     0.04s 0.023%  0.85%     51.54s 29.74%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
     0.34s   0.2%  1.04%     51.49s 29.71%  github.com/IodeSystems/graphql-go.writePlannedSelection
     0.83s  0.48%  1.52%     51.17s 29.53%  github.com/IodeSystems/graphql-go.writePlannedField
```

**Top allocs (cumulative alloc_space):**

```
File: gateway
Build ID: 0d39df041e80e25f240f8608e4e38de1d2b84c6c
Type: alloc_space
Time: 2026-08-18 14:52:17 PDT
Showing nodes accounting for 46759.14MB, 92.38% of 50614.20MB total
Dropped 477 nodes (cum <= 253.07MB)
      flat  flat%   sum%        cum   cum%
      60MB  0.12%  0.12% 43050.86MB 85.06%  net/http.(*conn).serve
         0     0%  0.12% 36550.74MB 72.21%  net/http.(*ServeMux).ServeHTTP
         0     0%  0.12% 36550.74MB 72.21%  net/http.HandlerFunc.ServeHTTP
         0     0%  0.12% 36550.74MB 72.21%  net/http.serverHandler.ServeHTTP
  805.65MB  1.59%  1.71% 36489.37MB 72.09%  github.com/iodesystems/gwag/v2/gw.(*Gateway).Handler.func1
         0     0%  1.71% 34446.67MB 68.06%  github.com/iodesystems/gwag/v2/gw.(*Gateway).serveGraphQLJSON
         0     0%  1.71% 28851.62MB 57.00%  github.com/IodeSystems/graphql-go.ExecutePlanAppend
         0     0%  1.71% 28851.62MB 57.00%  github.com/IodeSystems/graphql-go.ExecutePlanAppend.func1
  189.51MB  0.37%  2.08% 28851.62MB 57.00%  github.com/IodeSystems/graphql-go.writePlannedSelection
  303.52MB   0.6%  2.68% 28662.11MB 56.63%  github.com/IodeSystems/graphql-go.writePlannedField
```

Raw pprof files: `profile-proto.cpu.pprof` + `profile-proto.allocs.pprof` under the sweep out-dir; inspect interactively with `go tool pprof`.


## Tracing overhead

`WithTracer` is opt-in. When unset, the gateway wires a no-op
tracer and the per-request hot path stays branch-free. When set, the
gateway opens one server-kind span per ingress + one client-kind span
per dispatch, extracts inbound `traceparent`, and injects on
outbound HTTP / gRPC.

Microbench delta from `BenchmarkTracing_GraphQLIngress_*` —
GraphQL ingress over loopback gRPC, `-benchtime=3s -count=3`:

| Config | ns/op (range) | B/op | allocs/op |
|---|---|---|---|
| Tracing off (noop) | ~386k–424k | ~37.7 KB | 359 |
| Tracing on, sync exporter | ~373k–391k | ~44.4 KB | 380 |
| Tracing on, batching exporter | ~377k–382k | ~44.5 KB | 376 |

**+21 allocs and ~6.7 KB per request when tracing is enabled.** The
wall-time delta is below the HTTP-loopback noise floor on this host —
the sync exporter run overlaps the noop baseline. Sampling and
exporter wire time are separate operator concerns; use
`TraceIDRatioBased` for production volumes.

Reproduce:

```bash
go test ./gw/ -bench=BenchmarkTracing_GraphQLIngress -benchmem -run=^$ -benchtime=3s -count=3
```

Wiring + span reference: [`docs/tracing.md`](./tracing.md).

## How to read this

Three numbers tell most of the story per scenario:

- **Achieved RPS / target RPS** — anything < 80 % of target is saturation (gateway, client, or upstream).
- **Gateway self (mean)** — the gateway-only slice of each request (`request_self_seconds` mean). Compare across upstream-latency runs to see "how much does the gateway add"; this number should be roughly upstream-independent.
- **Dispatch (mean)** — upstream time as measured by the gateway. Climbs with configured upstream latency; the delta vs. self-time is the upstream's contribution.

### Knee heuristic

A rung is flagged as the knee when:

- **achieved_below_80pct** — actual RPS < 0.80 × target. The client / gateway / upstream couldn't keep up; throughput collapsed.
- **p99_cliff** — step's p99 > 2 × prior step's p99 **AND** achieved RPS no longer climbed vs the prior step. Catches saturation-via-latency where the gateway goes slow rather than dropping. A pure latency creep with healthy throughput growth is normal queueing under load, not a knee, and is intentionally not flagged.
- **latency_above_50ms** — step's p99 > 50ms. Absolute SLA ceiling — catches the case where throughput keeps climbing past the gateway's healthy zone but p99 has deteriorated past what any production deployment would tolerate.

First-firing predicate stops the sweep; the prior step is the recommended ceiling. Pass `--no-knee` to `bench perf run` to walk every rung regardless (useful for the full curve).

### Regenerating

The one-command path reads `bench/perf-scenarios.yaml`, brings up
the stack and the upstream services each scenario needs, runs every
sweep, and renders this file:

```bash
bin/bench perf
```

Customise the sweep (different RPS rungs, your own query, regression
runs) by editing `bench/perf-scenarios.yaml` or passing
`--config path/to/your.yaml`.

Subcommands for power users:

```bash
bin/bench perf specs                  # print host-specs header only
bin/bench perf run --scenario proto   # one ad-hoc sweep
bin/bench perf report --in-dir ...    # re-render without re-running
```
