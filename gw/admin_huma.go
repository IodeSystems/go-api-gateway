package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/iodesystems/gwag/v2/gw/ir"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// AdminHumaRouter mounts the gateway's admin routes — peers,
// services, forget-peer, sign-subscription-token — defined via huma
// (so they emit OpenAPI) on a *http.ServeMux that the example
// gateway can mount at any prefix. Returns the OpenAPI spec bytes
// alongside the mux so the gateway can self-ingest via
// AddOpenAPIBytes (huma → OpenAPI → GraphQL).
//
// Each handler delegates to the existing controlPlane gRPC service
// in-process. The huma route is a thin REST shape on top, which the
// gateway's OpenAPI ingestion lifts into the GraphQL surface — same
// path any external service-defined OpenAPI takes.
//
// Stability: stable
func (g *Gateway) AdminHumaRouter() (*http.ServeMux, []byte, error) {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Admin", "1.0.0")
	// Huma defaults OpenAPIPath to "/openapi", which would serve at
	// /api/admin/../openapi.json after StripPrefix("/api"). Move it
	// under /admin/openapi so inbound /api/admin/openapi.json
	// resolves cleanly.
	cfg.OpenAPIPath = "/admin/openapi"
	api := humago.New(mux, cfg)

	cp, ok := g.ControlPlane().(*controlPlane)
	if !ok {
		return nil, nil, fmt.Errorf("AdminHumaRouter: ControlPlane impl is not *controlPlane")
	}

	huma.Register(api, huma.Operation{
		OperationID: "listPeers",
		Method:      http.MethodGet,
		Path:        "/admin/peers",
		Summary:     "List active cluster peers.",
	}, func(ctx context.Context, _ *struct{}) (*peersOut, error) {
		resp, err := cp.ListPeers(ctx, &cpv1.ListPeersRequest{})
		if err != nil {
			return nil, err
		}
		out := &peersOut{}
		out.Body.Peers = []peerInfo{} // never nil — JSON null breaks NonNull
		for _, p := range resp.GetPeers() {
			out.Body.Peers = append(out.Body.Peers, peerInfo{
				NodeID:       p.GetNodeId(),
				Name:         p.GetName(),
				JoinedUnixMs: p.GetJoinedUnixMs(),
			})
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "listServices",
		Method:      http.MethodGet,
		Path:        "/admin/services",
		Summary:     "List registered services with hashes.",
	}, func(ctx context.Context, _ *struct{}) (*servicesOut, error) {
		resp, err := cp.ListServices(ctx, &cpv1.ListServicesRequest{})
		if err != nil {
			return nil, err
		}
		// Snapshot rejected-join counters under g.mu — orthogonal to
		// the proto wire shape so we can surface them without proto
		// regen. Lookups by (namespace, version).
		rej := cp.gw.rejectedJoinsSnapshot()
		out := &servicesOut{}
		out.Body.Services = []serviceInfo{}
		for _, s := range resp.GetServices() {
			info := serviceInfo{
				Namespace:               s.GetNamespace(),
				Version:                 s.GetVersion(),
				HashHex:                 s.GetHashHex(),
				ReplicaCount:            s.GetReplicaCount(),
				ManualDeprecationReason: s.GetManualDeprecationReason(),
			}
			if r := rej[poolKey{namespace: s.GetNamespace(), version: s.GetVersion()}]; r != nil {
				info.RejectedJoins = &rejectedJoinInfo{
					Count:                            r.Count,
					LastReason:                       r.LastReason,
					LastUnixMs:                       r.LastUnixMs,
					LastMaxConcurrency:               r.LastMaxConcurrency,
					LastMaxConcurrencyPerInstance:    r.LastMaxConcurrencyPerInstance,
					CurrentMaxConcurrency:            r.CurrentMaxConcurrency,
					CurrentMaxConcurrencyPerInstance: r.CurrentMaxConcurrencyPerInstance,
				}
			}
			out.Body.Services = append(out.Body.Services, info)
		}
		out.Body.StableVN = []stableVNEntry{}
		for ns, vN := range resp.GetStableVn() {
			out.Body.StableVN = append(out.Body.StableVN, stableVNEntry{
				Namespace: ns,
				VN:        vN,
			})
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "forgetPeer",
		Method:      http.MethodPost,
		Path:        "/admin/peers/{nodeId}/forget",
		Summary:     "Drop a TTL-expired peer and shrink the cluster's stream replicas.",
	}, func(ctx context.Context, in *forgetIn) (*forgetOut, error) {
		resp, err := cp.ForgetPeer(ctx, &cpv1.ForgetPeerRequest{NodeId: in.NodeID})
		if err != nil {
			return nil, err
		}
		out := &forgetOut{}
		out.Body.Removed = resp.GetRemoved()
		out.Body.NewReplicas = resp.GetNewReplicas()
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "signSubscriptionToken",
		Method:      http.MethodPost,
		Path:        "/admin/sign",
		Summary:     "Mint an HMAC subscription token (subject to the optional delegate).",
	}, func(ctx context.Context, in *signIn) (*signOut, error) {
		resp, err := cp.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
			Channel:    in.Body.Channel,
			TtlSeconds: in.Body.TtlSeconds,
			Kid:        in.Body.Kid,
		})
		if err != nil {
			return nil, err
		}
		out := &signOut{}
		out.Body.Code = resp.GetCode().String()
		out.Body.Hmac = resp.GetHmac()
		out.Body.TimestampUnix = resp.GetTimestampUnix()
		out.Body.Reason = resp.GetReason()
		out.Body.Kid = resp.GetKid()
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "listChannels",
		Method:      http.MethodGet,
		Path:        "/admin/channels",
		Summary:     "List active subscription subjects with their in-process consumer counts.",
	}, func(_ context.Context, _ *struct{}) (*channelsOut, error) {
		out := &channelsOut{}
		// Always emit a non-nil slice so JSON Subscription[] is never null.
		out.Body.Channels = []channelInfo{}
		for _, s := range g.activeSubjects() {
			out.Body.Channels = append(out.Body.Channels, channelInfo{
				Subject:   s.Subject,
				Consumers: s.Consumers,
			})
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "listBindings",
		Method:      http.MethodGet,
		Path:        "/admin/bindings",
		Summary:     "List all channel binding patterns with their payload message types.",
	}, func(_ context.Context, _ *struct{}) (*bindingsOut, error) {
		out := &bindingsOut{}
		out.Body.Bindings = []bindingInfo{}
		for _, e := range g.channelBindingIndexSnapshot() {
			out.Body.Bindings = append(out.Body.Bindings, bindingInfo{
				Pattern:    e.pattern,
				MessageFQN: e.messageFQN,
				Namespace:  e.namespace,
				Version:    e.version,
			})
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "listInjectors",
		Method:      http.MethodGet,
		Path:        "/admin/injectors",
		Summary:     "List every InjectType / InjectPath / InjectHeader registration with its current schema landings.",
	}, func(ctx context.Context, _ *struct{}) (*injectorsOut, error) {
		entries, err := g.injectorInventory()
		if err != nil {
			return nil, err
		}
		out := &injectorsOut{}
		out.Body.Injectors = []injectorInfo{}
		for _, e := range entries {
			info := injectorInfo{
				Kind:       string(e.Kind),
				TypeName:   e.TypeName,
				Path:       e.Path,
				HeaderName: e.HeaderName,
				Hide:       e.Hide,
				Nullable:   e.Nullable,
				State:      string(e.State),
				RegisteredAt: registeredAtInfo{
					File:     e.RegisteredAt.File,
					Line:     e.RegisteredAt.Line,
					Function: e.RegisteredAt.Function,
				},
				Landings: []injectorLandingInfo{},
			}
			for _, l := range e.Landings {
				info.Landings = append(info.Landings, injectorLandingInfo{
					Kind:       l.Kind,
					Namespace:  l.Namespace,
					Version:    l.Version,
					Op:         l.Op,
					TypeName:   l.TypeName,
					FieldName:  l.FieldName,
					ArgName:    l.ArgName,
					HeaderName: l.HeaderName,
				})
			}
			out.Body.Injectors = append(out.Body.Injectors, info)
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "retractStable",
		Method:      http.MethodPost,
		Path:        "/admin/services/{namespace}/stable/retract",
		Summary:     "Retract a namespace's `stable` alias to a lower vN. Operator-driven; refuses to skip past vNs that aren't currently registered.",
	}, func(ctx context.Context, in *retractStableIn) (*retractStableOut, error) {
		resp, err := cp.RetractStable(ctx, &cpv1.RetractStableRequest{
			Namespace: in.Namespace,
			TargetVN:  in.Body.TargetVN,
		})
		if err != nil {
			return nil, err
		}
		out := &retractStableOut{}
		out.Body.PriorVN = resp.GetPriorVN()
		out.Body.NewVN = resp.GetNewVN()
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "deprecate",
		Method:      http.MethodPost,
		Path:        "/admin/services/{namespace}/{version}/deprecate",
		Summary:     "Manually deprecate a (namespace, version). The reason surfaces as @deprecated in SDL alongside any auto-deprecation of older vN cuts.",
	}, func(ctx context.Context, in *deprecateIn) (*deprecateOut, error) {
		_, err := cp.Deprecate(ctx, &cpv1.DeprecateRequest{
			Namespace: in.Namespace,
			Version:   in.Version,
			Reason:    in.Body.Reason,
		})
		if err != nil {
			return nil, err
		}
		return &deprecateOut{}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "undeprecate",
		Method:      http.MethodPost,
		Path:        "/admin/services/{namespace}/{version}/undeprecate",
		Summary:     "Clear a previously-set manual deprecation. Auto-deprecation (older vN cuts) is unaffected.",
	}, func(ctx context.Context, in *undeprecateIn) (*undeprecateOut, error) {
		resp, err := cp.Undeprecate(ctx, &cpv1.UndeprecateRequest{
			Namespace: in.Namespace,
			Version:   in.Version,
		})
		if err != nil {
			return nil, err
		}
		out := &undeprecateOut{}
		out.Body.PriorReason = resp.GetPriorReason()
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "serviceStats",
		Method:      http.MethodGet,
		Path:        "/admin/services/{namespace}/{version}/stats",
		Summary:     "Per-method rolling stats (1m / 1h / 24h windows) for one (namespace, version). Plan §5.",
	}, func(_ context.Context, in *serviceStatsIn) (*serviceStatsOut, error) {
		window, err := parseStatsWindow(in.Window)
		if err != nil {
			return nil, err
		}
		rows := g.snapshot(window, nowFunc())
		out := &serviceStatsOut{}
		out.Body.Window = in.Window
		out.Body.Methods = []methodStatsOut{}
		for _, r := range rows {
			if r.Namespace != in.Namespace || r.Version != in.Version {
				continue
			}
			out.Body.Methods = append(out.Body.Methods, methodStatsOut{
				Method:     r.Method,
				Caller:     r.Caller,
				Count:      r.Count,
				OkCount:    r.OkCount,
				Throughput: r.Throughput,
				P50Millis:  int64(r.P50 / time.Millisecond),
				P95Millis:  int64(r.P95 / time.Millisecond),
				P99Millis:  int64(r.P99 / time.Millisecond),
			})
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "servicesStats",
		Method:      http.MethodGet,
		Path:        "/admin/services/stats",
		Summary:     "Aggregate per-(namespace, version) rolling stats across every registered service. The Services list pulls one row per (ns, ver) without N round-trips. Plan §5.",
	}, func(ctx context.Context, in *servicesStatsIn) (*servicesStatsOut, error) {
		window, err := parseStatsWindow(in.Window)
		if err != nil {
			return nil, err
		}
		rows := g.snapshot(window, nowFunc())
		// Aggregate: collapse per-(method, caller) rows into a single
		// per-(namespace, version) record. Counts sum, throughput sums,
		// and the percentile fields take the max — a worst-case readout
		// is the right summary for a list column ("the slowest method
		// in this service ran at p95=X"). Per-method drill-down stays
		// available via the existing /admin/services/{ns}/{ver}/stats.
		type aggKey struct{ ns, ver string }
		type agg struct {
			count, okCount uint64
			throughput     float64
			p50, p95, p99  time.Duration
		}
		bucket := map[aggKey]*agg{}
		for _, r := range rows {
			k := aggKey{r.Namespace, r.Version}
			a := bucket[k]
			if a == nil {
				a = &agg{}
				bucket[k] = a
			}
			a.count += r.Count
			a.okCount += r.OkCount
			a.throughput += r.Throughput
			if r.P50 > a.p50 {
				a.p50 = r.P50
			}
			if r.P95 > a.p95 {
				a.p95 = r.P95
			}
			if r.P99 > a.p99 {
				a.p99 = r.P99
			}
		}
		// Union with the registry of-truth: every registered
		// (ns, ver) appears in the response, even if zero traffic
		// has hit it yet. Otherwise a freshly-registered service
		// is invisible on the dashboard until its first dispatch.
		if svcResp, err := cp.ListServices(ctx, &cpv1.ListServicesRequest{}); err == nil {
			for _, s := range svcResp.GetServices() {
				k := aggKey{s.GetNamespace(), s.GetVersion()}
				if _, ok := bucket[k]; !ok {
					bucket[k] = &agg{}
				}
			}
		}
		out := &servicesStatsOut{}
		out.Body.Window = in.Window
		out.Body.Services = []serviceStatsRow{}
		for k, a := range bucket {
			out.Body.Services = append(out.Body.Services, serviceStatsRow{
				Namespace:  k.ns,
				Version:    k.ver,
				Count:      a.count,
				OkCount:    a.okCount,
				Throughput: a.throughput,
				P50Millis:  int64(a.p50 / time.Millisecond),
				P95Millis:  int64(a.p95 / time.Millisecond),
				P99Millis:  int64(a.p99 / time.Millisecond),
			})
		}
		// Stable order for UI diff-friendliness; map iteration is
		// nondeterministic on its own.
		sortServiceStatsRows(out.Body.Services)
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "servicesHistory",
		Method:      http.MethodGet,
		Path:        "/admin/services/history",
		Summary:     "Per-bucket history per (namespace, version) for the chosen window. The public status page renders one dot per bucket — color = error ratio. Bucket widths track the underlying ring (1s / 1m / 10m for 1m / 1h / 24h).",
	}, func(ctx context.Context, in *servicesHistoryIn) (*servicesHistoryOut, error) {
		window, err := parseStatsWindow(in.Window)
		if err != nil {
			return nil, err
		}
		rows := g.history(window, nowFunc())
		out := &servicesHistoryOut{}
		out.Body.Window = in.Window
		out.Body.Services = []serviceHistoryRow{}
		seen := make(map[serviceKey]bool, len(rows))
		for _, r := range rows {
			seen[serviceKey{namespace: r.Namespace, version: r.Version}] = true
			row := serviceHistoryRow{
				Namespace: r.Namespace,
				Version:   r.Version,
				Buckets:   make([]historyBucketOut, 0, len(r.Buckets)),
			}
			for _, b := range r.Buckets {
				row.Buckets = append(row.Buckets, historyBucketOut{
					StartUnixSec: b.StartUnixSec,
					DurationSec:  b.DurationSec,
					Count:        b.Count,
					OkCount:      b.OkCount,
					P50Millis:    int64(b.P50 / time.Millisecond),
					P95Millis:    int64(b.P95 / time.Millisecond),
					P99Millis:    int64(b.P99 / time.Millisecond),
				})
			}
			out.Body.Services = append(out.Body.Services, row)
		}
		// Union with the registry of-truth. Services with no stats
		// yet still appear, with an empty Buckets slice — the UI
		// renders a row with a "no traffic" dot strip rather than
		// dropping the service entirely.
		if svcResp, err := cp.ListServices(ctx, &cpv1.ListServicesRequest{}); err == nil {
			for _, s := range svcResp.GetServices() {
				k := serviceKey{namespace: s.GetNamespace(), version: s.GetVersion()}
				if seen[k] {
					continue
				}
				out.Body.Services = append(out.Body.Services, serviceHistoryRow{
					Namespace: s.GetNamespace(),
					Version:   s.GetVersion(),
					Buckets:   []historyBucketOut{},
				})
			}
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "deprecatedStats",
		Method:      http.MethodGet,
		Path:        "/admin/services/deprecated/stats",
		Summary:     "Cross-service report of every deprecated (namespace, version) with per-method + per-caller breakdown, sorted by call volume desc. The 'should I retire this?' panel — high-traffic deprecated services bubble up; zero-traffic ones surface as 'safe to retire' candidates at the bottom. Plan §5.",
	}, func(ctx context.Context, in *deprecatedStatsIn) (*deprecatedStatsOut, error) {
		window, err := parseStatsWindow(in.Window)
		if err != nil {
			return nil, err
		}

		// Source-of-truth for deprecation matches the SDL renderer's
		// rule: manual = slot.deprecationReason (echoed by ListServices
		// as ManualDeprecationReason); auto = "v<max> is current" for
		// any (ns, ver) whose vN is strictly below the namespace's
		// highest registered cut. Reuse cp.ListServices so this stays
		// in lockstep with what the dashboard's Services list flags.
		svcResp, err := cp.ListServices(ctx, &cpv1.ListServicesRequest{})
		if err != nil {
			return nil, err
		}
		maxVN := map[string]int{}
		for _, s := range svcResp.GetServices() {
			ns := s.GetNamespace()
			n := ir.ParseRuntimeVersionN(s.GetVersion())
			if cur, ok := maxVN[ns]; !ok || n > cur {
				maxVN[ns] = n
			}
		}
		type depKey struct{ ns, ver string }
		type depReasons struct{ manual, auto string }
		deprecated := map[depKey]depReasons{}
		for _, s := range svcResp.GetServices() {
			ns := s.GetNamespace()
			ver := s.GetVersion()
			manual := s.GetManualDeprecationReason()
			auto := ""
			if max, ok := maxVN[ns]; ok && ir.ParseRuntimeVersionN(ver) < max {
				auto = fmt.Sprintf("v%d is current", max)
			}
			if manual == "" && auto == "" {
				continue
			}
			deprecated[depKey{ns, ver}] = depReasons{manual, auto}
		}

		rows := g.snapshot(window, nowFunc())

		// Group: service → methods → callers. Total counts roll up so
		// the operator panel can sort either level by call volume.
		type methodAgg struct {
			callers        []callerStatsRow
			count, okCount uint64
			throughput     float64
			p50, p95, p99  time.Duration
		}
		type serviceAgg struct {
			reasons depReasons
			methods map[string]*methodAgg
			totalCount uint64
			totalThroughput float64
		}
		services := map[depKey]*serviceAgg{}
		ensureService := func(k depKey, r depReasons) *serviceAgg {
			a := services[k]
			if a == nil {
				a = &serviceAgg{reasons: r, methods: map[string]*methodAgg{}}
				services[k] = a
			}
			return a
		}
		// Seed every deprecated (ns, ver) so zero-traffic services
		// still surface — the "safe to retire" answer is a row with
		// totalCount == 0, not an absent row.
		for k, r := range deprecated {
			ensureService(k, r)
		}
		for _, r := range rows {
			k := depKey{r.Namespace, r.Version}
			reasons, ok := deprecated[k]
			if !ok {
				continue
			}
			a := ensureService(k, reasons)
			m := a.methods[r.Method]
			if m == nil {
				m = &methodAgg{}
				a.methods[r.Method] = m
			}
			m.callers = append(m.callers, callerStatsRow{
				Caller:     r.Caller,
				Count:      r.Count,
				OkCount:    r.OkCount,
				Throughput: r.Throughput,
				P50Millis:  int64(r.P50 / time.Millisecond),
				P95Millis:  int64(r.P95 / time.Millisecond),
				P99Millis:  int64(r.P99 / time.Millisecond),
			})
			m.count += r.Count
			m.okCount += r.OkCount
			m.throughput += r.Throughput
			if r.P50 > m.p50 {
				m.p50 = r.P50
			}
			if r.P95 > m.p95 {
				m.p95 = r.P95
			}
			if r.P99 > m.p99 {
				m.p99 = r.P99
			}
			a.totalCount += r.Count
			a.totalThroughput += r.Throughput
		}

		out := &deprecatedStatsOut{}
		out.Body.Window = in.Window
		out.Body.Services = []deprecatedServiceRow{}
		for k, a := range services {
			row := deprecatedServiceRow{
				Namespace:       k.ns,
				Version:         k.ver,
				ManualReason:    a.reasons.manual,
				AutoReason:      a.reasons.auto,
				TotalCount:      a.totalCount,
				TotalThroughput: a.totalThroughput,
				Methods:         []deprecatedMethodRow{},
			}
			for method, m := range a.methods {
				// Caller breakdown sorted by count desc; the
				// operator's "who's still hitting this" question.
				sort.Slice(m.callers, func(i, j int) bool {
					if m.callers[i].Count != m.callers[j].Count {
						return m.callers[i].Count > m.callers[j].Count
					}
					return m.callers[i].Caller < m.callers[j].Caller
				})
				row.Methods = append(row.Methods, deprecatedMethodRow{
					Method:     method,
					Count:      m.count,
					OkCount:    m.okCount,
					Throughput: m.throughput,
					P50Millis:  int64(m.p50 / time.Millisecond),
					P95Millis:  int64(m.p95 / time.Millisecond),
					P99Millis:  int64(m.p99 / time.Millisecond),
					Callers:    m.callers,
				})
			}
			sort.Slice(row.Methods, func(i, j int) bool {
				if row.Methods[i].Count != row.Methods[j].Count {
					return row.Methods[i].Count > row.Methods[j].Count
				}
				return row.Methods[i].Method < row.Methods[j].Method
			})
			out.Body.Services = append(out.Body.Services, row)
		}
		// Service rows: high-traffic first (the "chase this") then
		// quiet ones at the bottom (the "safe to retire").
		sort.Slice(out.Body.Services, func(i, j int) bool {
			a, b := out.Body.Services[i], out.Body.Services[j]
			if a.TotalCount != b.TotalCount {
				return a.TotalCount > b.TotalCount
			}
			if a.Namespace != b.Namespace {
				return a.Namespace < b.Namespace
			}
			return a.Version < b.Version
		})
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpList",
		Method:      http.MethodGet,
		Path:        "/admin/mcp",
		Summary:     "Read the MCP surface allowlist (auto_include flag + include / exclude path lists).",
	}, func(_ context.Context, _ *struct{}) (*mcpListOut, error) {
		return mcpListOutFrom(g.mcpConfigSnapshot()), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpInclude",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/include",
		Summary:     "Add a path or glob to the MCP include list. Default-deny mode (auto_include=false) exposes exactly the include entries; in auto_include=true mode the include list is unused. Idempotent — already-present entries are a no-op.",
	}, func(ctx context.Context, in *mcpIncludeIn) (*mcpListOut, error) {
		if in.Body.Path == "" {
			return nil, huma.Error400BadRequest("path must not be empty")
		}
		cfg, err := g.mutateMCPConfig(ctx, func(cfg *MCPConfig) {
			if !containsString(cfg.Include, in.Body.Path) {
				cfg.Include = append(cfg.Include, in.Body.Path)
			}
		})
		if err != nil {
			return nil, err
		}
		return mcpListOutFrom(cfg), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpExclude",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/exclude",
		Summary:     "Add a path or glob to the MCP exclude list. Only consulted in auto_include=true mode (subtractive surface). Idempotent.",
	}, func(ctx context.Context, in *mcpExcludeIn) (*mcpListOut, error) {
		if in.Body.Path == "" {
			return nil, huma.Error400BadRequest("path must not be empty")
		}
		cfg, err := g.mutateMCPConfig(ctx, func(cfg *MCPConfig) {
			if !containsString(cfg.Exclude, in.Body.Path) {
				cfg.Exclude = append(cfg.Exclude, in.Body.Path)
			}
		})
		if err != nil {
			return nil, err
		}
		return mcpListOutFrom(cfg), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpIncludeRemove",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/include/remove",
		Summary:     "Remove a previously-added path or glob from the MCP include list. Idempotent — entries that aren't present are a no-op.",
	}, func(ctx context.Context, in *mcpIncludeIn) (*mcpListOut, error) {
		if in.Body.Path == "" {
			return nil, huma.Error400BadRequest("path must not be empty")
		}
		cfg, err := g.mutateMCPConfig(ctx, func(cfg *MCPConfig) {
			cfg.Include = removeString(cfg.Include, in.Body.Path)
		})
		if err != nil {
			return nil, err
		}
		return mcpListOutFrom(cfg), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpExcludeRemove",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/exclude/remove",
		Summary:     "Remove a previously-added path or glob from the MCP exclude list. Idempotent.",
	}, func(ctx context.Context, in *mcpExcludeIn) (*mcpListOut, error) {
		if in.Body.Path == "" {
			return nil, huma.Error400BadRequest("path must not be empty")
		}
		cfg, err := g.mutateMCPConfig(ctx, func(cfg *MCPConfig) {
			cfg.Exclude = removeString(cfg.Exclude, in.Body.Path)
		})
		if err != nil {
			return nil, err
		}
		return mcpListOutFrom(cfg), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpSetAutoInclude",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/auto-include",
		Summary:     "Toggle auto_include. false (default) = surface is exactly the include list (default-deny); true = surface is every public leaf minus the exclude list. Internal `_*` namespaces are filtered first either way.",
	}, func(ctx context.Context, in *mcpSetAutoIncludeIn) (*mcpListOut, error) {
		cfg, err := g.mutateMCPConfig(ctx, func(cfg *MCPConfig) {
			cfg.AutoInclude = in.Body.AutoInclude
		})
		if err != nil {
			return nil, err
		}
		return mcpListOutFrom(cfg), nil
	})

	// MCP tool surface. Dogfooded via huma so adopters can drive the
	// schema_list / search / expand / query tools through the standard
	// admin OpenAPI path; the dedicated /mcp Streamable HTTP mount
	// wraps the same underlying MCPSchemaList / MCPSchemaSearch /
	// MCPSchemaExpand / MCPQuery gateway methods.

	huma.Register(api, huma.Operation{
		OperationID: "mcpSchemaList",
		Method:      http.MethodGet,
		Path:        "/admin/mcp/schema/list",
		Summary:     "List every operation the MCPConfig allowlist exposes, grouped by Query / Mutation / Subscription.",
	}, func(_ context.Context, _ *struct{}) (*mcpSchemaListOut, error) {
		rows := g.mcpSchemaList()
		out := &mcpSchemaListOut{}
		out.Body.Entries = rows
		if out.Body.Entries == nil {
			out.Body.Entries = []schemaListEntry{}
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpSchemaSearch",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/schema/search",
		Summary:     "Search the MCP-allowed operation surface by dot-segmented path glob AND/OR regex (over op name, arg names, description body). Empty body returns every allowed op.",
	}, func(_ context.Context, in *mcpSchemaSearchIn) (*mcpSchemaSearchOut, error) {
		rows, err := g.mcpSchemaSearch(schemaSearchInput{
			PathGlob: in.Body.PathGlob,
			Regex:    in.Body.Regex,
		})
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		out := &mcpSchemaSearchOut{}
		out.Body.Entries = rows
		if out.Body.Entries == nil {
			out.Body.Entries = []schemaSearchEntry{}
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpSchemaExpand",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/schema/expand",
		Summary:     "Return the structured definition of one MCP-allowed op path or type name, plus the transitive type closure (args + return types for ops; fields + variants for types).",
	}, func(_ context.Context, in *mcpSchemaExpandIn) (*mcpSchemaExpandOut, error) {
		res, err := g.mcpSchemaExpand(in.Body.Name)
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		out := &mcpSchemaExpandOut{}
		out.Body.Result = res
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "mcpQuery",
		Method:      http.MethodPost,
		Path:        "/admin/mcp/query",
		Summary:     "Execute a GraphQL operation in-process and wrap the result in ResponseWithEvents (events always empty in v1). Bearer auth (not the allowlist) is the security boundary — the allowlist is operator-curated discovery guidance.",
	}, func(ctx context.Context, in *mcpQueryIn) (*mcpQueryOut, error) {
		res, err := g.mcpQuery(ctx, mcpQueryInput{
			Query:         in.Body.Query,
			Variables:     in.Body.Variables,
			OperationName: in.Body.OperationName,
		})
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		out := &mcpQueryOut{}
		out.Body.Result = res
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "drain",
		Method:      http.MethodPost,
		Path:        "/admin/drain",
		Summary:     "Trigger graceful drain. /health flips to 503 immediately; the call returns when active streams reach 0 (or the per-request timeout expires).",
	}, func(ctx context.Context, in *drainIn) (*drainOut, error) {
		ttl := time.Duration(in.Body.TimeoutSeconds) * time.Second
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		dctx, cancel := context.WithTimeout(ctx, ttl)
		defer cancel()
		err := g.Drain(dctx)
		out := &drainOut{}
		out.Body.Drained = err == nil
		out.Body.ActiveStreams = int(g.streamGlobal.Load())
		if err != nil {
			out.Body.Reason = err.Error()
		}
		return out, nil
	})

	spec, err := json.Marshal(api.OpenAPI())
	if err != nil {
		return nil, nil, fmt.Errorf("marshal openapi: %w", err)
	}
	return mux, spec, nil
}

type peerInfo struct {
	NodeID       string `json:"nodeId"`
	Name         string `json:"name,omitempty"`
	JoinedUnixMs int64  `json:"joinedUnixMs"`
}

type peersOut struct {
	Body struct {
		Peers []peerInfo `json:"peers"`
	}
}

type serviceInfo struct {
	Namespace    string `json:"namespace"`
	Version      string `json:"version"`
	HashHex      string `json:"hashHex"`
	ReplicaCount uint32 `json:"replicaCount"`
	// ManualDeprecationReason is the operator-set reason from
	// Deprecate/Undeprecate. Empty = no manual deprecation.
	// Auto-deprecation (older `vN`) is computed by the UI from
	// `version` plus the namespace's latest registered `vN`.
	ManualDeprecationReason string `json:"manualDeprecationReason,omitempty"`
	// RejectedJoins surfaces the running `registerSlotLocked`
	// rejection counter for this slot's (namespace, version) — set
	// when a registration arrives whose caps / hash / kind don't
	// match the existing vN occupant. Helps operators spot stale
	// JetStream KV state vs. binary-version drift without profiling.
	RejectedJoins *rejectedJoinInfo `json:"rejectedJoins,omitempty"`
}

// rejectedJoinInfo is the admin-side projection of
// rejectedJoinSummary (defined on the Gateway). Hides the internal
// struct from the huma surface and gives the JSON shape stable.
type rejectedJoinInfo struct {
	Count                            uint32 `json:"count"`
	LastReason                       string `json:"lastReason"`
	LastUnixMs                       int64  `json:"lastUnixMs"`
	LastMaxConcurrency               int    `json:"lastMaxConcurrency"`
	LastMaxConcurrencyPerInstance    int    `json:"lastMaxConcurrencyPerInstance"`
	CurrentMaxConcurrency            int    `json:"currentMaxConcurrency"`
	CurrentMaxConcurrencyPerInstance int    `json:"currentMaxConcurrencyPerInstance"`
}

// stableVNEntry surfaces the per-namespace stable alias target. Plan §4
// monotonic invariant: VN advances at registration, decreases only via
// RetractStable. The UI matches `serviceInfo.version == "v" + VN` to
// flag which row is the current stable target.
type stableVNEntry struct {
	Namespace string `json:"namespace"`
	VN        uint32 `json:"vN"`
}

type servicesOut struct {
	Body struct {
		Services []serviceInfo   `json:"services"`
		StableVN []stableVNEntry `json:"stableVN"`
	}
}

type forgetIn struct {
	NodeID string `path:"nodeId"`
}

type forgetOut struct {
	Body struct {
		Removed     bool   `json:"removed"`
		NewReplicas uint32 `json:"newReplicas"`
	}
}

type signIn struct {
	Body struct {
		Channel    string `json:"channel"`
		TtlSeconds int64  `json:"ttlSeconds"`
		// Optional rotation key id; empty = legacy default secret.
		Kid string `json:"kid,omitempty"`
	}
}

type signOut struct {
	Body struct {
		Code          string `json:"code"`
		Hmac          string `json:"hmac,omitempty"`
		TimestampUnix int64  `json:"timestampUnix,omitempty"`
		Reason        string `json:"reason,omitempty"`
		// Echoes the kid the gateway signed under; clients must pass
		// it back as the `kid` arg on the subscribe field.
		Kid string `json:"kid,omitempty"`
	}
}

type channelInfo struct {
	Subject   string `json:"subject"`
	Consumers int    `json:"consumers"`
}

type channelsOut struct {
	Body struct {
		Channels []channelInfo `json:"channels"`
	}
}

type bindingInfo struct {
	Pattern    string `json:"pattern"`
	MessageFQN string `json:"messageFQN"`
	Namespace  string `json:"namespace"`
	Version    string `json:"version"`
}

type bindingsOut struct {
	Body struct {
		Bindings []bindingInfo `json:"bindings"`
	}
}

type retractStableIn struct {
	Namespace string `path:"namespace"`
	Body      struct {
		TargetVN uint32 `json:"targetVN"`
	}
}

type retractStableOut struct {
	Body struct {
		PriorVN uint32 `json:"priorVN"`
		NewVN   uint32 `json:"newVN"`
	}
}

type deprecateIn struct {
	Namespace string `path:"namespace"`
	Version   string `path:"version"`
	Body      struct {
		Reason string `json:"reason"`
	}
}

type deprecateOut struct {
	Body struct {
	}
}

type undeprecateIn struct {
	Namespace string `path:"namespace"`
	Version   string `path:"version"`
}

type undeprecateOut struct {
	Body struct {
		PriorReason string `json:"priorReason"`
	}
}

// serviceStatsIn — window defaults to "1m". Plan §5 followup adds
// by=replica when per-replica stats land.
//
// Window is a plain string (not an `enum:` constraint) because the
// OpenAPI → GraphQL ingest can't surface enum values that start with
// a digit ("1m", "1h", "24h" are all invalid GraphQL identifiers).
// parseStatsWindow validates inside the handler, so the boundary
// check still runs.
type serviceStatsIn struct {
	Namespace string `path:"namespace"`
	Version   string `path:"version"`
	Window    string `query:"window" default:"1m"`
}

type methodStatsOut struct {
	Method     string  `json:"method"`
	Caller     string  `json:"caller"`
	Count      uint64  `json:"count"`
	OkCount    uint64  `json:"okCount"`
	Throughput float64 `json:"throughput"`
	P50Millis  int64   `json:"p50Millis"`
	P95Millis  int64   `json:"p95Millis"`
	P99Millis  int64   `json:"p99Millis"`
}

type serviceStatsOut struct {
	Body struct {
		Window  string           `json:"window"`
		Methods []methodStatsOut `json:"methods"`
	}
}

// servicesStatsIn — aggregate stats across every registered service.
// Window strings match the per-service variant (1m, 1h, 24h). See
// serviceStatsIn for the enum-vs-plain-string note.
type servicesStatsIn struct {
	Window string `query:"window" default:"24h"`
}

type serviceStatsRow struct {
	Namespace  string  `json:"namespace"`
	Version    string  `json:"version"`
	Count      uint64  `json:"count"`
	OkCount    uint64  `json:"okCount"`
	Throughput float64 `json:"throughput"`
	P50Millis  int64   `json:"p50Millis"`
	P95Millis  int64   `json:"p95Millis"`
	P99Millis  int64   `json:"p99Millis"`
}

type servicesStatsOut struct {
	Body struct {
		Window   string            `json:"window"`
		Services []serviceStatsRow `json:"services"`
	}
}

// deprecatedStatsIn — window match the per-service variants. See
// serviceStatsIn for the enum-vs-plain-string note.
type deprecatedStatsIn struct {
	Window string `query:"window" default:"24h"`
}

// servicesHistoryIn — per-bucket history feeds the public status
// page. Default 1h gives 60 dots (one per minute) — a comfortable
// strip for a desktop layout. Window strings see serviceStatsIn for
// the enum-vs-plain-string note.
type servicesHistoryIn struct {
	Window string `query:"window" default:"1h"`
}

// historyBucketOut is one ring-bucket: a fixed-width slice of time.
// The dot-strip UI renders one dot per row in the array; color is
// (Count - OkCount) / Count.
type historyBucketOut struct {
	StartUnixSec int64  `json:"startUnixSec"`
	DurationSec  int64  `json:"durationSec"`
	Count        uint64 `json:"count"`
	OkCount      uint64 `json:"okCount"`
	P50Millis    int64  `json:"p50Millis"`
	P95Millis    int64  `json:"p95Millis"`
	P99Millis    int64  `json:"p99Millis"`
}

type serviceHistoryRow struct {
	Namespace string             `json:"namespace"`
	Version   string             `json:"version"`
	Buckets   []historyBucketOut `json:"buckets"`
}

type servicesHistoryOut struct {
	Body struct {
		Window   string              `json:"window"`
		Services []serviceHistoryRow `json:"services"`
	}
}

// callerStatsRow is the per-caller leaf of the deprecated-services
// drilldown. Mirrors methodStatsOut minus the Method field, which
// hangs one level above on deprecatedMethodRow.
type callerStatsRow struct {
	Caller     string  `json:"caller"`
	Count      uint64  `json:"count"`
	OkCount    uint64  `json:"okCount"`
	Throughput float64 `json:"throughput"`
	P50Millis  int64   `json:"p50Millis"`
	P95Millis  int64   `json:"p95Millis"`
	P99Millis  int64   `json:"p99Millis"`
}

// deprecatedMethodRow rolls per-(method) stats with a nested caller
// breakdown. Method-level percentiles take the worst across callers
// (max), matching the servicesStats aggregate posture.
type deprecatedMethodRow struct {
	Method     string           `json:"method"`
	Count      uint64           `json:"count"`
	OkCount    uint64           `json:"okCount"`
	Throughput float64          `json:"throughput"`
	P50Millis  int64            `json:"p50Millis"`
	P95Millis  int64            `json:"p95Millis"`
	P99Millis  int64            `json:"p99Millis"`
	Callers    []callerStatsRow `json:"callers"`
}

// deprecatedServiceRow is one (namespace, version) entry. Either
// ManualReason or AutoReason is non-empty (often both); the UI shows
// both badges so operators distinguish "older vN" from "operator
// flagged it." Methods empty + TotalCount 0 ⇒ deprecated but no
// recorded traffic in the window: a "safe to retire" candidate.
type deprecatedServiceRow struct {
	Namespace       string                `json:"namespace"`
	Version         string                `json:"version"`
	ManualReason    string                `json:"manualReason,omitempty"`
	AutoReason      string                `json:"autoReason,omitempty"`
	TotalCount      uint64                `json:"totalCount"`
	TotalThroughput float64               `json:"totalThroughput"`
	Methods         []deprecatedMethodRow `json:"methods"`
}

type deprecatedStatsOut struct {
	Body struct {
		Window   string                 `json:"window"`
		Services []deprecatedServiceRow `json:"services"`
	}
}

func sortServiceStatsRows(rows []serviceStatsRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Namespace != rows[j].Namespace {
			return rows[i].Namespace < rows[j].Namespace
		}
		return rows[i].Version < rows[j].Version
	})
}

// parseStatsWindow maps the operator-friendly window strings
// (matching the huma enum) to the registry's time.Duration windows.
func parseStatsWindow(s string) (time.Duration, error) {
	switch s {
	case "", "1m":
		return time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "24h":
		return 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("unknown window %q (want 1m, 1h, or 24h)", s)
}

type mcpListOut struct {
	Body struct {
		AutoInclude bool     `json:"autoInclude"`
		Include     []string `json:"include"`
		Exclude     []string `json:"exclude"`
	}
}

type mcpIncludeIn struct {
	Body struct {
		Path string `json:"path"`
	}
}

type mcpExcludeIn struct {
	Body struct {
		Path string `json:"path"`
	}
}

type mcpSetAutoIncludeIn struct {
	Body struct {
		AutoInclude bool `json:"autoInclude"`
	}
}

func mcpListOutFrom(cfg MCPConfig) *mcpListOut {
	out := &mcpListOut{}
	out.Body.AutoInclude = cfg.AutoInclude
	if cfg.Include == nil {
		out.Body.Include = []string{}
	} else {
		out.Body.Include = cfg.Include
	}
	if cfg.Exclude == nil {
		out.Body.Exclude = []string{}
	} else {
		out.Body.Exclude = cfg.Exclude
	}
	return out
}

func containsString(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func removeString(xs []string, v string) []string {
	out := xs[:0:0]
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

type mcpSchemaListOut struct {
	Body struct {
		Entries []schemaListEntry `json:"entries"`
	}
}

type mcpSchemaSearchIn struct {
	Body struct {
		PathGlob string `json:"pathGlob,omitempty"`
		Regex    string `json:"regex,omitempty"`
	}
}

type mcpSchemaSearchOut struct {
	Body struct {
		Entries []schemaSearchEntry `json:"entries"`
	}
}

type mcpSchemaExpandIn struct {
	Body struct {
		Name string `json:"name"`
	}
}

type mcpSchemaExpandOut struct {
	Body struct {
		Result *schemaExpandResult `json:"result"`
	}
}

type mcpQueryIn struct {
	Body struct {
		Query         string         `json:"query"`
		Variables     map[string]any `json:"variables,omitempty"`
		OperationName string         `json:"operationName,omitempty"`
	}
}

type mcpQueryOut struct {
	Body struct {
		Result *mcpResponseWithEvents `json:"result"`
	}
}

type drainIn struct {
	Body struct {
		// TimeoutSeconds caps how long Drain waits for active streams
		// to reach zero before returning. 0 → 30s default.
		TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
	}
}

type drainOut struct {
	Body struct {
		Drained       bool   `json:"drained"`
		ActiveStreams int    `json:"activeStreams"`
		Reason        string `json:"reason,omitempty"`
	}
}

type registeredAtInfo struct {
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Function string `json:"function,omitempty"`
}

type injectorLandingInfo struct {
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Version    string `json:"version,omitempty"`
	Op         string `json:"op,omitempty"`
	TypeName   string `json:"typeName,omitempty"`
	FieldName  string `json:"fieldName,omitempty"`
	ArgName    string `json:"argName,omitempty"`
	HeaderName string `json:"headerName,omitempty"`
}

type injectorInfo struct {
	Kind         string                `json:"kind"`
	TypeName     string                `json:"typeName,omitempty"`
	Path         string                `json:"path,omitempty"`
	HeaderName   string                `json:"headerName,omitempty"`
	Hide         bool                  `json:"hide"`
	Nullable     bool                  `json:"nullable"`
	State        string                `json:"state"`
	RegisteredAt registeredAtInfo      `json:"registeredAt"`
	Landings     []injectorLandingInfo `json:"landings"`
}

type injectorsOut struct {
	Body struct {
		Injectors []injectorInfo `json:"injectors"`
	}
}
