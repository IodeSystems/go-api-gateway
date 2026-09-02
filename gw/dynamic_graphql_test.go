package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// dynamicGraphQLBackend stands up a fake downstream GraphQL service
// that answers introspection with petsIntrospection (defined in
// graphql_ingest_test.go) and per-test query handler. Returns the
// httptest.Server and a counter of non-introspection queries.
func dynamicGraphQLBackend(t *testing.T, queryHandler func(query string, vars map[string]any) any) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var queries atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.Unmarshal(body, &req)
		if strings.Contains(req.Query, "IntrospectionQuery") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(petsIntrospection))
			return
		}
		queries.Add(1)
		var data any
		if queryHandler != nil {
			data = queryHandler(req.Query, req.Variables)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(srv.Close)
	return srv, &queries
}

// TestDynamicGraphQL_Standalone is the standalone-mode (no-cluster)
// happy path: control-plane sees a graphql_endpoint binding, the
// gateway introspects it at Register time, the schema rebuilds with
// the namespace-prefixed types, and a query through gw.Handler()
// forwards to the upstream.
func TestDynamicGraphQL_Standalone(t *testing.T) {
	backend, queries := dynamicGraphQLBackend(t, func(_ string, _ map[string]any) any {
		return map[string]any{
			"users": []map[string]any{{"id": "1", "name": "alice", "role": "ADMIN"}},
		}
	})

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)

	cp := gw.ControlPlane()
	resp, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr:       backend.URL, // ignored for graphql but valid
		InstanceId: "pets@1",
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "pets",
			GraphqlEndpoint: backend.URL,
		}},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.GetRegistrationId() == "" {
		t.Fatal("empty registration id")
	}

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	got := postGraphQLForDynamic(t, srv.URL, `{"query":"{ pets { users { id name role } } }"}`)
	if strings.Contains(got, "errors") || !strings.Contains(got, "alice") {
		t.Fatalf("unexpected response: %s", got)
	}
	if queries.Load() < 1 {
		t.Errorf("backend never received the query (count=%d)", queries.Load())
	}

	// Deregister → source removed → schema loses the `pets` namespace.
	if _, err := cp.Deregister(context.Background(), &cpv1.DeregisterRequest{
		RegistrationId: resp.GetRegistrationId(),
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	got = postGraphQLForDynamic(t, srv.URL, `{"query":"{ pets { users { id } } }"}`)
	if !strings.Contains(got, "errors") || !strings.Contains(got, `Cannot query field \"pets\"`) {
		t.Fatalf("expected pets namespace to disappear, got: %s", got)
	}
}

func TestDynamicGraphQL_HashMismatchRejected(t *testing.T) {
	// First Register loads petsIntrospection. Second Register hits a
	// different backend that returns a tweaked introspection — the
	// hash differs, so the gateway must reject.
	backendA, _ := dynamicGraphQLBackend(t, nil)
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()

	if _, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: backendA.URL,
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "pets",
			GraphqlEndpoint: backendA.URL,
		}},
	}); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	// Different backend that returns a slightly different
	// introspection (extra type) — different bytes → different hash.
	mutated := strings.Replace(petsIntrospection, `"MEMBER"`, `"VIEWER"`, 1)
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mutated))
	}))
	t.Cleanup(backendB.Close)

	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: backendB.URL,
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "pets",
			GraphqlEndpoint: backendB.URL,
		}},
	})
	if err == nil {
		t.Fatal("expected hash-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "different schema hash") {
		t.Fatalf("error: %v (want 'different schema hash')", err)
	}
}

func TestDynamicGraphQL_AllThreeFormsRejected(t *testing.T) {
	// Setting graphql_endpoint together with file_descriptor_set OR
	// openapi_spec must fail.
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()
	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: "http://x",
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "weird",
			ProtoSource:     []byte("anything"),
			GraphqlEndpoint: "http://x/graphql",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "may set only one of") {
		t.Fatalf("expected 'may set only one of', got %v", err)
	}
}

func TestDynamicGraphQL_NamespaceRequired(t *testing.T) {
	// graphql_endpoint with empty namespace is rejected — unlike the
	// proto and OpenAPI paths, there's no fallback (proto stems are
	// derived from the .proto filename; OpenAPI from Info.Title;
	// GraphQL has neither).
	backend, _ := dynamicGraphQLBackend(t, nil)
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()
	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: backend.URL,
		Services: []*cpv1.ServiceBinding{{
			GraphqlEndpoint: backend.URL,
		}},
	})
	if err == nil {
		t.Fatal("expected error when namespace is empty")
	}
	if !strings.Contains(err.Error(), "graphql_endpoint binding requires explicit namespace") {
		t.Fatalf("error: %v", err)
	}
}

// TestDynamicGraphQL_MultiReplica registers two distinct upstream
// GraphQL endpoints under the same namespace + matching introspection
// hash. Confirms dispatch alternates via least-in-flight selection,
// then deregisters one replica and confirms traffic shifts to the
// survivor. Mirrors TestDynamicOpenAPI_MultiReplica.
func TestDynamicGraphQL_MultiReplica(t *testing.T) {
	hits := func() (handler http.HandlerFunc, count func() int32) {
		var n atomic.Int32
		handler = func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Query string `json:"query"`
			}
			_ = json.Unmarshal(body, &req)
			if strings.Contains(req.Query, "IntrospectionQuery") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(petsIntrospection))
				return
			}
			n.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"users":[{"id":"1"}]}}`))
		}
		return handler, n.Load
	}
	hA, countA := hits()
	hB, countB := hits()
	a := httptest.NewServer(hA)
	t.Cleanup(a.Close)
	b := httptest.NewServer(hB)
	t.Cleanup(b.Close)

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()

	regA, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: a.URL,
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "pets",
			GraphqlEndpoint: a.URL,
		}},
	})
	if err != nil {
		t.Fatalf("Register A: %v", err)
	}
	if _, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: b.URL,
		Services: []*cpv1.ServiceBinding{{
			Namespace:       "pets",
			GraphqlEndpoint: b.URL,
		}},
	}); err != nil {
		t.Fatalf("Register B (same hash, different endpoint): %v", err)
	}

	gw.mu.Lock()
	src := gw.graphQLSlot(poolKey{namespace: "pets", version: "v1"})
	gw.mu.Unlock()
	if src == nil {
		t.Fatal("no pets source")
	}
	if got := src.replicaCount(); got != 2 {
		t.Fatalf("replicaCount = %d, want 2", got)
	}

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// Fire 10 queries serially. With pickReplica picking the lowest
	// in-flight, sequential requests alternate between A and B.
	for i := 0; i < 10; i++ {
		got := postGraphQLForDynamic(t, srv.URL, `{"query":"{ pets { users { id } } }"}`)
		if strings.Contains(got, "errors") {
			t.Fatalf("dispatch errored: %s", got)
		}
	}
	totalA, totalB := countA(), countB()
	if totalA+totalB != 10 {
		t.Fatalf("backend hits don't sum to 10: A=%d B=%d", totalA, totalB)
	}
	if totalA == 0 || totalB == 0 {
		t.Fatalf("expected pickReplica to spread load, got A=%d B=%d", totalA, totalB)
	}

	// Drop A's registration → only B should serve subsequent calls.
	if _, err := cp.Deregister(context.Background(), &cpv1.DeregisterRequest{
		RegistrationId: regA.GetRegistrationId(),
	}); err != nil {
		t.Fatalf("Deregister A: %v", err)
	}
	gw.mu.Lock()
	if got := gw.graphQLSlot(poolKey{namespace: "pets", version: "v1"}).replicaCount(); got != 1 {
		gw.mu.Unlock()
		t.Fatalf("after deregister A: replicaCount = %d, want 1", got)
	}
	gw.mu.Unlock()

	beforeB := countB()
	for i := 0; i < 5; i++ {
		_ = postGraphQLForDynamic(t, srv.URL, `{"query":"{ pets { users { id } } }"}`)
	}
	afterA, afterB := countA(), countB()
	if afterA != totalA {
		t.Errorf("A still receiving traffic after deregister: was %d, now %d", totalA, afterA)
	}
	if afterB-beforeB != 5 {
		t.Errorf("B should have caught all 5 post-deregister calls: was %d, now %d", beforeB, afterB)
	}
}

// TestDynamicGraphQL_CrossGatewayDispatch is the cluster equivalent
// of the standalone test: register on A, expect B's reconciler to
// pick up the introspection bytes from KV and create the source so a
// query through B forwards to the upstream.
func TestDynamicGraphQL_CrossGatewayDispatch(t *testing.T) {
	resetSharedCluster(t)
	_, _ = getSharedCluster(t)
	a := newSharedClusterNode(t, testCluster.a)
	b := newSharedClusterNode(t, testCluster.b)
	waitForPeers(t, a.gw, b.gw)

	backend, _ := dynamicGraphQLBackend(t, func(_ string, _ map[string]any) any {
		return map[string]any{
			"users": []map[string]any{{"id": "42", "name": "alice", "role": "ADMIN"}},
		}
	})

	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	var lastErr error
	for regCtx.Err() == nil {
		_, err := a.gw.ControlPlane().Register(regCtx, &cpv1.RegisterRequest{
			Addr: backend.URL,
			Services: []*cpv1.ServiceBinding{{
				Namespace:       "pets",
				GraphqlEndpoint: backend.URL,
			}},
		})
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		select {
		case <-regCtx.Done():
			break
		case <-time.After(250 * time.Millisecond):
		}
	}
	if lastErr != nil {
		t.Fatalf("Register: %v", lastErr)
	}

	// Wait for B's reconciler to install the source.
	waitForSlotTest(t, b.gw, "pets", "v1", 15*time.Second)

	// Query through B forwards to backend.
	got := postGraphQLForDynamic(t, b.httpSrv.URL,
		`{"query":"{ pets { users { id name role } } }"}`)
	if strings.Contains(got, "errors") || !strings.Contains(got, "alice") {
		t.Fatalf("response via B: %s", got)
	}
}
