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

const dynamicOpenAPISpec = `{
  "openapi": "3.0.0",
  "info": {"title": "billing", "version": "1.0.0"},
  "paths": {
    "/invoices/{id}": {
      "get": {
        "operationId": "getInvoice",
        "parameters": [
          {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {
            "description": "ok",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "id":     {"type": "string"},
                    "amount": {"type": "number"}
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}`

// TestDynamicOpenAPI_Standalone verifies the standalone-mode
// (no-cluster) Register path: control-plane sees an openapi_spec
// binding, addOpenAPISourceLocked creates the source, the schema
// rebuilds, and a query through gw.Handler() dispatches HTTP to the
// registered base URL.
func TestDynamicOpenAPI_Standalone(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/invoices/INV-1" {
			http.Error(w, "wrong path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"INV-1","amount":42.5}`))
	}))
	t.Cleanup(backend.Close)

	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)

	cp := gw.ControlPlane()
	resp, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr:       backend.URL,
		InstanceId: "billing@1",
		Services: []*cpv1.ServiceBinding{{
			Namespace:   "billing",
			OpenapiSpec: []byte(dynamicOpenAPISpec),
		}},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.GetRegistrationId() == "" {
		t.Fatal("empty registration id")
	}

	// Force schema assembly + query via Handler.
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	got := postGraphQLForDynamic(t, srv.URL,
		`{"query":"{ billing { getInvoice(id:\"INV-1\") { id amount } } }"}`)
	if !strings.Contains(got, `INV-1`) || !strings.Contains(got, `42.5`) || strings.Contains(got, `errors`) {
		t.Fatalf("unexpected response: %s", got)
	}

	// Deregister and confirm the source disappears.
	if _, err := cp.Deregister(context.Background(), &cpv1.DeregisterRequest{
		RegistrationId: resp.GetRegistrationId(),
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	got = postGraphQLForDynamic(t, srv.URL,
		`{"query":"{ billing { getInvoice(id:\"INV-1\") { id } } }"}`)
	// After deregister the `billing` namespace container disappears
	// from Query, so the parent field can no longer be selected.
	if !strings.Contains(got, "errors") || !strings.Contains(got, `Cannot query field \"billing\"`) {
		t.Fatalf("expected schema to lose billing namespace after deregister, got: %s", got)
	}
}

func TestDynamicOpenAPI_HashMismatchRejected(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()

	if _, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: "http://a.example/api",
		Services: []*cpv1.ServiceBinding{{
			Namespace:   "billing",
			OpenapiSpec: []byte(dynamicOpenAPISpec),
		}},
	}); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	// Mutate the spec slightly so the hash differs.
	mutated := strings.Replace(dynamicOpenAPISpec, "1.0.0", "1.0.1", 1)
	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: "http://b.example/api",
		Services: []*cpv1.ServiceBinding{{
			Namespace:   "billing",
			OpenapiSpec: []byte(mutated),
		}},
	})
	if err == nil {
		t.Fatal("expected hash-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "different schema hash") {
		t.Fatalf("error: %v (want 'different schema hash')", err)
	}
}

func TestDynamicOpenAPI_BothSpecAndDescriptorRejected(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()

	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: "http://x/",
		Services: []*cpv1.ServiceBinding{{
			Namespace:   "weird",
			ProtoSource: []byte("anything"),
			OpenapiSpec: []byte(dynamicOpenAPISpec),
		}},
	})
	if err == nil {
		t.Fatal("expected error when both descriptor and spec set")
	}
	if !strings.Contains(err.Error(), "may set only one of") {
		t.Fatalf("error: %v", err)
	}
}

func TestDynamicOpenAPI_NeitherSpecNorDescriptorRejected(t *testing.T) {
	gw := New(WithoutMetrics(), WithoutBackpressure(), WithAdminToken([]byte("test")))
	t.Cleanup(gw.Close)
	cp := gw.ControlPlane()

	_, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: "http://x/",
		Services: []*cpv1.ServiceBinding{{
			Namespace: "weird",
		}},
	})
	if err == nil {
		t.Fatal("expected error when neither set")
	}
	if !strings.Contains(err.Error(), "must set") {
		t.Fatalf("error: %v", err)
	}
}

// TestDynamicOpenAPI_CrossGatewayDispatch verifies an OpenAPI spec
// registered on gateway A becomes a GraphQL field on gateway B via
// the registry KV reconciler — the cluster equivalent of the proto
// cross-gateway test.
func TestDynamicOpenAPI_CrossGatewayDispatch(t *testing.T) {
	resetSharedCluster(t)
	_, _ = getSharedCluster(t)
	a := newSharedClusterNode(t, testCluster.a)
	b := newSharedClusterNode(t, testCluster.b)
	waitForPeers(t, a.gw, b.gw)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"INV-1","amount":99}`))
	}))
	t.Cleanup(backend.Close)

	// Register on A.
	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	var lastErr error
	for regCtx.Err() == nil {
		_, err := a.gw.ControlPlane().Register(regCtx, &cpv1.RegisterRequest{
			Addr: backend.URL,
			Services: []*cpv1.ServiceBinding{{
				Namespace:   "billing",
				OpenapiSpec: []byte(dynamicOpenAPISpec),
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

	// Wait for B's reconciler to install the openAPISource locally.
	waitForSlotTest(t, b.gw, "billing", "v1", 15*time.Second)

	// Query through B → dispatches HTTP to backend.
	got := postGraphQLForDynamic(t, b.httpSrv.URL,
		`{"query":"{ billing { getInvoice(id:\"INV-1\") { id amount } } }"}`)
	if strings.Contains(got, "errors") || !strings.Contains(got, "INV-1") {
		t.Fatalf("response via B: %s", got)
	}
}

// TestDynamicOpenAPI_MultiReplica registers two distinct backends
// under the same namespace + matching spec hash and confirms that
// dispatch alternates between them via least-in-flight selection.
// Removes one replica and confirms traffic shifts to the survivor.
func TestDynamicOpenAPI_MultiReplica(t *testing.T) {
	hits := func() (handler http.HandlerFunc, count func() int32) {
		var n atomic.Int32
		handler = func(w http.ResponseWriter, _ *http.Request) {
			n.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"INV","amount":1}`))
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
			Namespace:   "billing",
			OpenapiSpec: []byte(dynamicOpenAPISpec),
		}},
	})
	if err != nil {
		t.Fatalf("Register A: %v", err)
	}
	if _, err := cp.Register(context.Background(), &cpv1.RegisterRequest{
		Addr: b.URL,
		Services: []*cpv1.ServiceBinding{{
			Namespace:   "billing",
			OpenapiSpec: []byte(dynamicOpenAPISpec),
		}},
	}); err != nil {
		t.Fatalf("Register B (same hash, different addr): %v", err)
	}

	// Two replicas now under "billing".
	gw.mu.Lock()
	src := gw.openAPISlot(poolKey{namespace: "billing", version: "v1"})
	gw.mu.Unlock()
	if src == nil {
		t.Fatal("no billing source")
	}
	if got := src.replicaCount(); got != 2 {
		t.Fatalf("replicaCount = %d, want 2", got)
	}

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// Fire several queries serially. With pickReplica picking the
	// lowest in-flight, sequential requests alternate between A and B
	// (each finishes before the next starts).
	for i := 0; i < 10; i++ {
		got := postGraphQLForDynamic(t, srv.URL,
			`{"query":"{ billing { getInvoice(id:\"INV\") { id } } }"}`)
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
	if got := gw.openAPISlot(poolKey{namespace: "billing", version: "v1"}).replicaCount(); got != 1 {
		gw.mu.Unlock()
		t.Fatalf("after deregister A: replicaCount = %d, want 1", got)
	}
	gw.mu.Unlock()

	beforeB := countB()
	for i := 0; i < 5; i++ {
		_ = postGraphQLForDynamic(t, srv.URL,
			`{"query":"{ billing { getInvoice(id:\"INV\") { id } } }"}`)
	}
	afterA, afterB := countA(), countB()
	if afterA != totalA {
		t.Errorf("A still receiving traffic after deregister: was %d, now %d", totalA, afterA)
	}
	if afterB-beforeB != 5 {
		t.Errorf("B should have caught all 5 post-deregister calls: was %d, now %d", beforeB, afterB)
	}
}

// postGraphQLForDynamic posts a GraphQL request to srv.URL+/graphql
// and returns the body as a string. Helper deduped from existing
// fixtures to keep this file self-contained.
func postGraphQLForDynamic(t *testing.T, srvURL, query string) string {
	t.Helper()
	resp, err := http.Post(srvURL+"/graphql", "application/json", strings.NewReader(query))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(body)
}

// silence
var _ = json.Marshal
var _ = time.Second
