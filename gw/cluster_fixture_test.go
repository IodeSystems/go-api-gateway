package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nats-io/nats.go/jetstream"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// sharedCluster is a package-level 2-node NATS cluster started once
// in TestMain and reused across all cluster tests. Each test creates
// its own gateways that connect to the shared NATS servers, avoiding
// the per-test Raft meta-leader election cost (~15 s per test).
type sharedCluster struct {
	once sync.Once
	// Two NATS servers forming a cluster. Each has its own JetStream
	// and data directory. The servers are started in TestMain and
	// shut down when the test process exits.
	a, b *Cluster
	// Shared temp directory for both nodes' JetStream storage.
	dataDir string
	// mu guards setup/teardown.
	mu sync.Mutex
}

var testCluster sharedCluster

// getSharedCluster returns the shared 2-node cluster, starting it
// lazily on first call. The cluster is shut down in TestMain's
// deferred cleanup.
func getSharedCluster(t *testing.T) (a, b *Cluster) {
	t.Helper()
	testCluster.once.Do(func() {
		testCluster.dataDir = t.TempDir()
		addrA := freeAddr(t)
		addrB := freeAddr(t)

		dirA := testCluster.dataDir + "/nodeA"
		dirB := testCluster.dataDir + "/nodeB"
		os.MkdirAll(dirA, 0755)
		os.MkdirAll(dirB, 0755)

		var err error
		testCluster.a, err = startSharedClusterNode(t, "A", addrA, []string{addrB}, dirA)
		if err != nil {
			t.Fatalf("shared cluster node A: %v", err)
		}
		testCluster.b, err = startSharedClusterNode(t, "B", addrB, []string{addrA}, dirB)
		if err != nil {
			testCluster.a.Close()
			t.Fatalf("shared cluster node B: %v", err)
		}

		// Wait for both nodes to have JetStream ready and peer.
		jsCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, n := range []*Cluster{testCluster.a, testCluster.b} {
			if err := n.WaitForJetStream(jsCtx); err != nil {
				t.Fatalf("WaitForJetStream %s: %v", n.Server.Name(), err)
			}
		}

		// Wait for route to be established (both nodes see each other).
		peerCtx, peerCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer peerCancel()
		for peerCtx.Err() == nil {
			routesA := testCluster.a.Server.NumRoutes()
			routesB := testCluster.b.Server.NumRoutes()
			if routesA >= 1 && routesB >= 1 {
				return
			}
			select {
			case <-peerCtx.Done():
				t.Fatalf("shared cluster route convergence timeout: A routes=%d B routes=%d", routesA, routesB)
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	return testCluster.a, testCluster.b
}

// startSharedClusterNode is like startClusterNode but only starts the
// NATS server (no gateway, no control plane). Used by getSharedCluster.
func startSharedClusterNode(t *testing.T, name, clusterAddr string, peers []string, dataDir string) (*Cluster, error) {
	clientAddr := freeAddr(t)
	cluster, err := StartCluster(ClusterOptions{
		NodeName:      name,
		ClientListen:  clientAddr,
		ClusterListen: clusterAddr,
		Peers:         peers,
		DataDir:       dataDir,
		StartTimeout:  10 * time.Second,
		LogLevel:      "silent",
	})
	if err != nil {
		return nil, fmt.Errorf("[%s] StartCluster: %w", name, err)
	}
	return cluster, nil
}

// resetSharedCluster purges all KV buckets so the next test starts
// with a clean slate. Must be called at the beginning of each cluster
// test (via t.Cleanup or explicitly).
func resetSharedCluster(t *testing.T) {
	t.Helper()
	jsCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	testCluster.mu.Lock()
	clusterA := testCluster.a
	clusterB := testCluster.b
	testCluster.mu.Unlock()
	for _, cl := range []*Cluster{clusterA, clusterB} {
		if cl == nil || cl.JS == nil {
			continue
		}
		// Delete and recreate buckets. Using the first node's JS context
		// is sufficient — the buckets are cluster-scoped.
		for _, bucket := range []string{
			peersBucketName,
			registryBucketName,
			stableBucketName,
			deprecatedBucketName,
			mcpConfigBucketName,
		} {
			kv, err := cl.JS.KeyValue(jsCtx, bucket)
			if err != nil {
				// Bucket may not exist yet — that's fine.
				continue
			}
			_ = kv.Delete(jsCtx, bucket, jetstream.LastRevision(0))
			_ = cl.JS.DeleteKeyValue(jsCtx, bucket)
		}
	}
	// Brief pause to let JetStream settle after bucket deletion.
	time.Sleep(200 * time.Millisecond)
}

// newSharedClusterNode creates a gateway + control plane attached to
// the shared NATS cluster node (a or b). The gateway and its listeners
// are cleaned up via t.Cleanup.
func newSharedClusterNode(t *testing.T, cl *Cluster) *clusterNode {
	t.Helper()
	gw := New(WithCluster(cl), WithoutMetrics(), WithoutBackpressure())
	t.Cleanup(gw.Close)

	cpAddr := freeAddr(t)
	cpLis, err := net.Listen("tcp", cpAddr)
	if err != nil {
		t.Fatalf("cp listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	cpv1.RegisterControlPlaneServer(grpcSrv, gw.ControlPlane())
	go func() { _ = grpcSrv.Serve(cpLis) }()
	t.Cleanup(grpcSrv.Stop)

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	// Start cluster tracking (creates KV buckets, watch loops, reconciler).
	trkCtx, trkCancel := context.WithCancel(context.Background())
	t.Cleanup(trkCancel)
	if _, err := gw.startClusterTracking(trkCtx); err != nil {
		t.Fatalf("startClusterTracking: %v", err)
	}

	return &clusterNode{
		cluster: cl, gw: gw, cpAddr: cpAddr,
		httpSrv: srv, grpcSrv: grpcSrv,
	}
}

// waitForPeers blocks until both gateways see at least 2 live peers
// (each other + self).
func waitForPeers(t *testing.T, ga, gb *Gateway) {
	t.Helper()
	peerCtx, peerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer peerCancel()
	for peerCtx.Err() == nil {
		if len(ga.peers.LivePeers()) >= 2 && len(gb.peers.LivePeers()) >= 2 {
			return
		}
		select {
		case <-peerCtx.Done():
			t.Fatalf("peer convergence timeout: A live=%v B live=%v",
				ga.peers.LivePeers(), gb.peers.LivePeers())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForSlotTest blocks until the gateway has a slot for (ns, ver).
func waitForSlotTest(t *testing.T, gw *Gateway, ns, ver string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil {
		gw.mu.Lock()
		s := gw.slots[poolKey{namespace: ns, version: ver}]
		gw.mu.Unlock()
		if s != nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("slot %s/%s never appeared on gateway (timeout %v)", ns, ver, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestMain starts and stops the shared cluster fixture.
func TestMain(m *testing.M) {
	code := m.Run()
	// Shut down shared cluster after all tests.
	testCluster.mu.Lock()
	if testCluster.a != nil {
		testCluster.a.Close()
	}
	if testCluster.b != nil {
		testCluster.b.Close()
	}
	testCluster.mu.Unlock()
	os.Exit(code)
}
