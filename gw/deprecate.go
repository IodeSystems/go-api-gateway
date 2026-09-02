package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

// Plan §5: per-(namespace, version) manual deprecation. Operators flip
// it on/off via Deprecate / Undeprecate admin RPCs; the renderer OR-
// combines the manual reason with auto-deprecation of older `vN`
// cuts. State lives on the slot in-process and (in cluster mode) in
// the `gwag-deprecated` KV bucket so it propagates across
// peers and survives restarts.

// deprecatedKVValue is the JSON payload written to the deprecated KV
// bucket. Just the operator-supplied reason today; future fields
// (DeprecatedAt, DeprecatedBy) can land additively without breaking
// the wire format.
type deprecatedKVValue struct {
	Reason string `json:"reason"`
}

func deprecatedKey(ns, ver string) string {
	return ns + "_" + ver
}

// setDeprecationLocked applies a manual deprecation (or clears it
// when `reason == ""`) to the local slot index AND the side-state
// mirror of the deprecated KV. Returns the prior reason and whether
// a slot was found. Caller holds g.mu. Cluster propagation is the
// caller's responsibility — admin RPCs call this then write to the
// KV bucket; the watch loop on remote nodes calls this without
// writing back.
//
// The side-state mirror (`g.deprecation`) is what makes the slot-
// registers-after-watch race benign: when a fresh slot lands via
// reconciler, registerSlotLocked reads `g.deprecation` and stamps
// the prior reason on the new slot before bake.
func (g *Gateway) setDeprecationLocked(ns, ver, reason string) (priorReason string, ok bool) {
	key := poolKey{namespace: ns, version: ver}
	if g.deprecation == nil {
		g.deprecation = map[poolKey]string{}
	}
	priorReason = g.deprecation[key]
	if reason == "" {
		delete(g.deprecation, key)
	} else {
		g.deprecation[key] = reason
	}
	s, exists := g.slots[key]
	if !exists {
		return priorReason, false
	}
	if s.deprecationReason == reason {
		return priorReason, true
	}
	s.deprecationReason = reason
	g.bakeSlotIRLocked(s)
	return priorReason, true
}

// Deprecate is the admin RPC counterpart to setDeprecationLocked: it
// validates the request, applies in-process, writes to the cluster
// KV bucket when present, and triggers a schema rebuild + service-
// change event so live peers re-fetch.
func (cp *controlPlane) Deprecate(ctx context.Context, req *cpv1.DeprecateRequest) (*cpv1.DeprecateResponse, error) {
	ns, ver, reason := req.GetNamespace(), req.GetVersion(), req.GetReason()
	if ns == "" || ver == "" {
		return nil, errors.New("controlplane: namespace and version are required")
	}
	if reason == "" {
		return nil, errors.New("controlplane: reason is required (use Undeprecate to clear)")
	}

	cp.gw.mu.Lock()
	_, exists := cp.gw.setDeprecationLocked(ns, ver, reason)
	if !exists {
		cp.gw.mu.Unlock()
		return nil, fmt.Errorf("controlplane: %s/%s is not currently registered on this gateway", ns, ver)
	}
	t := cp.gw.peers
	rebuildErr := cp.gw.assembleLocked()
	cp.gw.mu.Unlock()
	if rebuildErr != nil {
		return nil, fmt.Errorf("controlplane: schema rebuild after deprecate: %w", rebuildErr)
	}

	if t != nil && t.deprecated != nil {
		val := deprecatedKVValue{Reason: reason}
		b, _ := json.Marshal(val)
		kctx, cancel := kvCtx(ctx)
		_, err := t.deprecated.Put(kctx, deprecatedKey(ns, ver), b)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("controlplane: persist deprecate to KV: %w", err)
		}
	}

	cp.gw.publishServiceChange(adminEventsActionRegistered, ns, ver, "", 0)
	return &cpv1.DeprecateResponse{}, nil
}

// Undeprecate clears a previously-set manual deprecation. Returns the
// prior reason for operator-friendly UX (so an accidental
// undeprecate can be undone). Auto-deprecation (older vN) is
// untouched — only the manual override is removed.
func (cp *controlPlane) Undeprecate(ctx context.Context, req *cpv1.UndeprecateRequest) (*cpv1.UndeprecateResponse, error) {
	ns, ver := req.GetNamespace(), req.GetVersion()
	if ns == "" || ver == "" {
		return nil, errors.New("controlplane: namespace and version are required")
	}

	cp.gw.mu.Lock()
	prior, exists := cp.gw.setDeprecationLocked(ns, ver, "")
	if !exists {
		cp.gw.mu.Unlock()
		return nil, fmt.Errorf("controlplane: %s/%s is not currently registered on this gateway", ns, ver)
	}
	t := cp.gw.peers
	rebuildErr := cp.gw.assembleLocked()
	cp.gw.mu.Unlock()
	if rebuildErr != nil {
		return nil, fmt.Errorf("controlplane: schema rebuild after undeprecate: %w", rebuildErr)
	}

	if t != nil && t.deprecated != nil {
		kctx, cancel := kvCtx(ctx)
		err := t.deprecated.Delete(kctx, deprecatedKey(ns, ver))
		cancel()
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return &cpv1.UndeprecateResponse{PriorReason: prior}, fmt.Errorf("controlplane: clear deprecate KV: %w", err)
		}
	}

	cp.gw.publishServiceChange(adminEventsActionRegistered, ns, ver, "", 0)
	return &cpv1.UndeprecateResponse{PriorReason: prior}, nil
}

// observeDeprecatedFromKVLocked reflects an authoritative KV value
// into local slot state. Called by the watch loop on every node.
// "Authoritative" means: KV-truth, not monotonic — a delete event
// clears the local reason (UnDeprecate semantics from a peer).
// Caller holds g.mu.
func (g *Gateway) observeDeprecatedFromKVLocked(ns, ver, reason string) {
	g.setDeprecationLocked(ns, ver, reason)
}

// parseDeprecatedKey reverses deprecatedKey: "<ns>_<ver>" → (ns, ver).
// Versions in this codebase are either "unstable" or "v<N>" (no
// underscores), so a right-most underscore split is unambiguous.
func parseDeprecatedKey(key string) (ns, ver string, ok bool) {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '_' {
			return key[:i], key[i+1:], i > 0 && i < len(key)-1
		}
	}
	return "", "", false
}

// deprecatedWatchLoop converges every node's local slot deprecation
// state with the cluster KV bucket. PUT events stamp the reason on
// the matching slot; DELETE events clear it. Each transition
// triggers a schema rebuild so live consumers re-fetch promptly.
// IncludeHistory ensures a fresh node joining mid-life sees the
// current set, not just future changes — operators don't have to
// re-deprecate every restart.
func (t *peerTracker) deprecatedWatchLoop(ctx context.Context) {
	defer close(t.deprecatedDone)
	w, err := t.deprecated.WatchAll(ctx, jetstream.IncludeHistory())
	if err != nil {
		t.gw.cfg.cluster.Server.Warnf("gateway: deprecated watch start: %v", err)
		return
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.Updates():
			if !ok {
				return
			}
			if ev == nil {
				continue
			}
			ns, ver, parsedOK := parseDeprecatedKey(ev.Key())
			if !parsedOK {
				t.gw.cfg.cluster.Server.Warnf("gateway: deprecated bucket: malformed key %q", ev.Key())
				continue
			}
			var reason string
			switch ev.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				reason = ""
			default:
				var v deprecatedKVValue
				if err := json.Unmarshal(ev.Value(), &v); err != nil {
					t.gw.cfg.cluster.Server.Warnf("gateway: deprecated bucket: bad value at %s: %v", ev.Key(), err)
					continue
				}
				reason = v.Reason
			}
			t.gw.mu.Lock()
			t.gw.observeDeprecatedFromKVLocked(ns, ver, reason)
			rebuildErr := t.gw.assembleLocked()
			t.gw.mu.Unlock()
			if rebuildErr != nil {
				t.gw.cfg.cluster.Server.Warnf("gateway: schema rebuild after deprecated update: %v", rebuildErr)
			}
		}
	}
}
