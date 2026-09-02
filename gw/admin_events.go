package gateway

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aev1 "github.com/iodesystems/gwag/v2/gw/proto/adminevents/v1"
)

//go:embed proto/adminevents/v1/adminevents.proto
var admineventsProtoSource []byte

// adminEventsNamespace is the registration namespace under which the
// AdminEvents proto is exposed. Surfaces in the SDL as
// `admin_events_watchServices`.
const adminEventsNamespace = "admin_events"

// Shortcuts so the registry hooks don't have to import the
// adminevents package directly.
const (
	adminEventsActionRegistered   = aev1.ServiceChange_ACTION_REGISTERED
	adminEventsActionDeregistered = aev1.ServiceChange_ACTION_DEREGISTERED
)

// adminEventsOutputDesc caches the ServiceChange message descriptor
// for the broker acquire call. Resolved once at first use.
var (
	adminEventsOutputOnce sync.Once
	adminEventsOutputDesc protoreflect.MessageDescriptor
	adminEventsOutputErr  error
)

func resolveAdminEventsOutputDesc() (protoreflect.MessageDescriptor, error) {
	adminEventsOutputOnce.Do(func() {
		fd, err := compileProtoBytes("adminevents.proto", admineventsProtoSource, nil)
		if err != nil {
			adminEventsOutputErr = err
			return
		}
		svc := fd.Services().ByName(protoreflect.Name("AdminEvents"))
		if svc == nil {
			adminEventsOutputErr = fmt.Errorf("admin_events: service AdminEvents not found")
			return
		}
		md := svc.Methods().ByName(protoreflect.Name("WatchServices"))
		if md == nil {
			adminEventsOutputErr = fmt.Errorf("admin_events: method WatchServices not found")
			return
		}
		adminEventsOutputDesc = md.Output()
	})
	return adminEventsOutputDesc, adminEventsOutputErr
}

// AddAdminEvents registers the built-in AdminEvents proto so the
// gateway's GraphQL schema gains the `admin_events_watchServices`
// Subscription field. Whenever the registry mutates, the gateway
// publishes a ServiceChange to NATS — clients subscribed via WS
// receive frames in real time.
//
// Cluster-less gateways can still register the schema fragment so
// SDL-driven codegen consumers pick up the subscription field;
// runtime subscriptions return a "subscription broker not available"
// error from the handler if no broker resolves at subscribe time.
//
// Stability: stable
func (g *Gateway) AddAdminEvents() error {
	fd, err := compileProtoBytes("adminevents.proto", admineventsProtoSource, nil)
	if err != nil {
		return fmt.Errorf("admin_events: compile: %w", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addInternalProtoSlotLocked(
		adminEventsNamespace,
		"v1",
		fd,
		admineventsProtoSource,
		nil,
		map[string]internalProtoSubscriptionHandler{
			"WatchServices": g.adminEventsWatchServicesHandler,
		},
	)
}

// adminEventsWatchServicesHandler is the in-process subscription
// handler for admin_events_watchServices. It joins the gateway's
// NATS subscription broker on the WatchServices subject for the
// requested namespace and returns a channel of decoded ServiceChange
// events.
func (g *Gateway) adminEventsWatchServicesHandler(ctx context.Context, args map[string]any) (any, error) {
	broker := g.subscriptionBroker()
	if broker == nil {
		return nil, fmt.Errorf("gateway: subscription broker not available")
	}
	ns, _ := args["namespace"].(string)
	subject := fmt.Sprintf("events.%s.WatchServices.%s", adminEventsNamespace, ns)
	if ns == "" {
		subject = fmt.Sprintf("events.%s.WatchServices.*", adminEventsNamespace)
	}
	outputDesc, err := resolveAdminEventsOutputDesc()
	if err != nil {
		return nil, err
	}
	source, release, err := broker.acquire(subject, outputDesc)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		release()
	}()
	return source, nil
}

// publishServiceChange fires a ServiceChange event onto the NATS
// subject the admin_events_watchServices Subscription field reads
// from. No-op when the gateway isn't in cluster mode.
//
// Subject convention:
//
//	events.admin_events.WatchServices.<namespace>
//
// so subscribers with `namespace=""` get a NATS wildcard match
// (`events.admin_events.WatchServices.*`).
func (g *Gateway) publishServiceChange(action aev1.ServiceChange_Action, ns, ver, addr string, replicas uint32) {
	cl := g.cfg.cluster
	if cl == nil || cl.Conn == nil {
		return
	}
	msg := &aev1.ServiceChange{
		Action:          action,
		Namespace:       ns,
		Version:         ver,
		Addr:            addr,
		TimestampUnixMs: time.Now().UnixMilli(),
		ReplicaCount:    replicas,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	subject := fmt.Sprintf("events.%s.WatchServices.%s", adminEventsNamespace, ns)
	_ = cl.Conn.Publish(subject, payload)
}
