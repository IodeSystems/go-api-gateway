package gateway

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// internalProtoDispatcher implements ir.Dispatcher for one in-process
// proto RPC. Symmetric with protoDispatcher: marshal canonical args
// into a dynamicpb request, run the user runtime middleware chain,
// invoke the handler, adapt the response back to a canonical map.
//
// No replicas, no backpressure, no per-replica sem — dispatch is a
// direct Go call. Metrics still fire via RecordDispatch so the slot's
// per-method histograms behave the same way operator dashboards
// expect.
type internalProtoDispatcher struct {
	inputDesc  protoreflect.MessageDescriptor
	outputDesc protoreflect.MessageDescriptor
	handler    Handler // chain(inner): user runtime middleware around the in-process handler
	namespace  string
	version    string
	op         string
}

// newInternalProtoInvocationHandler returns the inner Handler that
// performs the in-process call. The handler converts the returned
// protoreflect.ProtoMessage into a pooled *dynamicpb.Message so the
// caller — protoDispatcher-style canonical-args path or the gRPC
// ingress's wire-level SendMsg — gets the same response shape as the
// upstream-gRPC proto path. Concrete generated types round-trip
// through wire bytes; *dynamicpb.Message is returned as-is.
func newInternalProtoInvocationHandler(src *internalProtoSource, sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor, metrics Metrics) Handler {
	method := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
	ns, ver := src.namespace, src.version
	methodName := string(md.Name())
	outputDesc := md.Output()
	h := src.handlers[methodName]
	return Handler(func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		start := time.Now()
		if h == nil {
			err := fmt.Errorf("gateway: no handler registered for %s/%s/%s", ns, ver, methodName)
			elapsed := time.Since(start)
			metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
			addDispatchTime(ctx, elapsed)
			return nil, err
		}
		resp, err := h(ctx, req)
		elapsed := time.Since(start)
		metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
		addDispatchTime(ctx, elapsed)
		if err != nil {
			return nil, err
		}
		// Normalise to *dynamicpb.Message: both the canonical-args
		// dispatcher and the gRPC ingress dispatch expect that type
		// on the response side (matches protoDispatcher behaviour).
		dm, err := normaliseInternalProtoResponse(resp, outputDesc)
		if err != nil {
			return nil, err
		}
		return dm, nil
	})
}

// newInternalProtoDispatcher wraps the inner handler with the user
// runtime middleware chain and adapts to ir.Dispatcher.
func newInternalProtoDispatcher(src *internalProtoSource, sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor, chain Middleware, metrics Metrics) *internalProtoDispatcher {
	inner := newInternalProtoInvocationHandler(src, sd, md, metrics)
	return &internalProtoDispatcher{
		inputDesc:  md.Input(),
		outputDesc: md.Output(),
		handler:    chain(inner),
		namespace:  src.namespace,
		version:    src.version,
		op:         string(md.Name()),
	}
}

// Dispatch satisfies ir.Dispatcher.
func (d *internalProtoDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.internal",
		namespaceAttr(d.namespace),
		versionAttr(d.version),
		methodAttr(d.op),
	)
	defer span.End()
	ctx = withDispatchOpInfo(ctx, d.namespace, d.version, d.op)
	req := acquireDynamicMessage(d.inputDesc)
	defer releaseDynamicMessage(d.inputDesc, req)
	if err := argsToMessage(args, req); err != nil {
		return nil, err
	}
	resp, err := d.handler(ctx, req)
	if err != nil {
		return nil, err
	}
	respMsg := resp.(*dynamicpb.Message)
	out := messageToMap(respMsg)
	releaseDynamicMessage(respMsg.Descriptor(), respMsg)
	return out, nil
}

// normaliseInternalProtoResponse converts a handler response into a
// pooled *dynamicpb.Message of the method's declared output type.
// Concrete generated types round-trip through wire bytes; nil
// returns a zero-valued message; an already-dynamicpb response is
// passed through. The round-trip for concrete types costs one
// Marshal + one Unmarshal — acceptable for the v1 use case
// (pub/sub returns mostly empty messages); promote to a concrete-
// aware path if it ever shows up in a profile.
func normaliseInternalProtoResponse(resp protoreflect.ProtoMessage, outputDesc protoreflect.MessageDescriptor) (*dynamicpb.Message, error) {
	if dm, ok := resp.(*dynamicpb.Message); ok {
		return dm, nil
	}
	dm := acquireDynamicMessage(outputDesc)
	if resp == nil {
		return dm, nil
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		releaseDynamicMessage(outputDesc, dm)
		return nil, fmt.Errorf("internalproto: marshal response: %w", err)
	}
	if err := proto.Unmarshal(b, dm); err != nil {
		releaseDynamicMessage(outputDesc, dm)
		return nil, fmt.Errorf("internalproto: unmarshal response: %w", err)
	}
	return dm, nil
}

// internalProtoSubscriptionDispatcher implements ir.Dispatcher for one
// in-process server-streaming proto RPC. Dispatch returns a chan any of
// decoded events that graphql-go's Subscribe field receives.
//
// Stream-cap accounting and broker-handle release are the handler's
// responsibility (it's the only place that knows whether a broker is
// in play). For ps.sub, the handler joins the gateway's subBroker and
// arms a goroutine on ctx.Done() that releases the per-subject fanout.
type internalProtoSubscriptionDispatcher struct {
	handler   internalProtoSubscriptionHandler
	namespace string
	version   string
	op        string
}

func newInternalProtoSubscriptionDispatcher(src *internalProtoSource, md protoreflect.MethodDescriptor) *internalProtoSubscriptionDispatcher {
	methodName := string(md.Name())
	return &internalProtoSubscriptionDispatcher{
		handler:   src.subscriptionHandlers[methodName],
		namespace: src.namespace,
		version:   src.version,
		op:        methodName,
	}
}

// Dispatch satisfies ir.Dispatcher. Returns the broker channel as
// `any`; graphql-go pumps frames off it and feeds each as rp.Source.
func (d *internalProtoSubscriptionDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	if d.handler == nil {
		return nil, fmt.Errorf("gateway: no subscription handler registered for %s/%s/%s", d.namespace, d.version, d.op)
	}
	ctx = withDispatchOpInfo(ctx, d.namespace, d.version, d.op)
	return d.handler(ctx, args)
}

// Compile-time assertions.
var _ ir.Dispatcher = (*internalProtoDispatcher)(nil)
var _ ir.Dispatcher = (*internalProtoSubscriptionDispatcher)(nil)
