package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// grpcIngressRoute is one (METHOD URL) → handler entry. Distinct
// from ingressRoute (HTTP/JSON) because gRPC ingress skips the
// canonical-args round trip on unary calls — wire bytes deserialize
// into a dynamicpb.Message of the input descriptor and feed straight
// into the chained Handler the canonical protoDispatcher uses.
//
// Streaming routes go through the canonical-args ir.Dispatcher
// (protoDirectSubscriptionDispatcher) which opens a direct gRPC
// server-streaming call to the upstream per subscriber.
//
// Built once per assembleLocked from the live pool set and discarded
// on rebuild. Atomic-swap so the unknown-service handler doesn't
// take g.mu on the hot path.
type grpcIngressRoute struct {
	pool       *pool
	inputDesc  protoreflect.MessageDescriptor
	outputDesc protoreflect.MessageDescriptor
	streaming  bool

	// Unary path: chain(inner) with the matching backpressure config.
	handler Handler
	bp      backpressureConfig

	// Streaming path: ir.Dispatcher whose Dispatch returns a chan any
	// of decoded event maps from the upstream gRPC stream.
	streamDispatcher ir.Dispatcher
}

type grpcIngressTable struct {
	routes map[string]*grpcIngressRoute // key: "/<svcFullName>/<methodName>"
}

// rebuildGRPCIngressLocked walks every proto pool's RPCs and emits
// one route per method. Unary routes capture the same chained
// Handler the canonical protoDispatcher uses (so transforms apply
// identically) alongside the per-pool backpressureConfig. Server-
// streaming routes capture the protoDirectSubscriptionDispatcher
// already registered in g.dispatchers — it opens a direct gRPC
// stream to the upstream per subscriber.
//
// Bidi / client-streaming methods are skipped — egress doesn't
// support them and there's no canonical args shape to forward.
// Internal namespaces (`_*` or AsInternal) skip just like the
// GraphQL surface skips them.
//
// After the proto walk, a second pass synthesizes routes for
// non-proto slots (slotKindOpenAPI, slotKindGraphQL) from
// `ir.RenderProtoFiles` so a gRPC client can reach OpenAPI- or
// GraphQL-registered ops too. Synthesized routes go through the
// canonical-args dispatcher (slower than the proto-on-proto fast
// path; fine for cross-kind which won't be hot).
//
// Caller holds g.mu.
func (g *Gateway) rebuildGRPCIngressLocked() {
	chain := g.runtimeChain()
	headers := g.headerInjectorSnapshot()
	metrics := g.cfg.metrics
	bpOpts := g.cfg.backpressure
	t := &grpcIngressTable{routes: map[string]*grpcIngressRoute{}}
	for _, slot := range g.slots {
		switch slot.kind {
		case slotKindProto:
			p := slot.proto
			if g.isInternal(p.key.namespace) {
				continue
			}
			services := p.file.Services()
			for i := 0; i < services.Len(); i++ {
				sd := services.Get(i)
				methods := sd.Methods()
				for j := 0; j < methods.Len(); j++ {
					md := methods.Get(j)
					if md.IsStreamingClient() {
						// bidi/client-streaming — egress can't synthesise
						// these and ingress has no canonical-args shape.
						continue
					}
					path := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
					if md.IsStreamingServer() {
						sid := ir.MakeSchemaID(p.key.namespace, p.key.version, string(md.Name()))
						sd := g.dispatchers.Get(sid)
						if sd == nil {
							continue
						}
						t.routes[path] = &grpcIngressRoute{
							pool:             p,
							inputDesc:        md.Input(),
							outputDesc:       md.Output(),
							streaming:        true,
							streamDispatcher: sd,
						}
						continue
					}
					inner := newProtoInvocationHandler(p, sd, md, headers, metrics, bpOpts)
					label := methodLabel(sd, md)
					t.routes[path] = &grpcIngressRoute{
						pool:       p,
						inputDesc:  md.Input(),
						outputDesc: md.Output(),
						handler:    chain(inner),
						bp:         poolBackpressureConfig(p, label, metrics, bpOpts),
					}
				}
			}
		case slotKindInternalProto:
			src := slot.internalProto
			if g.isInternal(slot.key.namespace) {
				continue
			}
			services := src.file.Services()
			for i := 0; i < services.Len(); i++ {
				sd := services.Get(i)
				methods := sd.Methods()
				for j := 0; j < methods.Len(); j++ {
					md := methods.Get(j)
					if md.IsStreamingClient() {
						continue
					}
					path := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
					if md.IsStreamingServer() {
						// Subscription path is broker-driven (see
						// registerProtoDispatchersLocked); gRPC
						// ingress for in-process subscriptions
						// lands when the PubSub handler does.
						continue
					}
					inner := newInternalProtoInvocationHandler(src, sd, md, metrics)
					// No pool / backpressure for in-process dispatch —
					// route.bp left zero (acquireBackpressureSlot
					// short-circuits on a nil Sem).
					t.routes[path] = &grpcIngressRoute{
						inputDesc:  md.Input(),
						outputDesc: md.Output(),
						handler:    chain(inner),
					}
				}
			}
		}
	}

	// Cross-kind second pass: synthesize routes for openapi /
	// graphql slots via ir.RenderProtoFiles. The dispatcher
	// already includes backpressureMiddleware (see
	// openapi_register / graphql_register) so the route's bp
	// config is intentionally left zero — acquireBackpressureSlot
	// short-circuits on a nil Sem.
	for k, slot := range g.slots {
		if slot.kind == slotKindProto || slot.kind == slotKindInternalProto {
			continue
		}
		if g.isInternal(k.namespace) {
			continue
		}
		g.addCanonicalArgsGRPCRoutesLocked(t, k, slot.ir)
	}

	g.grpcIngressRoutes.Store(t)
}

// addCanonicalArgsGRPCRoutesLocked synthesizes input/output
// MessageDescriptors per non-proto service via ir.RenderProtoFiles
// and emits routes that bridge dynamicpb ↔ canonical-args
// dispatcher: messageToMap on the input → ir.Dispatcher.Dispatch
// → argsToMessage onto the synthesized response message (which has
// shape `{value: <output>}` per ir.synthResponseMessage). Streaming
// methods are skipped — graphql subscriptions and openapi
// long-poll-as-stream don't have a stable canonical-args bridge
// from the gRPC side yet. Caller holds g.mu.
func (g *Gateway) addCanonicalArgsGRPCRoutesLocked(t *grpcIngressTable, key poolKey, services []*ir.Service) {
	for _, svc := range services {
		fds, err := ir.RenderProtoFiles([]*ir.Service{svc})
		if err != nil || fds == nil || len(fds.File) == 0 {
			continue
		}
		files, err := protodesc.NewFiles(fds)
		if err != nil {
			continue
		}
		for _, fp := range fds.File {
			fd, err := files.FindFileByPath(fp.GetName())
			if err != nil {
				continue
			}
			ss := fd.Services()
			for i := 0; i < ss.Len(); i++ {
				sd := ss.Get(i)
				methods := sd.Methods()
				for j := 0; j < methods.Len(); j++ {
					md := methods.Get(j)
					if md.IsStreamingClient() || md.IsStreamingServer() {
						continue
					}
					sid := ir.MakeSchemaID(key.namespace, key.version, string(md.Name()))
					disp := g.dispatchers.Get(sid)
					if disp == nil {
						continue
					}
					path := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
					if _, dup := t.routes[path]; dup {
						continue
					}
					t.routes[path] = &grpcIngressRoute{
						inputDesc:  md.Input(),
						outputDesc: md.Output(),
						handler:    newCanonicalArgsGRPCHandler(disp, md.Output()),
					}
				}
			}
		}
	}
}

// newCanonicalArgsGRPCHandler builds a Handler that bridges
// dynamicpb ↔ canonical-args ir.Dispatcher: the input dynamicpb
// converts to args via messageToMap, the dispatcher returns a
// scalar / map / list / nil, and the result is either passed
// through to argsToMessage (when outputDesc is the user-facing
// response shape — non-repeated Named output) or wrapped as
// `{value: <result>}` (when outputDesc is the synthesised wrapper
// emitted by ir.synthResponseMessage for primitive / repeated /
// nil-output ops).
//
// The wrap-or-pass-through decision is a structural property of
// outputDesc: a wrapper always has exactly one field named "value"
// (cf. ir.synthResponseMessage). That signal's robust against
// user-facing types that happen to have a "value" field — they'd
// be a Named ref into svc.Types and the renderer would point the
// method at them directly instead of synthesising a wrapper.
//
// The response message comes from acquireDynamicMessage; the gRPC
// ingress serve path releases it after stream.SendMsg returns.
func newCanonicalArgsGRPCHandler(disp ir.Dispatcher, outputDesc protoreflect.MessageDescriptor) Handler {
	wrapInValue := isSynthResponseWrapper(outputDesc)
	return Handler(func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		var args map[string]any
		if reqMsg, ok := req.(*dynamicpb.Message); ok && reqMsg != nil {
			args = messageToMap(reqMsg)
		} else {
			args = map[string]any{}
		}
		result, err := disp.Dispatch(ctx, args)
		if err != nil {
			return nil, err
		}
		respMsg := acquireDynamicMessage(outputDesc)
		if result == nil {
			return respMsg, nil
		}
		var payload map[string]any
		if wrapInValue {
			payload = map[string]any{"value": coerceCanonicalListValue(result)}
		} else if m, ok := result.(map[string]any); ok {
			payload = m
		} else {
			// Non-wrapper outputDesc but the dispatcher returned
			// something that isn't a map — refuse to lose the body.
			// argsToMessage doesn't carry a primitive-only path.
			releaseDynamicMessage(outputDesc, respMsg)
			return nil, fmt.Errorf("ingress: cannot project %T into %s", result, outputDesc.FullName())
		}
		if err := argsToMessage(payload, respMsg); err != nil {
			releaseDynamicMessage(outputDesc, respMsg)
			return nil, err
		}
		return respMsg, nil
	})
}

// isSynthResponseWrapper reports whether md has the structural
// shape ir.synthResponseMessage emits: exactly one field named
// "value". Used by the canonical-args gRPC handler to decide
// whether to wrap a dispatcher's return value.
func isSynthResponseWrapper(md protoreflect.MessageDescriptor) bool {
	if md == nil || md.Fields().Len() != 1 {
		return false
	}
	f := md.Fields().Get(0)
	return string(f.Name()) == "value"
}

// coerceCanonicalListValue normalises a dispatcher's return value
// to the JSON-decoded shape argsToMessage expects: lists must be
// []any. Both openapi and graphql dispatchers go through
// json.Unmarshal into `any` so the common case is already []any /
// map[string]any / scalar — but []map[string]any (e.g. a hand-built
// response in tests) shows up too. Unknown slice element types fall
// through unchanged so argsToMessage's per-field type check
// surfaces the mismatch.
func coerceCanonicalListValue(v any) any {
	if items, ok := v.([]map[string]any); ok {
		out := make([]any, len(items))
		for i, m := range items {
			out[i] = m
		}
		return out
	}
	return v
}

// GRPCUnknownHandler returns a grpc.StreamHandler the operator wires
// onto a *grpc.Server via grpc.UnknownServiceHandler — typically the
// same server that hosts ControlPlane:
//
//	srv := grpc.NewServer(grpc.UnknownServiceHandler(gw.GRPCUnknownHandler()))
//	cpv1.RegisterControlPlaneServer(srv, gw.ControlPlane())
//
// Unary RPCs decode wire bytes into a dynamicpb.Message of the target
// method's input descriptor, apply per-pool backpressure, and run the
// chained Handler the canonical protoDispatcher uses (so InjectType /
// HideType / user runtime middleware all apply). Skipping canonical
// args avoids two map↔message conversions on the proto→proto path.
//
// Server-streaming RPCs route through the canonical-args
// protoDirectSubscriptionDispatcher: args are converted to a
// dynamicpb request message and sent to the upstream via a direct
// gRPC server-streaming call. Each delivered event re-encodes to
// dynamicpb on the wire.
//
// Bidi / client-streaming RPCs return Unimplemented. Unmatched
// methods return Unimplemented.
//
// Safe to call before any service is registered; the handler reads
// the route table at dispatch time so registrations after the
// listener is up are visible immediately.
//
// Stability: stable
func (g *Gateway) GRPCUnknownHandler() grpc.StreamHandler {
	g.mu.Lock()
	if g.schema.Load() == nil {
		if err := g.assembleLocked(); err != nil {
			g.mu.Unlock()
			return func(any, grpc.ServerStream) error {
				return status.Errorf(codes.Internal, "ingress: assemble: %v", err)
			}
		}
	}
	g.mu.Unlock()
	return g.serveGRPCUnknown
}

func (g *Gateway) serveGRPCUnknown(_ any, stream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "ingress: missing method on stream")
	}
	if g.draining.Load() {
		return status.Error(codes.Unavailable, "ingress: draining")
	}
	t := g.grpcIngressRoutes.Load()
	if t == nil {
		return status.Errorf(codes.Unimplemented, "ingress: no routes (gateway not assembled)")
	}
	route, ok := t.routes[method]
	if !ok {
		return status.Errorf(codes.Unimplemented, "ingress: no route for %s", method)
	}

	ns, ver := "", ""
	if route.pool != nil {
		ns, ver = route.pool.key.namespace, route.pool.key.version
	}

	if route.streaming {
		// Subscription lifetime is open-ended; skip request_*_seconds.
		// Trace context is extracted + a span opened so the ingress
		// timing lands in the trace; child spans on the dispatcher
		// path land under the ingress span via the per-dispatch wiring.
		ctx := g.tracer.extractGRPC(stream.Context())
		ctx, span := g.tracer.startIngressSpan(ctx, "gateway.grpc.subscription",
			ingressAttr("grpc"),
			namespaceAttr(ns),
			versionAttr(ver),
			methodAttr(method),
			grpcSystemAttr(),
			rpcMethodAttr(method),
		)
		defer span.End()
		ctx = withTracer(ctx, g.tracer)
		return g.serveGRPCStreamingUnknown(ctx, stream, route)
	}

	req := acquireDynamicMessage(route.inputDesc)
	defer releaseDynamicMessage(route.inputDesc, req)
	if err := stream.RecvMsg(req); err != nil {
		// EOF / cancellation propagate as-is so client sees the right
		// status; everything else maps to InvalidArgument since the
		// request bytes failed to decode against the descriptor.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return status.Errorf(codes.InvalidArgument, "ingress: decode request: %v", err)
	}

	ctx := g.tracer.extractGRPC(stream.Context())
	ctx, span := g.tracer.startIngressSpan(ctx, "gateway.grpc",
		ingressAttr("grpc"),
		namespaceAttr(ns),
		versionAttr(ver),
		methodAttr(method),
		grpcSystemAttr(),
		rpcMethodAttr(method),
	)
	defer span.End()
	ctx = withTracer(ctx, g.tracer)
	ctx, accum := withDispatchAccumulator(ctx)
	ctx = withInjectCache(ctx)
	start := time.Now()
	defer func() {
		total := time.Since(start)
		dispatchSum := time.Duration(accum.Sum.Load())
		g.cfg.metrics.RecordRequest("grpc", total, total-dispatchSum)
		g.logRequestLine("grpc", method, total, dispatchSum, int(accum.Count.Load()))
	}()

	release, err := acquireBackpressureSlot(ctx, route.bp)
	if err != nil {
		return ingressGRPCStatus(err)
	}
	defer release()

	resp, err := route.handler(ctx, req)
	if err != nil {
		return ingressGRPCStatus(err)
	}
	respMsg := resp.(*dynamicpb.Message)
	sendErr := stream.SendMsg(respMsg)
	releaseDynamicMessage(route.outputDesc, respMsg)
	return sendErr
}

// gRPC metadata keys the streaming ingress consults for subscription
// auth handles. Lowercase per gRPC convention; "x-gateway-" prefixed
// to avoid colliding with anything the upstream service might forward.
const (
	mdSubscribeHMAC      = "x-gateway-hmac"
	mdSubscribeTimestamp = "x-gateway-timestamp"
	mdSubscribeKid       = "x-gateway-kid"
)

func (g *Gateway) serveGRPCStreamingUnknown(ctx context.Context, stream grpc.ServerStream, route *grpcIngressRoute) error {
	req := acquireDynamicMessage(route.inputDesc)
	defer releaseDynamicMessage(route.inputDesc, req)
	if err := stream.RecvMsg(req); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return status.Errorf(codes.InvalidArgument, "ingress: decode request: %v", err)
	}

	args := messageToMap(req)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if v := md.Get(mdSubscribeHMAC); len(v) > 0 {
			args["hmac"] = v[0]
		}
		if v := md.Get(mdSubscribeTimestamp); len(v) > 0 {
			args["timestamp"] = v[0]
		}
		if v := md.Get(mdSubscribeKid); len(v) > 0 {
			args["kid"] = v[0]
		}
	}

	ctx = withInjectCache(ctx)

	out, err := route.streamDispatcher.Dispatch(ctx, args)
	if err != nil {
		return ingressGRPCStatus(err)
	}
	ch, ok := out.(chan any)
	if !ok {
		return status.Error(codes.Internal, "ingress: subscription dispatcher returned non-channel")
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, open := <-ch:
			if !open {
				return nil
			}
			evMap, ok := ev.(map[string]any)
			if !ok {
				continue
			}
			outMsg := acquireDynamicMessage(route.outputDesc)
			if err := argsToMessage(evMap, outMsg); err != nil {
				releaseDynamicMessage(route.outputDesc, outMsg)
				continue
			}
			err := stream.SendMsg(outMsg)
			releaseDynamicMessage(route.outputDesc, outMsg)
			if err != nil {
				return err
			}
		}
	}
}

// acquireBackpressureSlot is the slot-acquisition prologue shared by
// gRPC ingress and the canonical-args backpressureMiddleware.
// Returns a release func (always non-nil when err == nil; defer it
// to release the slot) and a Reject when the wait budget expires.
//
// Safe to call with cfg.Sem == nil — returns a no-op release and no
// error. Two dispatch paths through one helper means they can't
// drift on backpressure semantics.
func acquireBackpressureSlot(ctx context.Context, cfg backpressureConfig) (release func(), err error) {
	if cfg.Sem == nil {
		return func() {}, nil
	}
	waitStart := time.Now()
	select {
	case cfg.Sem <- struct{}{}:
		cfg.Metrics.RecordDwell(cfg.Namespace, cfg.Version, cfg.Label, cfg.Kind, time.Since(waitStart))
	default:
		depth := int(cfg.Queueing.Add(1))
		setQueueDepthForCfg(cfg, depth)
		dwell, werr := waitForSlot(ctx, cfg.Sem, cfg.MaxWaitTime)
		now := int(cfg.Queueing.Add(-1))
		setQueueDepthForCfg(cfg, now)
		cfg.Metrics.RecordDwell(cfg.Namespace, cfg.Version, cfg.Label, cfg.Kind, dwell)
		if werr != nil {
			cfg.Metrics.RecordBackoff(cfg.Namespace, cfg.Version, cfg.Label, cfg.Kind, "wait_timeout")
			return nil, Reject(CodeResourceExhausted, fmt.Sprintf("%s/%s: %s", cfg.Namespace, cfg.Version, werr.Error()))
		}
	}
	return func() { <-cfg.Sem }, nil
}

// ingressGRPCStatus maps an error from the ingress dispatch path
// onto the gRPC status code clients expect. Reject errors carry
// the gateway Code enum; other errors fall through to Internal.
//
// gRPC status errors from the upstream Invoke are pass-through (the
// gateway doesn't reclassify upstream codes).
func ingressGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		// Upstream gRPC error already classified — pass through.
		return err
	}
	var rej *rejection
	if errors.As(err, &rej) {
		return status.Error(codeToGRPC(rej.Code), rej.Msg)
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// codeToGRPC is the inverse of httpStatusToCode / codeToHTTPStatus —
// classifies a gateway Code onto its gRPC status equivalent.
func codeToGRPC(c Code) codes.Code {
	switch c {
	case CodeUnauthenticated:
		return codes.Unauthenticated
	case CodePermissionDenied:
		return codes.PermissionDenied
	case CodeResourceExhausted:
		return codes.ResourceExhausted
	case CodeInvalidArgument:
		return codes.InvalidArgument
	case CodeNotFound:
		return codes.NotFound
	}
	return codes.Internal
}
