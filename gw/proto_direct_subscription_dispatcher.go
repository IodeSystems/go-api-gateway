package gateway

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// protoDirectSubscriptionDispatcher implements ir.Dispatcher for one
// server-streaming proto RPC using honest per-subscriber upstream
// dispatch. Each subscriber gets its own gRPC stream to a replica;
// frames from the upstream are forwarded as map[string]any events
// through a channel that graphql-go's executor pumps as WebSocket
// next-frames.
//
// Stream caps (pool.streamSem + gateway streamGlobalSem) gate the
// number of concurrent streams to bound FDs and memory. Unlike unary
// backpressure, subscriptions use a non-blocking try-acquire: a
// subscriber hitting the cap gets an immediate ResourceExhausted
// rather than queuing with a dwell timer. A client waiting for a
// lazy upstream message is not the same urgency as a req/rsp cycle —
// no queue-depth, no dwell metrics, no MaxWaitTime timeout.
// backpressureMiddleware is intentionally NOT wrapped around this
// dispatcher.
type protoDirectSubscriptionDispatcher struct {
	g          *Gateway
	pool       *pool
	sd         protoreflect.ServiceDescriptor
	md         protoreflect.MethodDescriptor
	inputDesc  protoreflect.MessageDescriptor
	outputDesc protoreflect.MessageDescriptor
}

func newProtoDirectSubscriptionDispatcher(g *Gateway, p *pool, sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor) *protoDirectSubscriptionDispatcher {
	return &protoDirectSubscriptionDispatcher{
		g:          g,
		pool:       p,
		sd:         sd,
		md:         md,
		inputDesc:  md.Input(),
		outputDesc: md.Output(),
	}
}

// Dispatch opens a direct gRPC server-streaming call to an upstream
// replica and returns a chan any of decoded event maps. The channel
// is closed when the upstream stream ends or the context cancels.
func (d *protoDirectSubscriptionDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	ns := d.pool.key.namespace
	ver := d.pool.key.version
	method := fmt.Sprintf("/%s/%s", d.sd.FullName(), d.md.Name())

	tr := tracerFromContext(ctx)
	ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.proto.subscription",
		namespaceAttr(ns),
		versionAttr(ver),
		methodAttr(string(d.md.Name())),
		grpcSystemAttr(),
		rpcMethodAttr(method),
	)
	// Subscription spans wrap the OPEN-side only — the stream lifetime
	// is unbounded; the span ends once SendMsg + CloseSend complete.
	// Per-event spans would balloon trace volume without payoff.
	defer span.End()

	// Try-acquire stream caps: non-blocking. Subscriptions don't
	// queue — hitting the cap is an immediate ResourceExhausted.
	acquiredGlobal := d.g.streamGlobalSem == nil
	if d.g.streamGlobalSem != nil {
		select {
		case d.g.streamGlobalSem <- struct{}{}:
			acquiredGlobal = true
		default:
			return nil, Reject(CodeResourceExhausted, "gateway stream cap reached")
		}
	}
	acquiredPool := d.pool.streamSem == nil
	if d.pool.streamSem != nil {
		select {
		case d.pool.streamSem <- struct{}{}:
			acquiredPool = true
		default:
			if acquiredGlobal {
				<-d.g.streamGlobalSem
			}
			return nil, Reject(CodeResourceExhausted, fmt.Sprintf("%s/%s: stream cap reached", ns, ver))
		}
	}

	rollbackSlots := func() {
		if acquiredPool {
			<-d.pool.streamSem
		}
		if acquiredGlobal {
			<-d.g.streamGlobalSem
		}
	}

	r := d.pool.pickReplica()
	if r == nil {
		rollbackSlots()
		return nil, fmt.Errorf("gateway: no live replicas for %s/%s", ns, ver)
	}

	// Build request message from args.
	req := acquireDynamicMessage(d.inputDesc)
	defer releaseDynamicMessage(d.inputDesc, req)
	if err := argsToMessage(args, req); err != nil {
		rollbackSlots()
		return nil, err
	}

	// Apply header injectors.
	headers := d.g.headerInjectorSnapshot()
	if len(headers) > 0 {
		injected, err := applyHeaderInjectors(ctx, headers)
		if err != nil {
			rollbackSlots()
			return nil, err
		}
		if len(injected) > 0 {
			kvs := make([]string, 0, 2*len(injected))
			for k, v := range injected {
				kvs = append(kvs, k, v)
			}
			ctx = metadata.AppendToOutgoingContext(ctx, kvs...)
		}
		releaseInjectorMap(injected)
	}

	// Open server-streaming call.
	streamDesc := &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: false,
	}
	ctx = bridgeTraceMetadata(ctx)
	ctx = tr.injectGRPC(ctx)
	stream, err := r.conn.NewStream(ctx, streamDesc, method)
	if err != nil {
		rollbackSlots()
		return nil, err
	}

	// Send request and close send side.
	if err := stream.SendMsg(req); err != nil {
		rollbackSlots()
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		rollbackSlots()
		return nil, err
	}

	// Track active streams.
	poolN := int(d.pool.streamInflight.Add(1))
	d.g.cfg.metrics.SetStreamsInflight(ns, ver, poolN)
	globalN := int(d.g.streamGlobal.Add(1))
	d.g.cfg.metrics.SetStreamsInflightTotal(globalN)

	// Pump upstream frames into a channel.
	ch := make(chan any, 32)
	go func() {
		<-ctx.Done()
		poolNow := int(d.pool.streamInflight.Add(-1))
		d.g.cfg.metrics.SetStreamsInflight(ns, ver, poolNow)
		globalNow := int(d.g.streamGlobal.Add(-1))
		d.g.cfg.metrics.SetStreamsInflightTotal(globalNow)
		rollbackSlots()
	}()
	go func() {
		defer close(ch)
		for {
			resp := acquireDynamicMessage(d.outputDesc)
			err := stream.RecvMsg(resp)
			if err != nil {
				releaseDynamicMessage(d.outputDesc, resp)
				return
			}
			payload := messageToMap(resp)
			releaseDynamicMessage(d.outputDesc, resp)
			select {
			case ch <- payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

var _ ir.Dispatcher = (*protoDirectSubscriptionDispatcher)(nil)
