package gateway

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/IodeSystems/graphql-go/v2/language/ast"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/gw/ir"
)

// protoDispatcher implements ir.Dispatcher for one proto-pool RPC.
// It owns the marshal (canonical args → dynamicpb request) /
// unmarshal (dynamicpb response → map) bridge and runs the pool's
// pre-built proto-shaped Handler chain (user runtime middleware +
// inner pickReplica/Invoke). backpressureMiddleware wraps the
// outside; the backpressure prologue is no longer inline here.
//
// Build once per (pool, RPC) at schema build time and reuse for
// every dispatch — the inner Handler chain is captured once.
type protoDispatcher struct {
	inputDesc protoreflect.MessageDescriptor
	handler   Handler // chain(inner): user runtime middleware around pickReplica+Invoke+RecordDispatch
	namespace string
	version   string
	op        string

	// uploadStore + uploadLimit snapshot g.cfg at schema-build time so
	// per-dispatch *Upload args bound to bytes fields marked
	// `(gwag.upload.v1.upload) = true` materialise here without
	// reaching back into the gateway. Nil store + a tus-staged value
	// surfaces as a configuration error from (*Upload).Open. Zero
	// uploadLimit = no cap (inline is already capped at the ingress;
	// tus is capped at the PATCH path).
	uploadStore UploadStore
	uploadLimit int64
}

// newProtoInvocationHandler returns the proto-native inner Handler
// for one (pool, RPC): pickReplica, increment inflight, gRPC Invoke,
// RecordDispatch. Shared by the canonical-args protoDispatcher and
// the proto-native gRPC ingress path — both want middleware wrapped
// around the same inner.
//
// The returned Handler is uncomposed: callers wrap it with the
// runtime middleware chain themselves. Backpressure is the outer
// layer in both call sites.
//
// Response message comes from the per-descriptor pool. The caller
// is responsible for calling releaseDynamicMessage on it once
// they've read out the contents (e.g. messageToMap) or written them
// to the wire (stream.SendMsg). On Invoke error the handler returns
// the message to the pool itself — `nil, err` doesn't dangle a
// pooled allocation.
func newProtoInvocationHandler(p *pool, sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor, headers []headerInjector, metrics Metrics, bpOpts BackpressureOptions) Handler {
	method := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
	outputDesc := md.Output()
	ns, ver := p.key.namespace, p.key.version
	return Handler(func(ctx context.Context, req protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
		tr := tracerFromContext(ctx)
		ctx, span := tr.startDispatchSpan(ctx, "gateway.dispatch.proto",
			namespaceAttr(ns),
			versionAttr(ver),
			methodAttr(string(md.Name())),
			grpcSystemAttr(),
			rpcMethodAttr(method),
		)
		defer span.End()
		start := time.Now()
		r := p.pickReplica()
		if r == nil {
			err := fmt.Errorf("gateway: no live replicas for %s/%s", ns, ver)
			elapsed := time.Since(start)
			metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
			addDispatchTime(ctx, elapsed)
			return nil, err
		}
		// Per-instance cap: acquired AFTER pickReplica because the
		// per-instance sem is keyed on a specific replica. Service-
		// level backpressure already gated entry above (in
		// backpressureMiddleware on the canonical-args path; in the
		// gRPC ingress prologue otherwise).
		releaseInstance, err := acquireReplicaSlot(ctx, r, p, method, metrics, bpOpts)
		if err != nil {
			elapsed := time.Since(start)
			metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
			addDispatchTime(ctx, elapsed)
			return nil, err
		}
		defer releaseInstance()

		if len(headers) > 0 {
			injected, err := applyHeaderInjectors(ctx, headers)
			if err != nil {
				elapsed := time.Since(start)
				metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
				addDispatchTime(ctx, elapsed)
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

		r.inflight.Add(1)
		defer r.inflight.Add(-1)
		ctx = bridgeTraceMetadata(ctx)
		ctx = tr.injectGRPC(ctx)
		resp := acquireDynamicMessage(outputDesc)
		err = r.conn.Invoke(ctx, method, req, resp)
		elapsed := time.Since(start)
		metrics.RecordDispatch(ctx, ns, ver, method, elapsed, err)
		addDispatchTime(ctx, elapsed)
		if err != nil {
			releaseDynamicMessage(outputDesc, resp)
			return nil, err
		}
		return resp, nil
	})
}

// newProtoDispatcher wraps newProtoInvocationHandler with the user
// runtime middleware chain and adapts to the canonical-args
// ir.Dispatcher interface used by GraphQL / HTTP/JSON ingress.
//
// RecordDispatch fires from the inner Handler with the per-Invoke
// duration. Pre-cutover the same metric covered queue+invoke wall
// time; with backpressureMiddleware now the outer layer the dwell
// metric carries the queue portion separately. No test asserts on
// the prior shape.
func newProtoDispatcher(p *pool, sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor, chain Middleware, headers []headerInjector, metrics Metrics, bpOpts BackpressureOptions, uploadStore UploadStore, uploadLimit int64) *protoDispatcher {
	inner := newProtoInvocationHandler(p, sd, md, headers, metrics, bpOpts)
	return &protoDispatcher{
		inputDesc:   md.Input(),
		handler:     chain(inner),
		namespace:   p.key.namespace,
		version:     p.key.version,
		op:          string(md.Name()),
		uploadStore: uploadStore,
		uploadLimit: uploadLimit,
	}
}

// Dispatch satisfies ir.Dispatcher. Marshals canonical args into a
// dynamicpb request, runs the captured Handler chain, unmarshals
// the response back to a canonical map.
//
// Both request and response messages come from
// acquireDynamicMessage; they're released after the response has
// been turned into the canonical map (gRPC has already finished
// marshaling by then, so the buffer is safe to reuse).
func (d *protoDispatcher) Dispatch(ctx context.Context, args map[string]any) (any, error) {
	ctx = withDispatchOpInfo(ctx, d.namespace, d.version, d.op)
	closeUploadReaders, err := d.materializeUploadArgs(ctx, args)
	if closeUploadReaders != nil {
		defer closeUploadReaders()
	}
	if err != nil {
		return nil, err
	}
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

// DispatchAppend is the byte-splice variant of Dispatch. Same proto
// invocation + Handler chain; instead of messageToMap → map[string]any
// (and graphql-go's per-field resolver tree projecting against that
// map), appendProtoMessage walks the dynamicpb response in lockstep
// with the local selection AST and writes JSON straight to dst with
// proto snake_case → lowerCamel renaming applied via the cached
// fieldInfo lookup.
//
// Per-request alloc drops to roughly the depth of the response tree
// (one slice grow per JSON nesting layer) instead of one map per
// nested message that messageToMap allocates. This is the bulk of
// the projected wedge for proto-origin services.
func (d *protoDispatcher) DispatchAppend(ctx context.Context, args map[string]any, dst []byte) ([]byte, error) {
	ctx = withDispatchOpInfo(ctx, d.namespace, d.version, d.op)
	closeUploadReaders, err := d.materializeUploadArgs(ctx, args)
	if closeUploadReaders != nil {
		defer closeUploadReaders()
	}
	if err != nil {
		return dst, err
	}
	req := acquireDynamicMessage(d.inputDesc)
	defer releaseDynamicMessage(d.inputDesc, req)
	if err := argsToMessage(args, req); err != nil {
		return dst, err
	}
	resp, err := d.handler(ctx, req)
	if err != nil {
		return dst, err
	}
	respMsg := resp.(*dynamicpb.Message)
	defer releaseDynamicMessage(respMsg.Descriptor(), respMsg)

	info := graphQLForwardInfoFrom(ctx)
	var sel *ast.SelectionSet
	if info != nil && len(info.FieldASTs) > 0 {
		sel = info.FieldASTs[0].SelectionSet
	}
	return appendProtoMessage(dst, respMsg, sel), nil
}

var _ ir.AppendDispatcher = (*protoDispatcher)(nil)

// methodLabel returns the proto wire path for an RPC, used as the
// metric label and the backpressureMiddleware Label slot. Mirrors
// what the inner Handler computes for RecordDispatch so the two
// metrics share their per-method label space.
func methodLabel(sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor) string {
	return fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
}

// poolBackpressureConfig bundles a pool's per-dispatch knobs into a
// backpressureConfig once per (pool, RPC). Callers pass it to
// backpressureMiddleware to wrap the protoDispatcher.
func poolBackpressureConfig(p *pool, label string, metrics Metrics, bp BackpressureOptions) backpressureConfig {
	return backpressureConfig{
		Sem:         p.sem,
		Queueing:    &p.queueing,
		MaxWaitTime: bp.MaxWaitTime,
		Metrics:     metrics,
		Namespace:   p.key.namespace,
		Version:     p.key.version,
		Label:       label,
		Kind:        "unary",
	}
}

// acquireReplicaSlot is the per-instance counterpart to
// acquireBackpressureSlot — acquires the replica's own sem, with the
// same MaxWaitTime budget and queue-depth tracking. The pool's
// MaxConcurrencyPerInstance setting drives the sem size; nil sem
// (unbounded per replica) returns a no-op release.
//
// Metrics use a separate "unary_instance" Kind so per-replica queue
// depth doesn't blur into the service-level "unary" counter.
func acquireReplicaSlot(ctx context.Context, r *replica, p *pool, label string, metrics Metrics, bp BackpressureOptions) (release func(), err error) {
	if r.sem == nil {
		return func() {}, nil
	}
	cfg := backpressureConfig{
		Sem:         r.sem,
		Queueing:    &r.queueing,
		MaxWaitTime: bp.MaxWaitTime,
		Metrics:     metrics,
		Namespace:   p.key.namespace,
		Version:     p.key.version,
		Label:       label,
		Kind:        "unary_instance",
		Replica:     r.addr,
	}
	return acquireBackpressureSlot(ctx, cfg)
}

// Compile-time assertion: protoDispatcher implements ir.Dispatcher.
var _ ir.Dispatcher = (*protoDispatcher)(nil)

// materializeUploadArgs walks d.inputDesc looking for bytes fields
// marked `(gwag.upload.v1.upload) = true` and replaces any *Upload
// values in `args` with the upload's body bytes (capped at
// d.uploadLimit). Recurses into message-typed args so nested upload
// fields work. Returns a closer that releases every reader the
// helper opened — caller defers it regardless of error.
//
// Args without an Upload-flagged target field, args whose value
// isn't *Upload, and Upload-flagged fields with no corresponding arg
// entry are all no-ops. Read errors and limit overruns surface as
// dispatch errors so the caller can return them to the client.
func (d *protoDispatcher) materializeUploadArgs(ctx context.Context, args map[string]any) (func(), error) {
	if len(args) == 0 {
		return nil, nil
	}
	var openers []io.Closer
	closer := func() {
		for _, r := range openers {
			_ = r.Close()
		}
	}
	if err := materializeUploadFieldsInto(ctx, args, d.inputDesc, d.uploadStore, d.uploadLimit, &openers); err != nil {
		return closer, err
	}
	if len(openers) == 0 {
		return nil, nil
	}
	return closer, nil
}

// materializeUploadFieldsInto is the recursive worker. Walks every
// field on `desc`; for each bytes field marked with the upload
// extension, replaces a matching *Upload arg with its body bytes;
// for message fields, recurses into the nested map. Filename /
// ContentType / Size on the *Upload are not threaded into the proto
// message — those would need their own fields on the user's proto;
// the binding is intentionally minimal.
func materializeUploadFieldsInto(ctx context.Context, args map[string]any, desc protoreflect.MessageDescriptor, store UploadStore, limit int64, openers *[]io.Closer) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		key := lowerCamel(string(fd.Name()))
		v, ok := args[key]
		if !ok {
			continue
		}
		switch fd.Kind() {
		case protoreflect.BytesKind:
			if !ir.IsUploadField(fd) {
				continue
			}
			up, ok := v.(*Upload)
			if !ok {
				continue
			}
			body, err := readUploadBytes(ctx, up, store, limit, openers)
			if err != nil {
				return fmt.Errorf("upload arg %q: %w", key, err)
			}
			args[key] = body
		case protoreflect.MessageKind, protoreflect.GroupKind:
			nested, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if err := materializeUploadFieldsInto(ctx, nested, fd.Message(), store, limit, openers); err != nil {
				return err
			}
		}
	}
	return nil
}

// readUploadBytes opens the upload, caps the read at `limit` (0 =
// no cap), and returns the body bytes. The opened reader is added
// to `openers` so the caller's deferred close releases it; reading
// to EOF doesn't relieve the close obligation (tus-staged readers
// hold a file handle).
func readUploadBytes(ctx context.Context, up *Upload, store UploadStore, limit int64, openers *[]io.Closer) ([]byte, error) {
	rc, err := up.Open(ctx, store)
	if err != nil {
		return nil, err
	}
	*openers = append(*openers, rc)
	var src io.Reader = rc
	if limit > 0 {
		// Read one extra byte so a body that exactly fills the cap
		// succeeds while one that overruns surfaces as an error.
		src = io.LimitReader(rc, limit+1)
	}
	body, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, fmt.Errorf("upload exceeds WithUploadLimit (%d bytes)", limit)
	}
	return body, nil
}
