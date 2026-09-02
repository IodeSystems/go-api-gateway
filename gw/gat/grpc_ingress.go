package gat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// HandleMux is the minimal mux surface RegisterGRPC writes to. Stdlib
// *http.ServeMux satisfies it; chi/gorilla/etc. do too.
//
// Stability: experimental
type HandleMux interface {
	Handle(pattern string, handler http.Handler)
}

// RegisterGRPC mounts a connect-go handler per captured operation on
// mux, under prefix. Each operation becomes available at the
// canonical gRPC/Connect path:
//
//	{prefix}/{Service.FullName}/{MethodName}
//
// Wire-protocol coverage is what connect-go gives: clients can use
// grpc-go (HTTP/2 native), connect-go (Connect protocol), or
// grpc-web (browser-friendly) — all hit the same handlers.
//
// Dispatch round-trip: incoming proto (dynamic message) →
// a cached field plan (connect_bind.go), or the map walk in
// connect_args.go when the plan declines →
// captured huma handler → extracted Body → JSON → protojson →
// outgoing proto.
//
// Must be called AFTER RegisterHuma — RegisterGRPC reads the
// FileDescriptorSet projection of g.services, which only exists
// once the schema is built.
//
// Stability: experimental
func RegisterGRPC(mux HandleMux, g *Gateway, prefix string) error {
	if !g.built {
		return fmt.Errorf("gat: RegisterHuma must be called before RegisterGRPC")
	}
	fds, err := ir.RenderProtoFiles(g.services)
	if err != nil {
		return fmt.Errorf("gat: render proto files: %w", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return fmt.Errorf("gat: build files registry: %w", err)
	}

	capturedByName := make(map[string]*capturedOp, len(g.captured))
	for _, c := range g.captured {
		capturedByName[c.op.OperationID] = c
	}

	prefix = strings.TrimRight(prefix, "/")

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			for j := 0; j < sd.Methods().Len(); j++ {
				md := sd.Methods().Get(j)
				name := string(md.Name())
				cap, ok := capturedByName[name]
				if !ok {
					continue
				}
				procedure := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
				handler := connect.NewUnaryHandler(
					procedure,
					connectUnary(cap, md),
					connect.WithSchema(md),
					connect.WithRequestInitializer(newDynamicInitializer()),
					connect.WithCodec(fastProtoCodec{}),
				)
				mux.Handle(prefix+procedure, handler)
			}
		}
		return true
	})

	return nil
}

// newDynamicInitializer returns a connect.WithRequestInitializer that
// allocates a dynamicpb.Message of the right MethodDescriptor input
// (or output, on the client side) shape.
func newDynamicInitializer() func(connect.Spec, any) error {
	return func(spec connect.Spec, msg any) error {
		dyn, ok := msg.(*dynamicpb.Message)
		if !ok {
			return nil
		}
		md, ok := spec.Schema.(protoreflect.MethodDescriptor)
		if !ok {
			return fmt.Errorf("gat: spec.Schema is %T, want protoreflect.MethodDescriptor", spec.Schema)
		}
		if spec.IsClient {
			*dyn = *dynamicpb.NewMessage(md.Output())
		} else {
			*dyn = *dynamicpb.NewMessage(md.Input())
		}
		return nil
	}
}

// handlerErrorToConnect maps a captured-handler error to a
// connect.Error with a status-aware code.
//
// Huma's `huma.NewError(status, msg)` (and its 4xx helpers) returns a
// value whose dynamic type implements `interface { GetStatus() int }`
// — that's the StatusError shape. We duck-type rather than import the
// huma package so gat stays loose-coupled to its OpenAPI ingest path;
// any framework that exposes the same shape (e.g., a hand-rolled
// error type) gets the same treatment for free.
//
// Unrecognised errors stay CodeInternal — same as before this helper
// existed.
func handlerErrorToConnect(err error) *connect.Error {
	type statusError interface{ GetStatus() int }
	if se, ok := err.(statusError); ok {
		return connect.NewError(httpStatusToConnectCode(se.GetStatus()), err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

// httpStatusToConnectCode maps an HTTP status code to the Connect code
// the gRPC spec uses for that semantic. The mapping mirrors the
// table in https://connectrpc.com/docs/protocol#error-codes (inverse
// direction). Anything we can't classify falls back to CodeUnknown so
// the caller can still tell something went wrong without misleading
// them into a more specific failure mode.
func httpStatusToConnectCode(status int) connect.Code {
	switch status {
	case http.StatusBadRequest: // 400
		return connect.CodeInvalidArgument
	case http.StatusUnauthorized: // 401
		return connect.CodeUnauthenticated
	case http.StatusForbidden: // 403
		return connect.CodePermissionDenied
	case http.StatusNotFound: // 404
		return connect.CodeNotFound
	case http.StatusConflict: // 409
		return connect.CodeAlreadyExists
	case http.StatusGone: // 410
		return connect.CodeNotFound
	case http.StatusPreconditionFailed: // 412
		return connect.CodeFailedPrecondition
	case http.StatusRequestEntityTooLarge: // 413
		return connect.CodeResourceExhausted
	case http.StatusTooManyRequests: // 429
		return connect.CodeResourceExhausted
	case http.StatusNotImplemented: // 501
		return connect.CodeUnimplemented
	case http.StatusBadGateway, http.StatusGatewayTimeout: // 502, 504
		return connect.CodeUnavailable
	case http.StatusServiceUnavailable: // 503
		return connect.CodeUnavailable
	}
	if status >= 500 {
		return connect.CodeInternal
	}
	if status >= 400 {
		return connect.CodeInvalidArgument
	}
	return connect.CodeUnknown
}

// connectUnary builds the typed unary handler that bridges
// dynamicpb messages to the captured huma handler.
func connectUnary(cap *capturedOp, md protoreflect.MethodDescriptor) func(context.Context, *connect.Request[dynamicpb.Message]) (*connect.Response[wireResult], error) {
	// Resolve the body arg once, at mount time. It is a property of the
	// operation, not of the request, and scanning Args per call put
	// mount-time work in the hot loop.
	bodyArg := ""
	if cap.irOp != nil {
		for _, a := range cap.irOp.Args {
			if strings.EqualFold(a.OpenAPILocation, "body") {
				bodyArg = a.Name
				break
			}
		}
	}

	// Plan both directions against the operation's types. A nil plan is
	// the normal outcome for anything the walkers can't prove they
	// handle identically (custom marshalers, maps, enums); that
	// operation simply keeps the older path.
	inPlan := buildBindPlan(cap.inputType, cap.irOp, bodyArg, md.Input())
	outPlan := planResponse(cap, md)

	if outPlan != nil {
		// Ascending field order matches what proto.Marshal emits, which
		// is what the emitter is checked against.
		outPlan.sortFields()
	}

	return func(ctx context.Context, req *connect.Request[dynamicpb.Message]) (*connect.Response[wireResult], error) {
		inPtr := reflect.New(cap.inputType)
		in := inPtr.Elem()
		switch {
		case inPlan != nil:
			// Fast path: proto fields written straight into the input
			// struct through the cached field plan. No intermediate map,
			// and no protojson representations to undo.
			if err := inPlan.bind(in, req.Msg); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("bind %s: %w", cap.op.OperationID, err))
			}
		case cap.irOp != nil:
			// Walk the message into the args map, then bind by tag. This
			// used to go protojson.Marshal → json.Unmarshal;
			// messageToArgs reproduces that output exactly, pinned
			// against protojson in connect_args_test.go.
			args := messageToArgs(req.Msg)
			if err := bindInput(in, cap.irOp, args, bodyArg); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("bind %s: %w", cap.op.OperationID, err))
			}
		default:
			// Best-effort JSON-into-input for ops never ingested.
			raw, _ := json.Marshal(messageToArgs(req.Msg))
			_ = json.Unmarshal(raw, inPtr.Interface())
		}

		out, err := cap.invoke(ctx, inPtr.Interface())
		if err != nil {
			return nil, handlerErrorToConnect(err)
		}
		body := extractBody(out)

		if outPlan != nil {
			// Hand over the value, not a message: the codec encodes it.
			return connect.NewResponse(newPlannedResult(md.Output(), outPlan, body)), nil
		}

		respMsg := dynamicpb.NewMessage(md.Output())
		if err := jsonToDynamic(body, respMsg); err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("encode response: %w", err))
		}
		return connect.NewResponse(newBuiltResult(md.Output(), respMsg)), nil
	}
}

// jsonToDynamic encodes body as JSON and decodes that into the
// dynamic proto message. The round-trip relies on body being JSON-
// representable, which it always is for huma outputs.
func jsonToDynamic(body any, msg *dynamicpb.Message) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("protojson decode: %w", err)
	}
	return nil
}
