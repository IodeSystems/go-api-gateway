package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/bench/cmd/traffic/runner"
)

// runGRPC parses grpc-adapter flags, fetches the gateway-rendered FDS
// for --service to find the requested method's input/output
// descriptors, dials the gateway gRPC ingress port, and fires unary
// calls via grpc.Invoke. JSON --args decodes into a dynamicpb.Message.
//
// --target is the gateway HTTP base URL (used for FDS fetch + metrics
// snapshot); --grpc-target is host:port for the gateway gRPC ingress.
func runGRPC(args []string) error {
	fs := flag.NewFlagSet("grpc", flag.ExitOnError)
	rps := fs.Int("rps", 100, "requests per second per target")
	duration := fs.Duration("duration", 30*time.Second, "test duration")
	concurrency := fs.Int("concurrency", 0, "max concurrent in-flight per target (extras are dropped); 0 = auto = max(64, rps/20)")
	shards := fs.Int("shards", 0, "driver goroutines per target; 0 = auto = ceil(rps/1500)")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request gRPC timeout")
	serverSide := fs.Bool("server-metrics", true, "snapshot gateway metrics before+after for the per-backend table")
	metricsPath := fs.String("metrics-path", "/api/metrics", "path the gateway exposes Prometheus metrics at; use /metrics for a raw gateway, empty to skip server-side capture")
	jsonOut := fs.String("json", "", "write the gateway-pass summary to PATH as JSON; '-' for stdout")
	service := fs.String("service", "", "registered namespace (e.g. greeter or greeter:v1); required")
	method := fs.String("method", "", "RPC method name within the service; required")
	argsJSON := fs.String("args", "{}", "JSON object decoded into the request message")
	var targetsRaw runner.StringFlag
	fs.Var(&targetsRaw, "target", "gateway HTTP base URL (used for FDS + metrics; repeat or comma-separate)")
	var grpcTargetsRaw runner.StringFlag
	fs.Var(&grpcTargetsRaw, "grpc-target", "gateway gRPC ingress host:port; one per --target in the same order, or single if shared")
	var directTargetsRaw runner.StringFlag
	fs.Var(&directTargetsRaw, "direct", "upstream service host:port to dial directly (bypassing the gateway). When set, runs a second pass after the gateway pass and prints a side-by-side compare. Repeat or comma-separate for multiple direct targets.")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *service == "" {
		return errors.New("--service is required")
	}
	if *method == "" {
		return errors.New("--method is required")
	}

	httpTargets := runner.SplitCSV(targetsRaw)
	if len(httpTargets) == 0 {
		return errors.New("at least one --target is required")
	}
	grpcTargets := runner.SplitCSV(grpcTargetsRaw)
	if len(grpcTargets) == 0 {
		return errors.New("at least one --grpc-target is required")
	}
	if len(grpcTargets) != 1 && len(grpcTargets) != len(httpTargets) {
		return fmt.Errorf("--grpc-target must be 1 or %d entries; got %d", len(httpTargets), len(grpcTargets))
	}

	ns, ver := splitServiceVer(*service)
	inputDesc, outputDesc, fullPath, err := fetchGRPCDescriptors(httpTargets[0], ns, ver, *method)
	if err != nil {
		return fmt.Errorf("resolve descriptor: %w", err)
	}

	requestProto, err := decodeArgsJSON(*argsJSON, inputDesc)
	if err != nil {
		return fmt.Errorf("decode --args: %w", err)
	}

	targets := make([]runner.Target, 0, len(httpTargets))
	for i, ht := range httpTargets {
		gt := grpcTargets[0]
		if len(grpcTargets) == len(httpTargets) {
			gt = grpcTargets[i]
		}
		conn, err := grpc.NewClient(gt, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("grpc dial %s: %w", gt, err)
		}
		fire := makeGRPCFire(*timeout, conn, fullPath, requestProto, outputDesc)
		targets = append(targets, runner.Target{
			Label:      fmt.Sprintf("%s -> %s%s", gt, ht, fullPath),
			MetricsURL: runner.MetricsURLWithPath(ht, *metricsPath),
			Fire:       fire,
		})
	}

	directTargets := runner.SplitCSV(directTargetsRaw)
	var directTargetsBuilt []runner.Target
	for _, dt := range directTargets {
		conn, err := grpc.NewClient(dt, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("grpc dial direct %s: %w", dt, err)
		}
		fire := makeGRPCFire(*timeout, conn, fullPath, requestProto, outputDesc)
		directTargetsBuilt = append(directTargetsBuilt, runner.Target{
			Label: fmt.Sprintf("direct %s%s", dt, fullPath),
			// MetricsURL intentionally empty: direct dials bypass the gateway,
			// so /api/metrics has nothing to say about them.
			Fire: fire,
		})
	}

	opts := runner.Options{
		RPS:           *rps,
		Duration:      *duration,
		Concurrency:   *concurrency,
		Shards:        *shards,
		ServerMetrics: *serverSide,
	}

	fmt.Fprintf(os.Stdout, "running %d req/s for %s against %d gRPC target(s); method=%s\n", *rps, duration.String(), len(targets), fullPath)
	gwRes, err := runner.Run(opts, ternaryStr(len(directTargetsBuilt) > 0, "gateway", ""), targets)
	if err != nil {
		return err
	}
	runner.PrintPass(opts, gwRes)
	if err := writeJSONIfRequested(*jsonOut, opts, gwRes); err != nil {
		return err
	}

	if len(directTargetsBuilt) == 0 {
		return nil
	}
	fmt.Fprintf(os.Stdout, "\nrunning direct pass: %d req/s for %s against %d direct target(s); bypassing gateway\n", *rps, duration.String(), len(directTargetsBuilt))
	directOpts := opts
	directOpts.ServerMetrics = false // gateway not in path
	dRes, err := runner.Run(directOpts, "direct", directTargetsBuilt)
	if err != nil {
		return err
	}
	runner.PrintPass(directOpts, dRes)
	runner.PrintCompare(gwRes, dRes)
	return nil
}

// ternaryStr is a tiny shim so the call-site stays one expression.
func ternaryStr(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func makeGRPCFire(timeout time.Duration, conn *grpc.ClientConn, fullPath string, request proto.Message, outputDesc protoreflect.MessageDescriptor) func(context.Context, *runner.Stats) {
	return func(ctx context.Context, s *runner.Stats) {
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		resp := dynamicpb.NewMessage(outputDesc)
		start := time.Now()
		err := conn.Invoke(callCtx, fullPath, request, resp)
		elapsed := time.Since(start)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				s.RecordErr(runner.ErrTransport, err.Error())
				return
			}
			label := "rpc:" + st.Code().String()
			s.RecordCode(label)
			s.RecordErr(runner.ErrTransport, st.Message())
			s.RecordBody(label, runner.Truncate(st.Message(), 200))
			return
		}
		s.RecordCode("rpc:OK")
		s.RecordOK(elapsed)
		// Snapshot one example response body so the summary block is
		// non-empty and operators can see what's coming back.
		body, mErr := protojson.Marshal(resp)
		if mErr == nil {
			s.RecordBody("rpc:OK", runner.Truncate(string(body), 200))
		}
	}
}

// fetchGRPCDescriptors GETs /api/schema/proto?service=<ns>(:<ver>)
// and walks the FDS to find the (service, method) named --method.
// Returns input + output MessageDescriptors and the canonical gRPC
// path "/<svc.FullName>/<method>" used by grpc.Invoke.
func fetchGRPCDescriptors(httpBase, ns, ver, methodName string) (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor, string, error) {
	u := strings.TrimRight(httpBase, "/") + "/api/schema/proto?service=" + ns
	if ver != "" {
		u += ":" + ver
	}
	resp, err := http.Get(u)
	if err != nil {
		return nil, nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil, "", fmt.Errorf("schema/proto status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, "", err
	}
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(raw, fds); err != nil {
		return nil, nil, "", fmt.Errorf("unmarshal FDS: %w", err)
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, nil, "", fmt.Errorf("build files: %w", err)
	}

	// Walk every service in every file. The gateway emits one or more
	// services per registered namespace; --method matches by leaf name.
	var matches []protoreflect.MethodDescriptor
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		svcs := fd.Services()
		for i := 0; i < svcs.Len(); i++ {
			sd := svcs.Get(i)
			methods := sd.Methods()
			for j := 0; j < methods.Len(); j++ {
				md := methods.Get(j)
				if string(md.Name()) == methodName {
					matches = append(matches, md)
				}
			}
		}
		return true
	})
	if len(matches) == 0 {
		return nil, nil, "", fmt.Errorf("method %q not found in service %q", methodName, ns)
	}
	if len(matches) > 1 {
		names := make([]string, 0, len(matches))
		for _, md := range matches {
			names = append(names, string(md.Parent().(protoreflect.ServiceDescriptor).FullName())+"."+string(md.Name()))
		}
		return nil, nil, "", fmt.Errorf("method %q is ambiguous in %q: %v", methodName, ns, names)
	}
	md := matches[0]
	if md.IsStreamingClient() || md.IsStreamingServer() {
		return nil, nil, "", fmt.Errorf("method %q is streaming; bench traffic grpc only supports unary", methodName)
	}
	svc := md.Parent().(protoreflect.ServiceDescriptor)
	path := fmt.Sprintf("/%s/%s", svc.FullName(), md.Name())
	return md.Input(), md.Output(), path, nil
}

// splitServiceVer parses "ns" or "ns:vN" into (ns, ver).
func splitServiceVer(s string) (string, string) {
	ns, ver, _ := strings.Cut(s, ":")
	return strings.TrimSpace(ns), strings.TrimSpace(ver)
}

// decodeArgsJSON parses a JSON object into a dynamicpb.Message of the
// given input descriptor. Uses protojson so canonical-args names map
// to proto field names per the JSON-mapping spec.
func decodeArgsJSON(jsonStr string, inputDesc protoreflect.MessageDescriptor) (proto.Message, error) {
	msg := dynamicpb.NewMessage(inputDesc)
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(jsonStr), msg); err != nil {
		return nil, err
	}
	return msg, nil
}
