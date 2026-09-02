// Greeter service: starts a real gRPC server on :50051 and self-registers
// with the gateway's control plane. Heartbeats forever; deregisters on
// SIGINT/SIGTERM.
//
// Also publishes Greetings events to NATS so the
// `Subscription.greeter_greetings(name: ...)` GraphQL field has a live
// producer. See README §Subscriptions for the channel-name contract —
// tl;dr the subject is `events.greeter.Greetings.<name>` (the
// stringified canonical call); subscribers picking distinct names land
// on distinct NATS subjects automatically.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/iodesystems/gwag/v2/gw/controlclient"
	greeterv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/greeter/v1"
)

type greeterImpl struct {
	greeterv1.UnimplementedGreeterServiceServer
	delay time.Duration
}

func (g *greeterImpl) Hello(ctx context.Context, req *greeterv1.HelloRequest) (*greeterv1.HelloResponse, error) {
	if g.delay > 0 {
		time.Sleep(g.delay)
	}
	// Surface the X-Source-IP metadata stamped by the gateway's
	// InjectHeader demo (see examples/multi/cmd/gateway/main.go).
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-source-ip"); len(v) > 0 {
			log.Printf("greeter: hello name=%q x-source-ip=%s", req.GetName(), v[0])
		}
	}
	name := req.GetName()
	if name == "" {
		name = "stranger"
	}
	return &greeterv1.HelloResponse{Greeting: "Hello, " + name + "!"}, nil
}

// buildTag is stamped at link time on release builds:
//
//	go build -ldflags "-X 'main.buildTag=v1.2.3'" ./cmd/greeter
//
// Trunk CI omits the -ldflags so the binary registers as `unstable`;
// release CI sets it to the cut version so SelfRegister refuses the
// `unstable` slot. Plan §4 forcing function — see controlclient.Options.
var buildTag string

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	gatewayAddr := flag.String("gateway", "localhost:50090", "Gateway control plane address")
	advertise := flag.String("advertise", "localhost:50051", "Address to advertise to the gateway")
	version := flag.String("version", "v1", "Service version (v1, v2, ...)")
	delay := flag.Duration("delay", 0, "Artificial delay per Hello call (for backpressure tests)")
	maxConcurrency := flag.Uint("max-concurrency", 0, "Pool-wide cap on simultaneous unary dispatches (0 = unbounded)")
	maxConcurrencyPerInstance := flag.Uint("max-concurrency-per-instance", 0, "Per-replica cap on simultaneous unary dispatches (0 = unbounded)")
	natsURL := flag.String("nats", "nats://localhost:14222", "NATS URL for publishing Greetings events; empty disables the publisher")
	publishInterval := flag.Duration("publish-interval", 2*time.Second, "Interval between Greetings publishes; 0 disables")
	flag.Parse()

	protoSource, err := os.ReadFile("protos/greeter.proto")
	if err != nil {
		log.Fatalf("read greeter.proto (run from examples/multi/): %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	greeterv1.RegisterGreeterServiceServer(srv, &greeterImpl{delay: *delay})
	go func() {
		log.Printf("greeter gRPC listening on %s", *addr)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	reg, err := controlclient.SelfRegister(context.Background(), controlclient.Options{
		GatewayAddr: *gatewayAddr,
		ServiceAddr: *advertise,
		InstanceID:  "greeter@" + *addr,
		BuildTag:    buildTag,
		Services: []controlclient.Service{{
			Namespace:   "greeter",
			Version:     *version,
			ProtoSource: protoSource,
			// Per-binding caps default to 0 (unbounded). Use
			// --max-concurrency / --max-concurrency-per-instance to
			// demo the backpressure knobs that ship in
			// cpv1.ServiceBinding.
			MaxConcurrency:            uint32(*maxConcurrency),
			MaxConcurrencyPerInstance: uint32(*maxConcurrencyPerInstance),
		}},
	})
	if err != nil {
		log.Fatalf("self-register: %v", err)
	}
	log.Printf("greeter registered with %s", *gatewayAddr)

	// Optional NATS publisher: emits a Greeting every --publish-interval
	// to events.greeter.Greetings.<name> so the GraphQL subscription
	// field actually has a producer to fan out from. Subject convention
	// matches gw/subscriptions.go::subjectFor — see README §Subscriptions.
	pubCtx, cancelPub := context.WithCancel(context.Background())
	defer cancelPub()
	if *natsURL != "" && *publishInterval > 0 {
		nc, err := nats.Connect(*natsURL)
		if err != nil {
			log.Printf("greeter: NATS connect %q failed (%v); subscription publisher disabled", *natsURL, err)
		} else {
			defer nc.Close()
			go publishGreetings(pubCtx, nc, *publishInterval)
			log.Printf("greeter publishing to %s every %s (subject events.greeter.Greetings.<name>)", *natsURL, *publishInterval)
		}
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("greeter shutting down")
	_ = reg.Close(context.Background())
	srv.GracefulStop()
}

// publishGreetings emits one Greeting every `interval` to
// events.greeter.Greetings.<name>, cycling through a small pool of
// names. The subject format encodes (namespace, method, args) per
// gw/subscriptions.go::subjectFor — distinct names land on distinct
// subjects, so a subscriber for `name="alice"` only receives alice's
// events. A subscriber passing `name="*"` (or no name) gets the
// wildcard and sees them all.
func publishGreetings(ctx context.Context, nc *nats.Conn, interval time.Duration) {
	names := []string{"alice", "bob", "world"}
	t := time.NewTicker(interval)
	defer t.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			name := names[i%len(names)]
			i++
			payload, err := proto.Marshal(&greeterv1.Greeting{
				Greeting: "Hello, " + name + "!",
				ForName:  name,
			})
			if err != nil {
				log.Printf("greeter: marshal Greeting: %v", err)
				continue
			}
			subject := fmt.Sprintf("events.greeter.Greetings.%s", name)
			if err := nc.Publish(subject, payload); err != nil {
				log.Printf("greeter: publish %s: %v", subject, err)
			}
		}
	}
}
