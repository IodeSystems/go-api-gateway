// hello-proto: a tiny native gRPC service self-registering with the
// gateway via the proto control plane. Sibling of hello-openapi +
// hello-graphql; the gateway dispatches `hello.v1.HelloService.Hello`
// to this server.
//
// Exists primarily so bench traffic grpc --direct has a format-native
// upstream that's parallel to its openapi/graphql counterparts. The
// existing greeter service covers proto Hello + subscriptions for the
// README tutorial; hello-proto is the bench-focused minimal twin.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	hellov1 "github.com/iodesystems/gwag/v2/examples/multi/gen/hello/v1"
	"github.com/iodesystems/gwag/v2/gw/controlclient"
)

type helloImpl struct {
	hellov1.UnimplementedHelloServiceServer
}

func (*helloImpl) Hello(_ context.Context, req *hellov1.HelloRequest) (*hellov1.HelloResponse, error) {
	return &hellov1.HelloResponse{Greeting: "Hello, " + req.GetName() + "!"}, nil
}

func main() {
	addr := flag.String("addr", ":50055", "gRPC listen address")
	gatewayAddr := flag.String("gateway", "localhost:50090", "Gateway control plane address")
	advertise := flag.String("advertise", "localhost:50055", "Address to advertise to the gateway")
	namespace := flag.String("namespace", "hello_proto", "Namespace to register under")
	version := flag.String("version", "v1", "Service version (unstable / vN)")
	register := flag.Bool("register", true, "Self-register with the gateway's control plane. Set false when running against a non-gwag gateway (Apollo Router, graphql-mesh) that introspects backends directly.")
	flag.Parse()

	protoSource, err := os.ReadFile("protos/hello.proto")
	if err != nil {
		log.Fatalf("read hello.proto (run from examples/multi/): %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	hellov1.RegisterHelloServiceServer(srv, &helloImpl{})
	go func() {
		log.Printf("hello-proto gRPC listening on %s", *addr)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	var reg *controlclient.Registration
	if *register {
		reg, err = controlclient.SelfRegister(context.Background(), controlclient.Options{
			GatewayAddr: *gatewayAddr,
			ServiceAddr: *advertise,
			InstanceID:  "hello-proto@" + *addr,
			Services: []controlclient.Service{{
				Namespace:   *namespace,
				Version:     *version,
				ProtoSource: protoSource,
			}},
		})
		if err != nil {
			log.Fatalf("self-register: %v", err)
		}
		log.Printf("hello-proto registered with %s as %s:%s", *gatewayAddr, *namespace, *version)
	} else {
		log.Printf("hello-proto: --register=false, skipping control-plane registration")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("hello-proto shutting down")
	if reg != nil {
		_ = reg.Close(context.Background())
	}
	srv.GracefulStop()
}
