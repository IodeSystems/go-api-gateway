// Library service: real gRPC server on :50052, self-registers with the
// gateway. Mirror of greeter but with a different namespace and a
// trivial book catalogue for demo data.
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

	"github.com/iodesystems/gwag/v2/gw/controlclient"
	libraryv1 "github.com/iodesystems/gwag/v2/examples/multi/gen/library/v1"
)

var books = []*libraryv1.Book{
	{Id: "1", Title: "Go Programming", Author: "Alan Donovan", Year: 2015},
	{Id: "2", Title: "The Go Programming Language", Author: "Brian Kernighan", Year: 2015},
	{Id: "3", Title: "Designing Data-Intensive Applications", Author: "Martin Kleppmann", Year: 2017},
}

type libraryImpl struct {
	libraryv1.UnimplementedLibraryServiceServer
}

func (*libraryImpl) ListBooks(_ context.Context, req *libraryv1.ListBooksRequest) (*libraryv1.ListBooksResponse, error) {
	out := &libraryv1.ListBooksResponse{}
	for _, b := range books {
		if req.GetAuthor() != "" && b.Author != req.GetAuthor() {
			continue
		}
		out.Books = append(out.Books, b)
	}
	return out, nil
}

func (*libraryImpl) GetBook(_ context.Context, req *libraryv1.GetBookRequest) (*libraryv1.Book, error) {
	for _, b := range books {
		if b.Id == req.GetId() {
			return b, nil
		}
	}
	return nil, nil
}

func main() {
	addr := flag.String("addr", ":50052", "gRPC listen address")
	gatewayAddr := flag.String("gateway", "localhost:50090", "Gateway control plane address")
	advertise := flag.String("advertise", "localhost:50052", "Address to advertise to the gateway")
	flag.Parse()

	protoSource, err := os.ReadFile("protos/library.proto")
	if err != nil {
		log.Fatalf("read library.proto (run from examples/multi/): %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	libraryv1.RegisterLibraryServiceServer(srv, &libraryImpl{})
	go func() {
		log.Printf("library gRPC listening on %s", *addr)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	reg, err := controlclient.SelfRegister(context.Background(), controlclient.Options{
		GatewayAddr: *gatewayAddr,
		ServiceAddr: *advertise,
		InstanceID:  "library@" + *addr,
		Services: []controlclient.Service{
			{Namespace: "library", ProtoSource: protoSource},
		},
	})
	if err != nil {
		log.Fatalf("self-register: %v", err)
	}
	log.Printf("library registered with %s", *gatewayAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("library shutting down")
	_ = reg.Close(context.Background())
	srv.GracefulStop()
}
