package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/yousuf64/chord-kv/bootstrap"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/node/peerserver"
	"github.com/yousuf64/chord-kv/node/transport"
	"github.com/yousuf64/chord-kv/router"
	"github.com/yousuf64/chord-kv/util"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
)

var addr = flag.String("addr", "localhost:8080", "host address")
var dns = flag.String("dns", "", "public dns")
var bootstrapAddr = flag.String("bootstrap", "localhost:55555", "bootstrap address")
var username = flag.String("username", "sugarcane", "username")
var m = flag.Int("M", 3, "M")
var ringSize = flag.Uint("ringSize", 9, "ring size")

func main() {
	flag.Parse()

	log.Println("starting...")

	if *dns == "" {
		*dns = *addr
	}

	log.Printf("Host: %s | DNS: %s | Bootstrap Server: %s | Username: %s | Node ID: %d | M: %d | Ring Size: %d\n", *addr, *dns, *bootstrapAddr, *username, util.Hash(*addr), *m, *ringSize)

	jaegerEndpoint, ok := os.LookupEnv("OTEL_EXPORTER_JAEGER_ENDPOINT")
	if !ok {
		jaegerEndpoint = "http://localhost:14268/api/traces"
	}
	log.Printf("Jaeger Endpoint: %s\n", jaegerEndpoint)

	util.M = *m
	util.RingSize = *ringSize

	shutdown := initTracer(fmt.Sprintf("%s/%s", *addr, *username))
	defer shutdown()

	bsChan := make(chan struct{})
	joinAddr := ""

	bs := bootstrap.New(*bootstrapAddr)
	bs.RegisterReply = func(status bootstrap.RegisterStatus, nodeIPs []string) {
		defer close(bsChan)

		if status > bootstrap.RegOkTwo {
			log.Fatalf("failed to register: %v", status)
		}

		log.Println("registered at bootstrap")
		if len(nodeIPs) > 0 {
			joinAddr = nodeIPs[0]
		}
	}

	bsUnregistered := make(chan struct{})
	bs.UnregisterReply = func(status bootstrap.UnregisterStatus) {
		defer close(bsUnregistered)

		if status != bootstrap.UnregOk {
			log.Fatalf("failed to unregister: %v", status)
		}

		log.Println("unregistered from bootstrap")
	}

	bs.Register(*dns, *username)
	<-bsChan

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithPropagators(propagation.TraceContext{})),
		),
	)

	ch := chord.NewChord(*addr)
	dkv := kv.NewDistributedKV(ch)

	r := router.New(grpcServer, dkv)

	h2s := &http2.Server{}
	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		panic(err)
	}

	h1s := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: h2c.NewHandler(r, h2s),
	}

	transport.RegisterPeerServer(grpcServer, peerserver.New(ch))

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, os.Kill)

	idleConnsClosed := make(chan struct{})
	go func() {
		<-sigint

		log.Println("Starting graceful shutdown")

		// We received an interrupt signal, shut down.
		if err := h1s.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}

		bs.Unregister(*dns, *username)
		err := ch.Leave(context.Background())
		if err != nil {
			// TODO:
		}

		close(idleConnsClosed)
	}()

	go func() {
		log.Println("HTTP and GRPC server listening at", *addr)
		if err := h1s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	if joinAddr != "" {
		err = ch.Join(context.Background(), node.NewRemoteNode(joinAddr))
		if err != nil {
			log.Printf("failed to join node %s: %v", joinAddr, err)
			sigint <- os.Interrupt
		} else {
			log.Println("joined to", joinAddr)
		}
	}

	if err == nil {
		ch.StartJobs()
	}

	<-idleConnsClosed
	<-bsUnregistered

	log.Println("exited!")
}
