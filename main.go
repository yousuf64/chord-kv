package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	bootstrapclient "github.com/yousuf64/chord-kv/bootstrap/client"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/chord/intercom"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/chord-kv/router"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"log"
	"net/http"
	"os"
	"os/signal"
)

var port = flag.Int("port", 80, "port to expose the HTTP and GRPC server")
var publicHost = flag.String("publichost", "", "public host")
var bootstrapAddr = flag.String("bootstrap", "localhost:55555", "bootstrap address")
var username = flag.String("username", "sugarcane", "username")
var m = flag.Int("M", 3, "M")
var ringSize = flag.Int("ringSize", 9, "ring size")

func main() {
	flag.Parse()

	log.Println("starting...")

	var publicAddress string
	if *publicHost == "" {
		publicAddress = fmt.Sprintf("%s:%d", "0.0.0.0", *port)
	} else {
		publicAddress = fmt.Sprintf("%s:%d", *publicHost, *port)
	}

	hasher := chord.GenHasher(uint64(*ringSize))
	ch := chord.NewChord(publicAddress, *m, hasher)

	log.Printf("Port: %d | PublicAddress: %s | BootstrapServer: %s | Username: %s | NodeID: %d | M: %d | RingSize: %d\n", *port, publicAddress, *bootstrapAddr, *username, ch.ID(), *m, *ringSize)

	jaegerEndpoint, ok := os.LookupEnv("OTEL_EXPORTER_JAEGER_ENDPOINT")
	if !ok {
		jaegerEndpoint = "http://localhost:14268/api/traces"
	}
	log.Printf("Jaeger Endpoint: %s\n", jaegerEndpoint)

	shutdown := initTracer(fmt.Sprintf("%d/%s", *port, *username))
	defer shutdown()

	bsChan := make(chan struct{})
	joinAddr := ""

	bs := bootstrapclient.New(*bootstrapAddr)
	bs.RegisterReply = func(status bootstrapclient.RegisterStatus, nodeIPs []string) {
		defer close(bsChan)

		if status > bootstrapclient.RegOkTwo {
			log.Fatalf("failed to register: %v", status)
		}

		log.Println("registered at bootstrap")
		if len(nodeIPs) > 0 {
			joinAddr = nodeIPs[0]
		}
	}

	bsUnregistered := make(chan struct{})
	bs.UnregisterReply = func(status bootstrapclient.UnregisterStatus) {
		defer close(bsUnregistered)

		if status != bootstrapclient.UnregOk {
			log.Fatalf("failed to unregister: %v", status)
		}

		log.Println("unregistered from bootstrap")
	}

	bs.Register(publicAddress, *username)
	<-bsChan

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithPropagators(propagation.TraceContext{})),
		),
	)

	dkv := kv.NewDistributedKV(ch)

	r := router.New(grpcServer, dkv)

	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: h2c.NewHandler(r, h2s),
	}

	intercom.RegisterIntercomServer(grpcServer, chord.New(ch))

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

		bs.Unregister(publicAddress, *username)
		err := ch.Leave(context.Background())
		if err != nil {
			// TODO:
		}

		close(idleConnsClosed)
	}()

	go func() {
		log.Println("HTTP and GRPC server listening at", *port)
		if err := h1s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	var err error
	if joinAddr != "" {
		err = ch.Join(context.Background(), chord.NewNodeClient(joinAddr, ch.Hasher()))
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
