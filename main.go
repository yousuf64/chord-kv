package main

import (
	"context"
	"encoding/json"
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
	"github.com/yousuf64/shift"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetReply struct {
	Value string `json:"value"`
}

func GrpcFilter(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			return next(w, r, route)
		}

		return nil
	}
}

var addr = flag.String("addr", "localhost:8080", "host address")
var bootstrapAddr = flag.String("bootstrap", "localhost:55555", "bootstrap address")
var username = flag.String("username", "sugarcane", "username")

//var joinAddr = flag.String("join", "", "join address")

func main() {
	flag.Parse()

	log.Println("starting...")
	log.Printf("Address: %s | Username: %s | Node ID: %d", *addr, *username, util.Hash(*addr))

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

	bs.Register(*addr, *username)
	<-bsChan

	grpcServer := grpc.NewServer()

	ch := chord.NewChord(*addr)
	dkv := kv.NewDistributedKV(ch)

	r := router.New(grpcServer, dkv)

	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:    *addr,
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

		bs.Unregister(*addr, *username)
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

	var err error
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

func main2() {
	os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "99")
	os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "info")
	svr0 := "localhost:3030"
	svr1 := "localhost:4245"
	svr3 := "localhost:7072"
	// START 3030 NODE
	var dkv0, dkv1, dkv2 kv.KV
	go func() {
		//lis, err := net.Listen("tcp", svr0)
		//if err != nil {
		//	log.Fatalf("failed to listen: %v", err)
		//}
		s := grpc.NewServer()

		ch := chord.NewChord(svr0)
		dkv0 = kv.NewDistributedKV(ch)

		router := shift.New()
		router.Group("/api", func(g *shift.Group) {
			g.POST("/set", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
				req := SetRequest{}
				err := json.NewDecoder(r.Body).Decode(&req)
				if err != nil {
					return err
				}

				err = dkv0.Insert(context.Background(), req.Key, req.Value)
				if err != nil {
					return err
				}
				return nil
			})

			g.GET("/get/:key", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
				value, err := dkv0.Get(context.Background(), route.Params.Get("key"))
				if err != nil {
					return err
				}

				err = json.NewEncoder(w).Encode(&GetReply{Value: value})
				if err != nil {
					return err
				}
				return nil
			})
		})
		router.With(GrpcFilter).All("/*any", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			s.ServeHTTP(w, r)
			return nil
		})

		h2s := &http2.Server{}
		h1s := &http.Server{
			Addr:    svr0,
			Handler: h2c.NewHandler(router.Serve(), h2s),
		}

		//http2.Transport{
		//	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		//}
		//err := http2.ConfigureServer(&h, nil)
		//if err != nil {
		//	panic(err)
		//}

		transport.RegisterPeerServer(s, peerserver.New(ch))

		err := h1s.ListenAndServe()
		if err != nil {
			panic(err)
		}

		//err := http.ListenAndServe(svr0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//	s.ServeHTTP(w, r)
		//}))
		//if err != nil {
		//	panic(err)
		//}

		startJobs(ch)

		//log.Printf("server listening at %v", lis.Addr())
		//if err := s.Serve(lis); err != nil {
		//	log.Fatalf("failed to serve: %v", err)
		//}
	}()

	time.Sleep(3 * time.Second)

	//go func() {

	lis, err := net.Listen("tcp", svr1)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	ch := chord.NewChord(svr1)
	dkv1 = kv.NewDistributedKV(ch)
	ch.Join(context.Background(), node.NewRemoteNode(svr0))
	startJobs(ch)

	transport.RegisterPeerServer(s, peerserver.New(ch))
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	//}()

	time.Sleep(3 * time.Second)

	go func() {
		lis, err := net.Listen("tcp", svr3)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		ch := chord.NewChord(svr3)
		dkv2 = kv.NewDistributedKV(ch)
		ch.Join(context.Background(), node.NewRemoteNode(svr1))
		startJobs(ch)

		transport.RegisterPeerServer(s, peerserver.New(ch))
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	return
	err = dkv0.Insert(context.Background(), "hello damn maxver", "food")
	if err != nil {
		panic(err)
	}

	fmt.Println(dkv0.Dump())
	fmt.Println(dkv1.Dump())
	fmt.Println(dkv2.Dump())

	v1, _ := dkv1.Get(context.Background(), "hello damn maxver")
	v2, _ := dkv1.Get(context.Background(), "damn maxver")
	v3, _ := dkv1.Get(context.Background(), "hello damn")
	v4, _ := dkv1.Get(context.Background(), "maxver")
	v5, _ := dkv1.Get(context.Background(), "maxver damn")
	v6, _ := dkv1.Get(context.Background(), "damn hello")
	fmt.Println("v1", v1)
	fmt.Println("v2", v2)
	fmt.Println("v3", v3)
	fmt.Println("v4", v4)
	fmt.Println("v5", v5)
	fmt.Println("v6", v6)

	c = make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func startJobs(chord chord.ChordNode) {
	go func() {
		t := time.NewTicker(time.Millisecond * 100)
		for range t.C {
			err := chord.Stabilize()
			if err != nil {
				//log.Println(err)
				//panic(err)
			}
		}
	}()

	go func() {
		n := 1

		t := time.NewTicker(time.Millisecond * 150)
		for range t.C {
			if n > util.M {
				n = 1
			}

			err := chord.FixFinger(n)
			if err != nil {
				//log.Println(err)
				//panic(err)
			}

			n++
		}
	}()

	go func() {
		n := 1

		t := time.NewTicker(time.Millisecond * 250)
		for range t.C {
			if n > util.M {
				n = 1
			}

			chord.CheckPredecessor()
			n++
		}
	}()
}
