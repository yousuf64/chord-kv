package main

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/node/server"
	"github.com/yousuf64/chord-kv/node/transport"
	"github.com/yousuf64/chord-kv/util"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

func main() {
	os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "99")
	os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "info")
	svr0 := "localhost:3030"
	svr1 := "localhost:4245"
	svr3 := "localhost:7072"
	// START 3030 NODE
	go func() {
		lis, err := net.Listen("tcp", svr0)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		ch := chord.NewChord(svr0)
		startJobs(ch)

		transport.RegisterPeerServer(s, server.New(ch))
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	go func() {
		lis, err := net.Listen("tcp", svr1)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		ch := chord.NewChord(svr1)
		ch.Join(context.Background(), node.NewRemoteNode(svr0))
		startJobs(ch)

		transport.RegisterPeerServer(s, server.New(ch))
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	go func() {
		lis, err := net.Listen("tcp", svr3)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		ch := chord.NewChord(svr3)
		ch.Join(context.Background(), node.NewRemoteNode(svr1))
		startJobs(ch)

		transport.RegisterPeerServer(s, server.New(ch))
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func startJobs(chord chord.ChordNode) {
	go func() {
		t := time.NewTicker(time.Millisecond * 100)
		for range t.C {
			err := chord.Stabilize()
			if err != nil {
				panic(err)
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
				panic(err)
			}

			n++
		}
	}()

	//chord.CheckPredecessor()
}
