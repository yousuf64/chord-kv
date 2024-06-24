package main

import (
	"github.com/yousuf64/chord-kv/node"
	"log"
	"os"
	"os/signal"
)

func fix(ns ...*node.Node) {
	for _, n := range ns {
		n.Stabilize()
		n.FixFinger(1)
		n.FixFinger(2)
		n.FixFinger(3)
	}
}

func main() {
	n0 := node.New("node6") // Id: 2
	n0.Join(nil)
	fix(n0)

	n1 := node.New("node7")
	n1.Join(n0)
	fix(n0, n1, n0, n1)

	n2 := node.New("node2")
	n2.Join(n0)
	fix(n0, n1, n2, n0, n1, n2, n0, n1, n2)

	log.Println(n0.Lookup("hello"))
	log.Println(n0.Lookup("lucid"))
	log.Println(n0.Lookup("bcs"))

	//n0 := node.New("node0") // Id: 2
	//n0.Join(nil)
	//fix(n0)
	//
	//n1 := node.New("node4")
	//n1.Join(n0)
	//fix(n0, n1, n0, n1)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	for {
		select {
		case <-c:
		default:
			//println("foo")
		}
	}
}
