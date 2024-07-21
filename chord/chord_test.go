package chord

import (
	"context"
	"github.com/yousuf64/chord-kv/util"
	"testing"
	"time"
)

var testTable = []struct {
	id            uint64
	successorId   uint64
	predecessorId uint64
	fingerIdx     [util.M]uint64
	fingerId      [util.M]uint64
}{
	{id: 0, successorId: 1, predecessorId: 3, fingerIdx: [util.M]uint64{1, 2, 4}, fingerId: [util.M]uint64{1, 3, 0}},
	{id: 1, successorId: 3, predecessorId: 0, fingerIdx: [util.M]uint64{2, 3, 5}, fingerId: [util.M]uint64{3, 3, 0}},
	{id: 3, successorId: 0, predecessorId: 1, fingerIdx: [util.M]uint64{4, 5, 7}, fingerId: [util.M]uint64{0, 0, 0}},
}

func Test_JoinAllToInitNode(t *testing.T) {
	n0 := NewChord("node6")
	n0.Join(context.Background(), nil) // init lnode, n0.predecessor = nil
	runPeriodicJobs(n0)
	//startJobs(n0)                      // --

	n1 := NewChord("node7")
	n1.Join(context.Background(), n0) // calls n0 -> n1.successor = n0
	runPeriodicJobs(n0, n1, n0, n1)
	//startJobs(n1)                     // checks if successor's predecessor is myself
	// Notify(n0 to update its predecessor)

	n2 := NewChord("node2")
	n2.Join(context.Background(), n0)
	runPeriodicJobs(n0, n1, n2, n0, n1, n2, n0, n1, n2)
	//startJobs(n2)

	//n2.ClientPut("Lord of the Rings")

	// Lord - 2		-> lnode
	// of - 4
	// the - 2
	// Rings - 5

	// Node02
	//// Lord
	////// [of, the, Rings]

	//n2.ClientGet("Lord (remote) Rings (local)")
	// Lord - 2		-> lnode.Get(key: of, match: Lord Rings)	-> Hit Node02

	//time.Sleep(time.Second * 20)
	//c := make(chan os.Signal, 1)
	//signal.Notify(c, syscall.SIGTERM, syscall.SIGKILL)
	//<-c

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes(t *testing.T) {
	n0 := NewChord("node6")
	n0.Join(context.Background(), nil)
	runPeriodicJobs(n0)

	n1 := NewChord("node7")
	n1.Join(context.Background(), n0)
	runPeriodicJobs(n0, n1, n0, n1)

	n2 := NewChord("node2")
	n2.Join(context.Background(), n1)
	runPeriodicJobs(n0, n1, n2, n0, n1, n2)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinAllNodes_DiffOrder(t *testing.T) {
	n0 := NewChord("node6")
	n0.Join(context.Background(), nil)
	runPeriodicJobs(n0)

	n2 := NewChord("node2")
	n2.Join(context.Background(), n0)
	runPeriodicJobs(n0, n2, n0, n2)

	n1 := NewChord("node7")
	n1.Join(context.Background(), n0)
	runPeriodicJobs(n0, n2, n1, n0, n2, n1)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes_DiffOrder(t *testing.T) {
	n0 := NewChord("node6")
	n0.Join(context.Background(), nil)
	runPeriodicJobs(n0)

	n2 := NewChord("node2")
	n2.Join(context.Background(), n0)
	runPeriodicJobs(n0, n2, n0, n2)

	n1 := NewChord("node7")
	n1.Join(context.Background(), n2)
	runPeriodicJobs(n0, n2, n1, n0, n2, n1)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinAllNodes_DiffOrder_2(t *testing.T) {
	n2 := NewChord("node2")
	n2.Join(context.Background(), nil)
	runPeriodicJobs(n2)

	n1 := NewChord("node7")
	n1.Join(context.Background(), n2)
	runPeriodicJobs(n2, n1, n2, n1)

	n0 := NewChord("node6")
	n0.Join(context.Background(), n2)
	runPeriodicJobs(n2, n1, n0, n2, n1, n0)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes_DiffOrder_2(t *testing.T) {
	n2 := NewChord("node2")
	n2.Join(context.Background(), nil)
	runPeriodicJobs(n2)

	n1 := NewChord("node7")
	n1.Join(context.Background(), n2)
	runPeriodicJobs(n2, n1, n2, n1)

	n0 := NewChord("node6")
	n0.Join(context.Background(), n1)
	runPeriodicJobs(n2, n1, n0, n2, n1, n0)

	evaluateNodes(t, n0, n1, n2)
}

func evaluateNodes(t *testing.T, ns ...*Chord) {
	for i, node := range ns {
		testItem := testTable[i]

		if node.ID() != testItem.id {
			t.Fatalf("[%d] lnode id mismatch", i)
		}

		if node.successor.ID() != testItem.successorId {
			t.Fatalf("[%d] lnode successor mismatch", i)
		}

		if node.predecessor.ID() != testItem.predecessorId {
			t.Fatalf("[%d] lnode predecessor mismatch", i)
		}

		// TODO: ADD
		//if node.fingerIdx != testItem.fingerIdx {
		//	t.Fatalf("[%d] finger idx mismatch", i)
		//}

		for fi, f := range node.finger {
			if f.ID() != testItem.fingerId[fi] {
				t.Fatalf("[%d:%d] finger id mismatch", i, fi)
			}
		}
	}
}

func runPeriodicJobs(ns ...*Chord) {
	for _, n := range ns {
		n.Stabilize()
		n.FixFinger(1)
		n.FixFinger(2)
		n.FixFinger(3)
	}
}

func startJobs(chord ChordNode) {
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
