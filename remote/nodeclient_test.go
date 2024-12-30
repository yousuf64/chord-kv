package remote

import (
	"github.com/yousuf64/chord-kv/node"
	"testing"
)

var testTable = []struct {
	id            uint64
	successorId   uint64
	predecessorId uint64
	fingerIdx     [M]uint64
	fingerId      [M]uint64
}{
	{id: 0, successorId: 1, predecessorId: 3, fingerIdx: [3]uint64{1, 2, 4}, fingerId: [3]uint64{1, 3, 0}},
	{id: 1, successorId: 3, predecessorId: 0, fingerIdx: [3]uint64{2, 3, 5}, fingerId: [3]uint64{3, 3, 0}},
	{id: 3, successorId: 0, predecessorId: 1, fingerIdx: [3]uint64{4, 5, 7}, fingerId: [3]uint64{0, 0, 0}},
}

func Test_JoinAllToInitNode(t *testing.T) {
	n0 := New("node6")
	n0.Join(nil)
	runPeriodicJobs(n0)

	n1 := New("node7")
	n1.Join(n0)
	runPeriodicJobs(n0, n1, n0, n1)

	n2 := New("node2")
	n2.Join(n0)
	runPeriodicJobs(n0, n1, n2, n0, n1, n2)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes(t *testing.T) {
	n0 := New("node6")
	n0.Join(nil)
	runPeriodicJobs(n0)

	n1 := New("node7")
	n1.Join(n0)
	runPeriodicJobs(n0, n1, n0, n1)

	n2 := New("node2")
	n2.Join(n1)
	runPeriodicJobs(n0, n1, n2, n0, n1, n2)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinAllNodes_DiffOrder(t *testing.T) {
	n0 := New("node6")
	n0.Join(nil)
	runPeriodicJobs(n0)

	n2 := New("node2")
	n2.Join(n0)
	runPeriodicJobs(n0, n2, n0, n2)

	n1 := New("node7")
	n1.Join(n0)
	runPeriodicJobs(n0, n2, n1, n0, n2, n1)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes_DiffOrder(t *testing.T) {
	n0 := New("node6")
	n0.Join(nil)
	runPeriodicJobs(n0)

	n2 := New("node2")
	n2.Join(n0)
	runPeriodicJobs(n0, n2, n0, n2)

	n1 := New("node7")
	n1.Join(n2)
	runPeriodicJobs(n0, n2, n1, n0, n2, n1)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinAllNodes_DiffOrder_2(t *testing.T) {
	n2 := New("node2")
	n2.Join(nil)
	runPeriodicJobs(n2)

	n1 := New("node7")
	n1.Join(n2)
	runPeriodicJobs(n2, n1, n2, n1)

	n0 := New("node6")
	n0.Join(n2)
	runPeriodicJobs(n2, n1, n0, n2, n1, n0)

	evaluateNodes(t, n0, n1, n2)
}

func Test_JoinDiffNodes_DiffOrder_2(t *testing.T) {
	n2 := New("node2")
	n2.Join(nil)
	runPeriodicJobs(n2)

	n1 := New("node7")
	n1.Join(n2)
	runPeriodicJobs(n2, n1, n2, n1)

	n0 := New("node6")
	n0.Join(n1)
	runPeriodicJobs(n2, n1, n0, n2, n1, n0)

	evaluateNodes(t, n0, n1, n2)
}

func evaluateNodes(t *testing.T, ns ...*node.Node) {
	for i, node := range ns {
		testItem := testTable[i]

		if node.Id != testItem.id {
			t.Fatalf("[%d] node id mismatch", i)
		}

		if node.Successor.Id != testItem.successorId {
			t.Fatalf("[%d] node successor mismatch", i)
		}

		if node.Predcessor.Id != testItem.predecessorId {
			t.Fatalf("[%d] node predecessor mismatch", i)
		}

		if node.fingerIdx != testItem.fingerIdx {
			t.Fatalf("[%d] finger idx mismatch", i)
		}

		for fi, f := range node.finger {
			if f.Id != testItem.fingerId[fi] {
				t.Fatalf("[%d:%d] finger id mismatch", i, fi)
			}
		}
	}
}

func runPeriodicJobs(ns ...*node.Node) {
	for _, n := range ns {
		n.Stabilize()
		n.FixFinger(1)
		n.FixFinger(2)
		n.FixFinger(3)
	}
}
