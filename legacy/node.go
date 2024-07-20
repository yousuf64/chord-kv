package legacy

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"time"
)

const M = 3
const RingSize = 8

type FingerEntry struct {
	start uint64
	node  []*Node
}

type Node struct {
	Id         uint64
	Addr       string
	Successor  *Node
	Predcessor *Node
	finger     [M]*Node
	fingerIdx  [M]uint64
}

func New(addr string) *Node {
	n := &Node{Id: hash(addr), Addr: addr}
	n.Successor = n
	n.Predcessor = nil
	n.finger = [M]*Node{}
	n.fingerIdx = [M]uint64{}
	//for i := 0; i < M; i++ {
	//	n.finger[i] = n
	//}
	return n
}

func hash(key string) uint64 {
	h := sha1.New()
	h.Write([]byte(key))
	b := h.Sum(nil)
	return binary.BigEndian.Uint64(b) % RingSize
}

func (n *Node) Join(jn *Node) {
	if jn == nil {
		return
	}

	n.Predcessor = nil
	n.Successor = jn.FindSuccessor(n.Id)
	log.Printf("%s [%d]: (Join) Successor changed %d", n.Addr, n.Id, n.Successor.Id)

	log.Printf("%s [%d]: (Join) Notified successor %d", n.Addr, n.Id, n.Successor.Id)
	n.Successor.Notify(n)
}

func (n *Node) FindSuccessor(id uint64) *Node {
	if between(id, n.Id, n.Successor.Id) {
		return n.Successor
	}

	closestNode := n.closestPrecedingNode(id)
	if closestNode.Id == n.Id {
		return n
	}
	if closestNode.Id == id {
		return closestNode
	}

	return closestNode.FindSuccessor(id)
}

func (n *Node) closestPrecedingNode(id uint64) *Node {
	for i := M - 1; i >= 0; i-- {
		if n.finger[i] != nil && between(n.finger[i].Id, n.Id, id) {
			return n.finger[i]
		}
	}

	//for i := M - 1; i >= 0; i-- {
	//	if fNode := n.finger[i]; fNode != nil {
	//		if between(id, n.Id, fNode.Id) {
	//			return fNode
	//		}
	//	}
	//}
	return n
}

func (n *Node) Notify(pn *Node) {
	//if n.Successor.Id == n.Id {
	//	// Only applies to 1st node
	//	n.Successor = pn
	//	pn.Notify(n)
	//}

	if n.Predcessor == nil || between(pn.Id, n.Predcessor.Id, n.Id) {
		n.Predcessor = pn
		log.Printf("%s [%d]: (Notify) Predcessor changed %d", n.Addr, n.Id, n.Predcessor.Id)
	}
}

func (n *Node) Stabilize() {
	x := n.Successor.Predcessor
	if x != nil && between(x.Id, n.Id, n.Successor.Id) {
		n.Successor = x
		log.Printf("%s [%d]: Stabilized successor %d", n.Addr, n.Id, n.Successor.Id)
		//n.Successor.Notify(n)
	}

	if n.Successor.Id != n.Id {
		log.Printf("%s [%d]: Notified successor %d", n.Addr, n.Id, n.Successor.Id)
		n.Successor.Notify(n)
	}
}

func (n *Node) StabilizerJob() {
	go func() {
		for {
			successor := n.FindSuccessor(n.Successor.Id)
			x := successor.Predcessor
			if x != nil && between(x.Id, n.Id, successor.Id) {
				n.Successor = x
				log.Printf("%s [%d]: Stabilized successor %d", n.Addr, n.Id, n.Successor.Id)
				//n.Successor.Notify(n)
			}

			if n.Successor.Id != n.Id {
				log.Printf("%s [%d]: Notified successor %d", n.Addr, n.Id, n.Successor.Id)
				n.Successor.Notify(n)
			}

			time.Sleep(1 * time.Second)
		}
	}()
}

func (n *Node) FixFinger(fingerNumber int) error {
	if fingerNumber < 0 {
		return errors.New("cannot be less than 0")
	}
	if fingerNumber > M {
		return errors.New(fmt.Sprintf("cannot exceed %d", M))
	}

	fingerIndex := fingerNumber - 1

	fId := (int(n.Id) + int(math.Pow(2, float64(fingerNumber-1)))) % int(math.Pow(2, M))
	//println(fId)
	//
	//bigId := big.NewInt(int64(n.Id))
	//exp := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(fingerNumber)), nil)
	//sum := new(big.Int).Add(bigId, exp)
	//ceil := new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil)
	//fingerID := new(big.Int).Mod(sum, ceil)

	var err error
	n.finger[fingerIndex] = n.FindSuccessor(uint64(fId))
	n.fingerIdx[fingerIndex] = uint64(fId)

	if n.finger[fingerIndex] != nil {
		log.Printf("%s [%d]: Finger resolved { Index: %d, Id: %d, Successor: %d }", n.Addr, n.Id, fingerIndex, fId, n.finger[fingerIndex].Id)
	}

	return err
}

func (n *Node) FixFingersInBackground() {
	fingerNumber := 1

	go func() {
		for {
			err := n.FixFinger(fingerNumber)
			if err != nil {
				fingerNumber = 1
				continue
			}

			if fingerNumber > M {
				fingerNumber = 1
			} else {
				fingerNumber++
			}

			time.Sleep(time.Duration(2.5 * float64(time.Second)))
		}
	}()
}

func (n *Node) Lookup(key string) uint64 {
	id := hash(key)
	return n.FindSuccessor(id).Id
}

func between(id, start, end uint64) bool {
	if start < end {
		return id > start && id <= end // 3 ...5 8 9... 12
	}
	return id > start || id <= end
	//else if start > end {
	//	return id > start || (id < end) // 8 ...10 0 4... 5
	//}
	//
	//return id != start
	//if id > start && id <= end || (id == end) {
	//	return true
	//}
	//return false

	//if start < end { // 0 - 4
	//	return id > start && id <= end
	//} else { // 4 - 0
	//	return id > start
	//}
}
