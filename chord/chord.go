package chord

import (
	"context"
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/util"
	"log"
	"math"
)

type Core interface {
	node.Node

	Join(ctx context.Context, n node.Node)
	Stabilize() error
	CheckPredecessor()
	FixFinger(fingerNumber int) error
}

type Chord struct {
	id          uint64
	addr        string
	successor   node.Node
	predecessor node.Node
	finger      [util.M]node.Node
}

func NewChord(addr string) *Chord {
	c := &Chord{
		id:          util.Hash(addr),
		addr:        addr,
		successor:   nil,
		predecessor: nil,
		finger:      [util.M]node.Node{},
	}
	c.successor = c

	return c
}

func (c *Chord) ID() uint64 {
	return c.id
}

func (c *Chord) Addr() string {
	return c.addr
}

func (c *Chord) FindSuccessor(ctx context.Context, id uint64) (node.Node, error) {
	if util.Between(id, c.id, c.successor.ID()) {
		return c.successor, nil
	}

	closestNode := c.closestPrecedingNode(id)
	if closestNode.ID() == c.ID() {
		return c, nil
	}
	if closestNode.ID() == id {
		return closestNode, nil
	}

	return closestNode.FindSuccessor(ctx, id)
}

func (c *Chord) closestPrecedingNode(id uint64) node.Node {
	for i := util.M - 1; i >= 0; i-- {
		if c.finger[i] != nil && util.Between(c.finger[i].ID(), c.ID(), id) {
			return c.finger[i]
		}
	}

	return c
}

func (c *Chord) Notify(ctx context.Context, p node.Node) error {
	if c.predecessor == nil || util.Between(p.ID(), c.predecessor.ID(), c.ID()) {
		// TODO: Transfer data
		c.predecessor = p
		log.Printf("%s [%d]: (Notify) predecessor changed %d", c.Addr(), c.ID(), c.predecessor.ID())
	}

	return nil
}

func (c *Chord) GetPredecessor(ctx context.Context) (node.Node, error) {
	if c.predecessor == nil {
		return nil, errors.New("no predecessor")
	}

	// TODO: was NewChord(c.predecessor.Addr(), nil)
	return c.predecessor, nil
}

func (c *Chord) Join(ctx context.Context, n node.Node) {
	if n == nil {
		return
	}

	c.predecessor = nil
	reply, err := n.FindSuccessor(ctx, c.ID())
	if err != nil {
		panic(err)
	}

	//sp := NewChord(reply.Addr(), nil)
	//sp, err := c.newNodeFn(reply.Addr())
	//if err != nil {
	//	panic(err)
	//}

	c.successor = reply
	err = c.successor.Notify(ctx, c)
	if err != nil {
		panic(err)
	}
}

func (c *Chord) Stabilize() error {
	x, err := c.successor.GetPredecessor(context.Background())
	if err != nil {
		if err.Error() != "no predecessor" {
			panic(err)
		}
	}

	if x != nil && util.Between(x.ID(), c.ID(), c.successor.ID()) {
		c.successor = x
		log.Printf("%s [%d]: Stabilized successor %d", c.Addr(), c.ID(), c.successor.ID())
		//n.successor.Notify(n)
	}

	if c.successor.ID() != c.ID() {
		log.Printf("%s [%d]: Notified successor %d", c.Addr(), c.ID(), c.successor.ID())
		err = c.successor.Notify(context.Background(), c)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (c *Chord) CheckPredecessor() {
	if c.predecessor != nil {
		// TODO: Better to have retries with a backoff policy to avoid temporary failures/false alarms
		//err := c.predecessor.Healthz()
		//if err != nil {
		//	log.Printf("%s [%d]: predecessor { %d: %s } not healthy", n.Addr, n.Id, n.Predecessor.Id, n.Predecessor.Addr)
		//}
	}
}

func (c *Chord) FixFinger(fingerNumber int) error {
	if fingerNumber < 0 {
		return errors.New("cannot be less than 0")
	}
	if fingerNumber > util.M {
		return errors.New(fmt.Sprintf("cannot exceed %d", util.M))
	}

	fingerIndex := fingerNumber - 1

	fId := (int(c.ID()) + int(math.Pow(2, float64(fingerNumber-1)))) % int(math.Pow(2, util.M))

	var err error
	c.finger[fingerIndex], err = c.FindSuccessor(context.Background(), uint64(fId))
	if err != nil {
		return err
	}
	//c.fingerIdx[fingerIndex] = uint64(fId)

	if c.finger[fingerIndex] != nil {
		log.Printf("%s [%d]: Finger resolved { Index: %d, id: %d, successor: %d }", c.Addr(), c.ID(), fingerIndex, fId, c.finger[fingerIndex].ID())
	}

	return err
}
