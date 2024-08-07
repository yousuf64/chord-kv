package chord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/chord/bucketmap"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/util"
	"log"
	"math"
	"sync"
	"time"
)

type ChordNode interface {
	node.Node

	Join(ctx context.Context, n node.Node) error
	Leave(ctx context.Context) error
	Stabilize() error
	CheckPredecessor()
	FixFinger(fingerNumber int) error

	// DEBUG
	Debug() string
}

type Chord struct {
	id              uint64
	addr            string
	successor       node.Node
	predecessor     node.Node
	finger          []node.Node
	fingerIdx       []uint64
	bm              *bucketmap.BucketMap
	stopChan        chan struct{}
	wg              sync.WaitGroup
	successorLock   sync.Mutex
	predecessorLock sync.Mutex
}

func NewChord(addr string) *Chord {
	c := &Chord{
		id:              util.Hash(addr),
		addr:            addr,
		successor:       nil,
		predecessor:     nil,
		finger:          make([]node.Node, util.M),
		fingerIdx:       make([]uint64, util.M),
		bm:              bucketmap.NewBucketMap(),
		stopChan:        make(chan struct{}),
		wg:              sync.WaitGroup{},
		successorLock:   sync.Mutex{},
		predecessorLock: sync.Mutex{},
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

func (c *Chord) SetSuccessor(ctx context.Context, successor node.Node) error {
	c.successorLock.Lock()
	defer c.successorLock.Unlock()

	if successor.ID() == c.ID() {
		c.successor = c
		log.Println("SetSuccessor: successor set to myself")
	} else {
		c.successor = successor
		log.Println("SetSuccessor: successor set to", c.successor.ID())
	}

	return nil
}

func (c *Chord) SetPredecessor(ctx context.Context, predecessor node.Node) error {
	c.predecessorLock.Lock()
	defer c.predecessorLock.Unlock()

	if c.ID() == predecessor.ID() {
		if c.predecessor != nil {
			log.Printf("SetPredecessor: setting the predecessor from %d to <nil>\n", c.predecessor.ID())
			c.predecessor = nil
		}

		return nil
	}

	if c.predecessor != nil {
		log.Printf("SetPredecessor: setting the predecessor from %d to %d\n", c.predecessor.ID(), predecessor.ID())
	} else {
		log.Printf("SetPredecessor: setting the predecessor from <nil> to %d\n", predecessor.ID())
	}

	c.predecessor = predecessor
	return nil
}

func (c *Chord) closestPrecedingNode(id uint64) node.Node {
	for i := util.M - 1; i >= 0; i-- {
		if c.finger[i] != nil && util.Between(c.finger[i].ID(), c.ID(), id) {
			return c.finger[i]
		}
	}

	return c
}

// InsertBatch locally stores the items having the Index hash within the range of node's and its predecessor's ID.
// Forwards the rest of the items to the correct successor.
func (c *Chord) InsertBatch(ctx context.Context, items ...node.InsertItem) error {
	if len(items) == 0 {
		return nil
	}

	itemsById := map[uint64][]node.InsertItem{}
	for _, item := range items {
		id := util.Hash(item.Index)
		if _, ok := itemsById[id]; !ok {
			itemsById[id] = make([]node.InsertItem, 0)
		}

		itemsById[id] = append(itemsById[id], item)
	}

	for id, its := range itemsById {
		if c.predecessor != nil && util.Between(id, c.predecessor.ID(), c.ID()) {
			err := c.insertLocal(ctx, its)
			if err != nil {
				return err
			}
		} else {
			successor, err := c.FindSuccessor(ctx, id)
			if err != nil {
				return err
			}

			if successor.ID() == c.ID() {
				err = c.insertLocal(ctx, its)
				if err != nil {
					return err
				}
				continue
			}

			err = successor.InsertBatch(ctx, its...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Chord) Query(ctx context.Context, index string, query string) (string, error) {
	id := util.Hash(index)
	if c.predecessor != nil && util.Between(id, c.predecessor.ID(), c.ID()) {
		return c.queryLocal(id, index, query)
	} else {
		successor, err := c.FindSuccessor(ctx, id)
		if err != nil {
			return "", err
		}

		if successor.ID() == c.ID() {
			return c.queryLocal(id, index, query)
		}

		value, err := successor.Query(ctx, index, query)
		if err != nil {
			return "", err
		}
		return value, nil
	}
}

func (c *Chord) queryLocal(id uint64, index string, query string) (string, error) {
	value, ok := c.bm.Query(id, index, query)
	if !ok {
		return "", errs.NotFoundError
	}

	return value, nil
}

func (c *Chord) insertLocal(ctx context.Context, items []node.InsertItem) error {
	for _, item := range items {
		itemHash := util.Hash(item.Index)
		err := c.bm.
			Add(itemHash, item)
		if err != nil {
			return err
		}

		//li, ok := c.data[itemHash]
		//if !ok {
		//	c.data[itemHash] = make([]Item, 0)
		//	c.uqIdx[itemHash] = make(map[string]struct{})
		//
		//	li = c.data[itemHash]
		//}
		//
		//// Ignore if duplicate... TODO: Might need to throw an error
		////_, ok = c.uqIdx[itemHash][item.Key]
		////if ok {
		////	log.Println("already have item", item.Key)
		////	continue
		////}
		//
		//secIdx := strings.Split(item.Key, " ")
		//li = append(li, Item{
		//	Index:  item.Index,
		//	SecIdx: secIdx,
		//	Key:    item.Key,
		//	Value:  item.Value,
		//})
		//c.data[itemHash] = li
		//
		//// Add entry to unique index
		//c.uqIdx[itemHash][item.Key] = struct{}{}
	}

	return nil
}

func (c *Chord) Notify(ctx context.Context, p node.Node) ([]node.InsertItem, error) {
	c.predecessorLock.Lock()
	defer c.predecessorLock.Unlock()

	//if p.ID() != c.ID() && (c.predecessor == nil || c.predecessor.ID() != p.ID()) {
	if c.predecessor == nil || (util.Between(p.ID(), c.predecessor.ID(), c.ID()) && p.ID() != c.ID()) {
		// transfer data having <= p.ID()

		// TODO: Transfer data
		if c.predecessor != nil {
			log.Printf("Notify: setting the predecessor from %d to %d\n", c.predecessor.ID(), p.ID())
		} else {
			log.Printf("Notify: setting the predecessor from <nil> to %d\n", p.ID())
		}
		c.predecessor = p

		items := c.bm.GetAndDeleteLessThanEqual(c.predecessor.ID(), c.ID())
		insert := make([]node.InsertItem, 0, len(items))

		for _, item := range items {
			insert = append(insert, node.InsertItem{
				Index: item.Index,
				Key:   item.Key,
				Value: item.Value,
			})
		}

		return insert, nil
		//log.Printf("transferring %+v\n", insert)
		//if len(insert) > 0 {
		//	err := c.predecessor.InsertBatch(ctx, insert...)
		//	if err != nil {
		//		return err
		//	}
		//}

		//log.Printf("%s [%d]: (Notify) predecessor changed %d", c.Addr(), c.ID(), c.predecessor.ID())
	}

	return nil, nil
}

func (c *Chord) GetPredecessor(ctx context.Context) (node.Node, error) {
	if c.predecessor == nil {
		return nil, errors.New("no predecessor")
	}

	// TODO: was NewChord(c.predecessor.Addr(), nil)
	return c.predecessor, nil
}

func (c *Chord) Join(ctx context.Context, n node.Node) error {
	if n == nil {
		return nil
	}

	c.predecessorLock.Lock()
	defer c.predecessorLock.Unlock()

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

	if reply.ID() == c.ID() {
		return errors.New(fmt.Sprintf("node ID [%d] already taken", c.ID()))
	}

	c.successor = reply
	insert, err := c.successor.Notify(ctx, c)
	if err != nil {
		panic(err)
	}

	if len(insert) > 0 {
		err = c.insertLocal(ctx, insert)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Chord) Stabilize() error {
	// TODO: Maybe not when successor is myself
	c.successorLock.Lock()
	defer c.successorLock.Unlock()

	x, err := c.successor.GetPredecessor(context.Background())
	if err != nil {
		if err.Error() != "no predecessor" {
			return err
		}
	}

	if x != nil && util.Between(x.ID(), c.ID(), c.successor.ID()) {
		log.Printf("Stabilize: successor set from %d to %d\n", c.successor.ID(), x.ID())
		c.successor = x
		//log.Printf("%s [%d]: Stabilized successor %d", c.Addr(), c.ID(), c.successor.ID())
		//n.successor.Notify(n)
	}

	if c.successor.ID() != c.ID() {
		//log.Printf("%s [%d]: Notified successor %d", c.Addr(), c.ID(), c.successor.ID())
		insert, err := c.successor.Notify(context.Background(), c)
		if err != nil {
			// TODO
			return err
		}

		if len(insert) > 0 {
			err = c.insertLocal(context.Background(), insert)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Chord) CheckPredecessor() {
	c.predecessorLock.Lock()
	defer c.predecessorLock.Unlock()

	if c.predecessor != nil {
		// TODO: Better to have retries with a backoff policy to avoid temporary failures/false alarms
		// Attempt to perform a health check on the predecessor
		err := c.predecessor.Healthz(context.Background())
		if err != nil {
			// If the health check fails, set the predecessor to nil
			log.Println("health check failed, setting the predecessor to <nil>")
			c.predecessor = nil
		}
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

	fId := (int(c.ID()) + int(math.Pow(2, float64(fingerNumber-1)))) % int(math.Pow(2, float64(util.M)))

	var err error
	c.finger[fingerIndex], err = c.FindSuccessor(context.Background(), uint64(fId))
	if err != nil {
		return err
	}
	c.fingerIdx[fingerIndex] = uint64(fId)

	if c.finger[fingerIndex] != nil {
		//log.Printf("%s [%d]: Finger resolved { Index: %d, id: %d, successor: %d }", c.Addr(), c.ID(), fingerIndex, fId, c.finger[fingerIndex].ID())
	}

	return err
}

func (c *Chord) Leave(ctx context.Context) error {
	close(c.stopChan)
	c.wg.Wait()

	hasSuccessor := c.successor.ID() != c.ID()
	if hasSuccessor && c.predecessor != nil {
		// Set the predecessor of the successor node to the current node's predecessor
		err := c.successor.SetPredecessor(ctx, c.predecessor)
		if err != nil {
			return err
		}
	}

	if c.predecessor != nil {
		// Notify the predecessor to update its successor pointer
		err := c.predecessor.SetSuccessor(ctx, c.successor)
		if err != nil {
			return err
		}
	}

	// Transfer key-value data to the successor
	if hasSuccessor && c.successor.ID() != c.ID() {
		snapshot := c.bm.Snapshot()
		insert := make([]node.InsertItem, 0, len(snapshot))

		for _, item := range snapshot {
			insert = append(insert, node.InsertItem{
				Index: item.Index,
				Key:   item.Key,
				Value: item.Value,
			})
		}

		log.Printf("transferring %+v\n", insert)
		if len(insert) > 0 {
			err := c.successor.InsertBatch(ctx, insert...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Chord) StartJobs() {
	go func() {
		c.wg.Add(1)

		t := time.NewTicker(time.Millisecond * 100)

		for {
			select {
			case <-c.stopChan:
				t.Stop()
				c.wg.Done()
				log.Println("stopping stabilize job")
				return
			case <-t.C:
				err := c.Stabilize()
				if err != nil {
					//log.Println(err)
					//panic(err)
				}
			}
		}
	}()

	go func() {
		c.wg.Add(1)
		n := 1

		t := time.NewTicker(time.Millisecond * 150)
		for {
			select {
			case <-c.stopChan:
				t.Stop()
				c.wg.Done()
				log.Println("stopping fix finger job")
				return
			case <-t.C:
				if n > util.M {
					n = 1
				}

				err := c.FixFinger(n)
				if err != nil {
					//log.Println(err)
					//panic(err)
				}
				n++
			}
		}
	}()

	go func() {
		c.wg.Add(1)
		n := 1

		t := time.NewTicker(time.Millisecond * 250)
		for {
			select {
			case <-c.stopChan:
				t.Stop()
				c.wg.Done()
				log.Println("stopping check predecessor job")
				return
			case <-t.C:
				if n > util.M {
					n = 1
				}

				c.CheckPredecessor()
				n++
			}
		}
	}()
}

func (c *Chord) Healthz(ctx context.Context) error {
	return nil
}

func (c *Chord) Debug() string {
	type fingerNode struct {
		ID      uint64 `json:"id"`
		Address string `json:"address"`
	}

	data := struct {
		ID          uint64          `json:"id"`
		Address     string          `json:"address"`
		Successor   *fingerNode     `json:"successor"`
		Predecessor *fingerNode     `json:"predecessor"`
		FingerTable json.RawMessage `json:"finger_table"`
		Buckets     json.RawMessage `json:"buckets"`
	}{}

	fingerTable := map[uint64]fingerNode{}

	for i, idx := range c.fingerIdx {
		fingerTable[idx] = fingerNode{ID: c.finger[i].ID(), Address: c.finger[i].Addr()}
	}

	fingerTableJson, err := json.Marshal(fingerTable)
	if err != nil {
		return ""
	}

	data.ID = c.ID()
	data.Address = c.Addr()
	if c.successor != nil {
		data.Successor = &fingerNode{ID: c.successor.ID(), Address: c.successor.Addr()}
	}
	if c.predecessor != nil {
		data.Predecessor = &fingerNode{ID: c.predecessor.ID(), Address: c.predecessor.Addr()}
	}
	data.FingerTable = fingerTableJson
	data.Buckets = c.bm.Debug()

	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return ""
	}

	return string(result)
}
