package bucketmap

import (
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/node"
	"log"
	"strings"
	"sync"
)

type item struct {
	Index  string
	SecIdx []string
	Key    string
	Value  string
}

type bucket struct {
	lock          sync.RWMutex
	items         []item
	uniqueIndexes sync.Map
}

type BucketMap struct {
	buckets sync.Map // NodeId -> [ { Index: 'hello', Key: 'hello world', 'foo' }, { Index: 'hello', Key: 'hello world', 'foo' } ]
}

func NewBucketMap() *BucketMap {
	return &BucketMap{
		buckets: sync.Map{},
	}
}

func (b *BucketMap) Add(bucketId uint64, insertItem node.InsertItem) error {
	val, _ := b.buckets.LoadOrStore(bucketId, &bucket{
		lock:  sync.RWMutex{},
		items: make([]item, 0),
	})

	bkt := val.(*bucket)
	bkt.lock.Lock()
	defer bkt.lock.Unlock()

	uqIdx := fmt.Sprintf("%s/%s", insertItem.Index, insertItem.Key)
	_, exists := bkt.uniqueIndexes.LoadOrStore(uqIdx, struct{}{})
	if exists {
		log.Println("already have item", insertItem.Key)
		return errors.New(fmt.Sprintf("item %s already exists", insertItem.Key))
	}

	secIdx := strings.Split(insertItem.Key, " ")
	bkt.items = append(bkt.items, item{
		Index:  insertItem.Index,
		SecIdx: secIdx,
		Key:    insertItem.Key,
		Value:  insertItem.Value,
	})

	return nil
}
