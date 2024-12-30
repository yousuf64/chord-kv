package bucketmap

import (
	"encoding/json"
	"fmt"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/util"
	"log"
	"strings"
	"sync"
)

type Item struct {
	Index string
	Key   string
	Value string
}

type item struct {
	Index  string   `json:"index"`
	SecIdx []string `json:"secondary_indexes"`
	Key    string   `json:"key"`
	Value  string   `json:"value"`
}

type bucket struct {
	lock          sync.RWMutex
	items         []item
	uniqueIndexes sync.Map
}

// BucketMap stores the key value pairs of nodes. Uses indexing for efficient querying.
type BucketMap struct {
	buckets sync.Map // NodeId -> [ { Index: 'hello', Key: 'hello world', 'foo' }, { Index: 'hello', Key: 'hello world', 'foo' } ]
}

func NewBucketMap() *BucketMap {
	return &BucketMap{
		buckets: sync.Map{},
	}
}

func (b *BucketMap) Add(nodeId uint64, insertItem Item) error {
	val, _ := b.buckets.LoadOrStore(nodeId, &bucket{
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
		return errs.AlreadyExistsError
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

// GetAndDeleteBetween returns all items of nodes with ID between lo and hi and drops the node buckets from the BucketMap.
func (b *BucketMap) GetAndDeleteBetween(nodeLo uint64, nodeHi uint64) []Item {
	items := make([]Item, 0)

	b.buckets.Range(func(key, value any) bool {
		if !util.Between(key.(uint64), nodeLo, nodeHi) {
			bkt := value.(*bucket)
			for _, it := range bkt.items {
				items = append(items, Item{
					Index: it.Index,
					Key:   it.Key,
					Value: it.Value,
				})
			}

			b.buckets.Delete(key)
		}

		return true
	})

	return items
}

// Query returns the value of the first item that matches the query.
// TODO: Maybe, mention how it only support subsequence matching.
func (b *BucketMap) Query(nodeId uint64, index string, query string) (string, bool) {
	value, ok := b.buckets.Load(nodeId)
	if !ok {
		return "", false
	}

	split := strings.Split(query, " ")

	bkt := value.(*bucket)
OuterLoop:
	for _, it := range bkt.items {
		if it.Index == index {
			z := 0
		Loop:
			for _, s := range split {
				for _, sidx := range it.SecIdx[z:] {
					z++
					if s == sidx {
						continue Loop
					}
				}
				continue OuterLoop
			}

			// Should have matched
			return it.Value, true
		}
	}

	return "", false
}

func (b *BucketMap) Snapshot() []Item {
	items := make([]Item, 0)

	b.buckets.Range(func(_, value any) bool {
		bkt := value.(*bucket)
		for _, it := range bkt.items {
			items = append(items, Item{
				Index: it.Index,
				Key:   it.Key,
				Value: it.Value,
			})
		}
		return true
	})

	return items
}

func (b *BucketMap) Debug() json.RawMessage {
	type debugBucket struct {
		NodeId        uint64   `json:"node_id"`
		Items         []item   `json:"items"`
		UniqueIndexes []string `json:"unique_indexes"`
	}

	var buckets []debugBucket
	b.buckets.Range(func(key, value any) bool {
		i := debugBucket{
			NodeId:        key.(uint64),
			Items:         value.(*bucket).items,
			UniqueIndexes: nil,
		}

		uq := make([]string, 0)
		value.(*bucket).uniqueIndexes.Range(func(key, value any) bool {
			uq = append(uq, key.(string))
			return true
		})
		i.UniqueIndexes = uq

		buckets = append(buckets, i)
		return true
	})

	v, err := json.Marshal(buckets)
	if err != nil {
		panic(err)
	}

	return v
}
