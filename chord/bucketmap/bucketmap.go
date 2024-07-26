package bucketmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/node"
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

func (b *BucketMap) GetAndDeleteLessThanEqual(lo uint64, hi uint64) []Item {
	items := make([]Item, 0)

	b.buckets.Range(func(key, value any) bool {
		if !util.Between(key.(uint64), lo, hi) {
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

func (b *BucketMap) Query(id uint64, index string, query string) (string, bool) {
	value, ok := b.buckets.Load(id)
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

func (b *BucketMap) Dump() string {
	var dump []struct {
		Id            uint64
		Items         []item
		UniqueIndexes []string
	}

	b.buckets.Range(func(key, value any) bool {
		i := struct {
			Id            uint64
			Items         []item
			UniqueIndexes []string
		}{
			Id:            key.(uint64),
			Items:         value.(*bucket).items,
			UniqueIndexes: nil,
		}

		uq := make([]string, 0)
		value.(*bucket).uniqueIndexes.Range(func(key, value any) bool {
			uq = append(uq, key.(string))
			return true
		})
		i.UniqueIndexes = uq

		dump = append(dump, i)
		return true
	})

	v, err := json.MarshalIndent(dump, "", "\t")
	if err != nil {
		panic(err)
	}

	return string(v)
}
