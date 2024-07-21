package kv

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/node"
	"strings"
)

type KV interface {
	Insert(ctx context.Context, key string, value string) error
}

type DistributedKV struct {
	c *chord.Chord
}

func NewDistributedKV(chord *chord.Chord) *DistributedKV {
	return &DistributedKV{chord}
}

// Insert inserts the KV pair to the correct node.
// When having multiple words in the key, it indexes by each word and stores in the correct nodes to facilitate part querying.
func (d *DistributedKV) Insert(ctx context.Context, key string, value string) error {
	key = strings.ToLower(key)
	split := strings.Split(key, " ")
	// TODO: Might need to ignore repeated words... also trim spaces

	vals := make([]node.InsertItem, 0, len(split))
	for _, token := range split {
		vals = append(vals, node.InsertItem{
			Index: token,
			Key:   key,
			Value: value,
		})
	}

	err := d.c.InsertBatch(ctx, vals...)
	if err != nil {
		return err
	}
	return nil
}
