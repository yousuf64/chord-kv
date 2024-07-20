package store

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
)

type Store interface {
	chord.Core

	Insert(ctx context.Context) error
	Lookup(ctx context.Context, key string) (string, error)
}
