package node

import "context"

type Node interface {
	ID() uint64
	Addr() string
	FindSuccessor(ctx context.Context, id uint64) (Node, error)
	SetSuccessor(ctx context.Context, successor Node) error
	SetPredecessor(ctx context.Context, predecessor Node) error
	Notify(ctx context.Context, pn Node) ([]InsertItem, error)
	GetPredecessor(ctx context.Context) (Node, error)
	Healthz(ctx context.Context) error

	InsertBatch(ctx context.Context, items ...InsertItem) error
	Query(ctx context.Context, index string, query string) (string, error)
}

type InsertItem struct {
	Index string
	Key   string
	Value string
}
