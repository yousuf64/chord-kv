package remote

import (
	"context"
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/remote/transport"
	"github.com/yousuf64/chord-kv/util"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RemoteNode struct {
	id     uint64
	addr   string
	client transport.PeerClient
}

func NewRemoteNode(addr string) *RemoteNode {
	client, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithPropagators(propagation.TraceContext{}))),
	)

	if err != nil {
		panic(err)
	}

	return &RemoteNode{
		id:     util.Hash(addr),
		addr:   addr,
		client: transport.NewPeerClient(client),
	}
}

func (r *RemoteNode) InsertBatch(ctx context.Context, items ...node.InsertItem) error {
	req := &transport.InsertRequest{
		Items: make([]*transport.InsertItem, 0, len(items)),
	}

	for _, item := range items {
		req.Items = append(req.Items, &transport.InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	_, err := r.client.Insert(ctx, req)
	if err != nil {
		st, _ := status.FromError(err)
		if st != nil {
			err = fmt.Errorf(st.Message())
			if err.Error() == errs.AlreadyExistsError.Error() {
				err = errs.AlreadyExistsError
			}
		}
	}
	return nil
}

func (r *RemoteNode) Query(ctx context.Context, index string, query string) (string, error) {
	req := &transport.QueryRequest{
		Index: index,
		Query: query,
	}

	reply, err := r.client.Query(ctx, req)
	if err != nil {
		st, _ := status.FromError(err)
		if st != nil {
			err = fmt.Errorf(st.Message())
			if err.Error() == errs.NotFoundError.Error() {
				err = errs.NotFoundError
			}
		}

		return "", err
	}

	return reply.Value, nil
}

func (r *RemoteNode) ID() uint64 {
	return r.id
}

func (r *RemoteNode) Addr() string {
	return r.addr
}

func (r *RemoteNode) FindSuccessor(ctx context.Context, id uint64) (node.Node, error) {
	reply, err := r.client.FindSuccessor(ctx, &transport.FindSuccessorRequest{Id: id})
	if err != nil {
		return nil, err
	}

	if reply.Address == "" {
		return nil, errors.New("not found")
	}

	return NewRemoteNode(reply.Address), nil
}

func (r *RemoteNode) SetSuccessor(ctx context.Context, successor node.Node) error {
	_, err := r.client.SetSuccessor(ctx, &transport.SetSuccessorRequest{Address: successor.Addr()})
	if err != nil {
		return err
	}

	return nil
}

func (r *RemoteNode) SetPredecessor(ctx context.Context, predecessor node.Node) error {
	_, err := r.client.SetPredecessor(ctx, &transport.SetPredecessorRequest{Address: predecessor.Addr()})
	if err != nil {
		return err
	}

	return nil
}

func (r *RemoteNode) Notify(ctx context.Context, p node.Node) ([]node.InsertItem, error) {
	reply, err := r.client.Notify(ctx, &transport.NotifyRequest{Address: p.Addr()})
	if err != nil {
		return nil, err
	}

	insert := make([]node.InsertItem, 0, len(reply.Items))
	for _, item := range reply.Items {
		insert = append(insert, node.InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return insert, nil
}

func (r *RemoteNode) GetPredecessor(ctx context.Context) (node.Node, error) {
	reply, err := r.client.GetPredecessor(ctx, &emptypb.Empty{})
	if err != nil {
		st, _ := status.FromError(err)
		return nil, fmt.Errorf(st.Message())
	}
	return NewRemoteNode(reply.Address), nil
}

func (r *RemoteNode) Healthz(ctx context.Context) error {
	_, err := r.client.Healthz(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	return nil
}
