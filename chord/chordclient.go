package chord

import (
	"context"
	"errors"
	"fmt"
	"github.com/yousuf64/chord-kv/chord/intercom"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/util"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NodeClient represents a client stub for a node in the Chord network.
// It uses gRPC to communicate with the remote node, making network calls for most methods.
// The ID and Addr methods return values directly from the struct without making network calls.
type NodeClient struct {
	id     uint64
	addr   string
	client intercom.IntercomClient
}

func NewNodeClient(addr string) *NodeClient {
	client, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithPropagators(propagation.TraceContext{}))),
	)

	if err != nil {
		panic(err)
	}

	return &NodeClient{
		id:     util.Hash(addr),
		addr:   addr,
		client: intercom.NewIntercomClient(client),
	}
}

func (r *NodeClient) InsertBatch(ctx context.Context, items ...InsertItem) error {
	req := &intercom.InsertRequest{
		Items: make([]*intercom.InsertItem, 0, len(items)),
	}

	for _, item := range items {
		req.Items = append(req.Items, &intercom.InsertItem{
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

func (r *NodeClient) Query(ctx context.Context, index string, query string) (string, error) {
	req := &intercom.QueryRequest{
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

func (r *NodeClient) ID() uint64 {
	return r.id
}

func (r *NodeClient) Addr() string {
	return r.addr
}

func (r *NodeClient) FindSuccessor(ctx context.Context, id uint64) (Node, error) {
	reply, err := r.client.FindSuccessor(ctx, &intercom.FindSuccessorRequest{Id: id})
	if err != nil {
		return nil, err
	}

	if reply.Address == "" {
		return nil, errors.New("not found")
	}

	return NewNodeClient(reply.Address), nil
}

func (r *NodeClient) SetSuccessor(ctx context.Context, successor Node) error {
	_, err := r.client.SetSuccessor(ctx, &intercom.SetSuccessorRequest{Address: successor.Addr()})
	if err != nil {
		return err
	}

	return nil
}

func (r *NodeClient) SetPredecessor(ctx context.Context, predecessor Node) error {
	_, err := r.client.SetPredecessor(ctx, &intercom.SetPredecessorRequest{Address: predecessor.Addr()})
	if err != nil {
		return err
	}

	return nil
}

func (r *NodeClient) Notify(ctx context.Context, p Node) ([]InsertItem, error) {
	reply, err := r.client.Notify(ctx, &intercom.NotifyRequest{Address: p.Addr()})
	if err != nil {
		return nil, err
	}

	insert := make([]InsertItem, 0, len(reply.Items))
	for _, item := range reply.Items {
		insert = append(insert, InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return insert, nil
}

func (r *NodeClient) GetPredecessor(ctx context.Context) (Node, error) {
	reply, err := r.client.GetPredecessor(ctx, &emptypb.Empty{})
	if err != nil {
		st, _ := status.FromError(err)
		return nil, fmt.Errorf(st.Message())
	}
	return NewNodeClient(reply.Address), nil
}

func (r *NodeClient) Healthz(ctx context.Context) error {
	_, err := r.client.Healthz(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	return nil
}
