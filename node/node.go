package node

import (
	"context"
	"errors"
	"github.com/yousuf64/chord-kv/node/transport"
	"github.com/yousuf64/chord-kv/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Node interface {
	ID() uint64
	Addr() string
	FindSuccessor(ctx context.Context, id uint64) (Node, error)
	Notify(ctx context.Context, pn Node) error
	GetPredecessor(ctx context.Context) (Node, error)
}

type RemoteNode struct {
	id     uint64
	addr   string
	client transport.PeerClient
}

func NewRemoteNode(addr string) *RemoteNode {
	client, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	return &RemoteNode{
		id:     util.Hash(addr),
		addr:   addr,
		client: transport.NewPeerClient(client),
	}
}

func (r *RemoteNode) ID() uint64 {
	return r.id
}

func (r *RemoteNode) Addr() string {
	return r.addr
}

func (r *RemoteNode) FindSuccessor(ctx context.Context, id uint64) (Node, error) {
	reply, err := r.client.FindSuccessor(ctx, &transport.FindSuccessorRequest{Id: id})
	if err != nil {
		return nil, err
	}

	if reply.Address == "" {
		return nil, errors.New("not found")
	}

	return NewRemoteNode(reply.Address), nil
}

func (r *RemoteNode) Notify(ctx context.Context, p Node) error {
	_, err := r.client.Notify(ctx, &transport.NotifyRequest{Address: p.Addr()})
	if err != nil {
		return err
	}

	return nil
}

func (r *RemoteNode) GetPredecessor(ctx context.Context) (Node, error) {
	reply, err := r.client.GetPredecessor(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	return NewRemoteNode(reply.Address), nil
}
