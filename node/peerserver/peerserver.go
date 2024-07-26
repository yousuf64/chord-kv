package peerserver

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/node/transport"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
)

type PeerServer struct {
	transport.UnimplementedPeerServer

	chord chord.ChordNode
}

func New(chord chord.ChordNode) *PeerServer {
	return &PeerServer{chord: chord}
}

func (ps *PeerServer) FindSuccessor(ctx context.Context, request *transport.FindSuccessorRequest) (*transport.FindSuccessorReply, error) {
	successor, err := ps.chord.FindSuccessor(ctx, request.Id)
	if err != nil {
		return nil, err
	}

	return &transport.FindSuccessorReply{Address: successor.Addr()}, nil
}

func (ps *PeerServer) SetSuccessor(ctx context.Context, request *transport.SetSuccessorRequest) (*emptypb.Empty, error) {
	err := ps.chord.SetSuccessor(ctx, node.NewRemoteNode(request.Address))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ps *PeerServer) SetPredecessor(ctx context.Context, request *transport.SetPredecessorRequest) (*emptypb.Empty, error) {
	err := ps.chord.SetPredecessor(ctx, node.NewRemoteNode(request.Address))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ps *PeerServer) Notify(ctx context.Context, request *transport.NotifyRequest) (*transport.NotifyReply, error) {
	insert, err := ps.chord.Notify(ctx, node.NewRemoteNode(request.Address))
	if err != nil {
		return nil, err
	}

	reply := &transport.NotifyReply{
		Items: make([]*transport.InsertItem, 0, len(insert)),
	}

	for _, item := range insert {
		reply.Items = append(reply.Items, &transport.InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return reply, nil
}

func (ps *PeerServer) GetPredecessor(ctx context.Context, _ *emptypb.Empty) (*transport.GetPredecessorReply, error) {
	predecessor, err := ps.chord.GetPredecessor(ctx)
	if err != nil {
		return nil, err
	}
	return &transport.GetPredecessorReply{Address: predecessor.Addr()}, nil
}

func (ps *PeerServer) Insert(ctx context.Context, request *transport.InsertRequest) (*emptypb.Empty, error) {
	items := make([]node.InsertItem, 0, len(request.Items))
	for _, item := range request.Items {
		items = append(items, node.InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	err := ps.chord.InsertBatch(ctx, items...)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ps *PeerServer) Query(ctx context.Context, request *transport.QueryRequest) (*transport.QueryReply, error) {
	reply, err := ps.chord.Query(ctx, request.GetIndex(), request.GetQuery())
	if err != nil {
		return nil, err
	}

	return &transport.QueryReply{Value: reply}, nil
}

func (ps *PeerServer) Leave(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return nil, err
	}

	// Send SIGINT to the current process
	err = p.Signal(os.Interrupt)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
	//err := ps.chord.Leave(ctx)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return &emptypb.Empty{}, nil
}

func (ps *PeerServer) Healthz(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	err := ps.chord.Healthz(ctx)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
