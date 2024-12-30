package chord

import (
	"context"
	"github.com/yousuf64/chord-kv/chord/intercom"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
)

// IntercomServer is the gRPC server implementation for the Intercom service.
type IntercomServer struct {
	intercom.UnsafeIntercomServer

	chord ChordNode
}

func New(chord ChordNode) *IntercomServer {
	return &IntercomServer{chord: chord}
}

func (ps *IntercomServer) FindSuccessor(ctx context.Context, request *intercom.FindSuccessorRequest) (*intercom.FindSuccessorReply, error) {
	successor, err := ps.chord.FindSuccessor(ctx, request.Id)
	if err != nil {
		return nil, err
	}

	return &intercom.FindSuccessorReply{Address: successor.Addr()}, nil
}

func (ps *IntercomServer) SetSuccessor(ctx context.Context, request *intercom.SetSuccessorRequest) (*emptypb.Empty, error) {
	err := ps.chord.SetSuccessor(ctx, NewNodeClient(request.Address, ps.chord.Hasher()))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ps *IntercomServer) SetPredecessor(ctx context.Context, request *intercom.SetPredecessorRequest) (*emptypb.Empty, error) {
	err := ps.chord.SetPredecessor(ctx, NewNodeClient(request.Address, ps.chord.Hasher()))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ps *IntercomServer) Notify(ctx context.Context, request *intercom.NotifyRequest) (*intercom.NotifyReply, error) {
	insert, err := ps.chord.Notify(ctx, NewNodeClient(request.Address, ps.chord.Hasher()))
	if err != nil {
		return nil, err
	}

	reply := &intercom.NotifyReply{
		Items: make([]*intercom.InsertItem, 0, len(insert)),
	}

	for _, item := range insert {
		reply.Items = append(reply.Items, &intercom.InsertItem{
			Index: item.Index,
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return reply, nil
}

func (ps *IntercomServer) GetPredecessor(ctx context.Context, _ *emptypb.Empty) (*intercom.GetPredecessorReply, error) {
	predecessor, err := ps.chord.GetPredecessor(ctx)
	if err != nil {
		return nil, err
	}
	return &intercom.GetPredecessorReply{Address: predecessor.Addr()}, nil
}

func (ps *IntercomServer) Insert(ctx context.Context, request *intercom.InsertRequest) (*emptypb.Empty, error) {
	items := make([]InsertItem, 0, len(request.Items))
	for _, item := range request.Items {
		items = append(items, InsertItem{
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

func (ps *IntercomServer) Query(ctx context.Context, request *intercom.QueryRequest) (*intercom.QueryReply, error) {
	reply, err := ps.chord.Query(ctx, request.GetIndex(), request.GetQuery())
	if err != nil {
		return nil, err
	}

	return &intercom.QueryReply{Value: reply}, nil
}

func (ps *IntercomServer) Leave(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
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

func (ps *IntercomServer) Healthz(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	err := ps.chord.Healthz(ctx)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
