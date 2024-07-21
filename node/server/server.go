package server

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/node/transport"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	transport.UnimplementedPeerServer

	chord chord.ChordNode
}

func New(chord chord.ChordNode) *Server {
	return &Server{chord: chord}
}

func (s *Server) FindSuccessor(ctx context.Context, request *transport.FindSuccessorRequest) (*transport.FindSuccessorReply, error) {
	successor, err := s.chord.FindSuccessor(ctx, request.Id)
	if err != nil {
		return nil, err
	}

	return &transport.FindSuccessorReply{Address: successor.Addr()}, nil
}

func (s *Server) Notify(ctx context.Context, request *transport.NotifyRequest) (*emptypb.Empty, error) {
	err := s.chord.Notify(ctx, node.NewRemoteNode(request.Address))
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetPredecessor(ctx context.Context, _ *emptypb.Empty) (*transport.GetPredecessorReply, error) {
	predecessor, err := s.chord.GetPredecessor(ctx)
	if err != nil {
		return nil, err
	}
	return &transport.GetPredecessorReply{Address: predecessor.Addr()}, nil
}
