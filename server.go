package main

import (
	"context"
	"github.com/yousuf64/chord-kv/chord"
	"github.com/yousuf64/chord-kv/node"
	"github.com/yousuf64/chord-kv/peer"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	peer.UnimplementedPeerServer

	chord chord.Core
}

func NewServer(chord chord.Core) *Server {
	return &Server{chord: chord}
}

func (s *Server) FindSuccessor(ctx context.Context, request *peer.FindSuccessorRequest) (*peer.FindSuccessorReply, error) {
	successor, err := s.chord.FindSuccessor(ctx, request.Id)
	if err != nil {
		return nil, err
	}

	return &peer.FindSuccessorReply{Address: successor.Addr()}, nil
}

func (s *Server) Notify(ctx context.Context, request *peer.NotifyRequest) (*emptypb.Empty, error) {
	err := s.chord.Notify(ctx, node.NewRemoteNode(request.Address))
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetPredecessor(ctx context.Context, _ *emptypb.Empty) (*peer.GetPredecessorReply, error) {
	predecessor, err := s.chord.GetPredecessor(ctx)
	if err != nil {
		return nil, err
	}
	return &peer.GetPredecessorReply{Address: predecessor.Addr()}, nil
}
