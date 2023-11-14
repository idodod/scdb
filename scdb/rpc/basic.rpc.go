package rpc

import (
	"context"

	"github.com/sjy-dv/scdb/scdb/pkg/protobuf/scdbpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (internal *InternalRpcServer) Ping(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (internal *InternalRpcServer) Save(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	err := internal.SCDB.Save(in.Key, in.Value)
	if err != nil {
		return &scdbpb.Result{
			Error:       err.Error(),
			ErrorStatus: true,
		}, nil
	}
	return &scdbpb.Result{ErrorStatus: false}, nil
}

func (internal *InternalRpcServer) Get(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	val, err := internal.SCDB.Get(in.Key)
	if err != nil {
		return &scdbpb.Result{Error: err.Error(), ErrorStatus: true}, nil
	}
	return &scdbpb.Result{Value: val, ErrorStatus: false}, nil
}

func (internal *InternalRpcServer) Del(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	err := internal.SCDB.Delete(in.Key)
	if err != nil {
		return &scdbpb.Result{Error: err.Error(), ErrorStatus: true}, nil
	}
	return &scdbpb.Result{ErrorStatus: false}, nil
}
