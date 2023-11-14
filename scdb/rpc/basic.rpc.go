package rpc

import (
	"context"

	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/pkg/protobuf/scdbpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (internal *InternalRpcServer) Ping(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	log.Debug("SCDB Established New Connection")
	return &emptypb.Empty{}, nil
}

func (internal *InternalRpcServer) Save(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	log.Info("Trying to Save New Key")
	err := internal.SCDB.Save(in.Key, in.Value)
	if err != nil {
		log.Warn("Failed to Save Key")
		return &scdbpb.Result{
			Error:       err.Error(),
			ErrorStatus: true,
		}, nil
	}
	log.Info("Succeeded to Save")
	return &scdbpb.Result{ErrorStatus: false}, nil
}

func (internal *InternalRpcServer) Get(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	log.Info("Trying to Get Data Btree Algo")
	val, err := internal.SCDB.Get(in.Key)
	if err != nil {
		log.Warn("Failed to Search Key")
		return &scdbpb.Result{Error: err.Error(), ErrorStatus: true}, nil
	}
	log.Info("Succeeded to Search")
	return &scdbpb.Result{Value: val, ErrorStatus: false}, nil
}

func (internal *InternalRpcServer) Del(ctx context.Context, in *scdbpb.KV) (*scdbpb.Result, error) {
	log.Info("Trying to Delete Data")
	err := internal.SCDB.Delete(in.Key)
	if err != nil {
		log.Warn("Failed to Delete Key")
		return &scdbpb.Result{Error: err.Error(), ErrorStatus: true}, nil
	}
	log.Info("Succeeded to Delete")
	return &scdbpb.Result{ErrorStatus: false}, nil
}
