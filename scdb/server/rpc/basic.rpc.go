package rpc

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/sjy-dv/scdb/scdb/core"
	"github.com/sjy-dv/scdb/scdb/pkg/log"
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

func (internal *InternalRpcServer) Transaction(stream scdbpb.Scdb_TransactionServer) error {
	var tx *core.Transaction
	var lockID string
	// defer func() {
	// 	if err := tx.Rollback(); err != nil {
	// 		log.Warn("transaction failed to lock-id %s", lockID)
	// 	}
	// }()
	for {
		txstream, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Warn(err)
		}
		if txstream.Begin {
			if tx != nil {
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: true,
					Error:       "transaction is aleady begin",
				}); err != nil {
					return err
				}
			}
			tx = internal.SCDB.NewTransaction(core.DefaultTxOptions)
			lockID = uuid.NewString()
			if err := stream.Send(&scdbpb.TxOut{
				ErrorStatus: false,
				TxId:        lockID,
			}); err != nil {
				return err
			}
		}
		if txstream.Do != "" {
			switch txstream.Do {
			case "SET":
				if txerr := tx.Save(txstream.Key, txstream.Value); txerr != nil {
					if err := stream.Send(&scdbpb.TxOut{
						ErrorStatus: true,
						TxId:        lockID,
						Error:       fmt.Sprintf("transaction set failed : %v", txerr),
					}); err != nil {
						return err
					}
				} else if txerr == nil {
					if err := stream.Send(&scdbpb.TxOut{
						ErrorStatus: false,
					}); err != nil {
						return err
					}
				}
				break
			case "GET":
				val, txerr := tx.Get(txstream.Key)
				if err != nil {
					if err := stream.Send(&scdbpb.TxOut{
						ErrorStatus: true,
						TxId:        lockID,
						Error:       fmt.Sprintf("transaction get failed : %v", txerr),
					}); err != nil {
						return err
					}
				}
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: false,
					Value:       val,
				}); err != nil {
					return err
				}
				break
			case "DEL":
				if txerr := tx.Delete(txstream.Key); txerr != nil {
					if err := stream.Send(&scdbpb.TxOut{
						ErrorStatus: true,
						TxId:        lockID,
						Error:       fmt.Sprintf("transaction del failed : %v", txerr),
					}); err != nil {
						return err
					}
				} else if txerr == nil {
					if err := stream.Send(&scdbpb.TxOut{
						ErrorStatus: false,
					}); err != nil {
						return err
					}
				}
				break
			default:
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: true,
					TxId:        lockID,
					Error:       "Not implemented Method",
				}); err != nil {
					return err
				}
				break
			}
		}
		if txstream.Rollback {
			if txerr := tx.Rollback(); txerr != nil {
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: true,
					TxId:        lockID,
					Error:       fmt.Sprintf("transaction rollback failed : %v", txerr),
				}); err != nil {
					return err
				}
			} else if txerr == nil {
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: false,
				}); err != nil {
					return err
				}
			}
		}
		if txstream.Commit {
			if txerr := tx.Commit(); txerr != nil {
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: false,
					TxId:        lockID,
					Error:       fmt.Sprintf("transaction commit failed : %v", txerr),
				}); err != nil {
					return err
				}
			} else if txerr == nil {
				if err := stream.Send(&scdbpb.TxOut{
					ErrorStatus: false,
				}); err != nil {
					return err
				}
			}
		}

	}
}
