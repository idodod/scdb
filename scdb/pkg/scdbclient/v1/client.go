package v1

import (
	"context"
	"errors"

	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/pkg/protobuf/scdbpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Conn struct {
	grpcclient *grpc.ClientConn
}

type Transaction struct {
	txconn scdbpb.Scdb_TransactionClient
}

// Creates a connection to the scdb. address format host:port example 127.0.0.1:50051
func NewScdbConn(addr string) (*Conn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Conn{grpcclient: conn}, nil
}

func (c *Conn) wrap() scdbpb.ScdbClient {
	return scdbpb.NewScdbClient(c.grpcclient)
}

// Ping-Pong connection, when the connection is established returns nil. but when the connection is closed returns error
func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.wrap().Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	return nil
}

// Stores a value using a key and value. If the storage fails, it returns an error, and if successful, it returns nil.
func (c *Conn) Save(ctx context.Context, key string, value []byte) error {
	retval, err := c.wrap().Save(ctx, &scdbpb.KV{Key: []byte(key), Value: value})
	if err != nil {
		return err
	}
	if retval.ErrorStatus {
		return errors.New(retval.Error)
	}
	return nil
}

// Based on the key, it retrieves the value. If the retrieval fails, it returns an error, and if successful, it returns the value associated with the key.
func (c *Conn) Get(ctx context.Context, key string) ([]byte, error) {
	retval, err := c.wrap().Get(ctx, &scdbpb.KV{Key: []byte(key)})
	if err != nil {
		return nil, err
	}
	if retval.ErrorStatus {
		return nil, errors.New(retval.Error)
	}
	return retval.GetValue(), nil
}

// Deletes data based on the key. If successful, it returns nil, and if it fails, it returns an error.
func (c *Conn) Del(ctx context.Context, key string) error {
	retval, err := c.wrap().Del(ctx, &scdbpb.KV{Key: []byte(key)})
	if err != nil {
		return err
	}
	if retval.ErrorStatus {
		return errors.New(retval.Error)
	}
	return nil
}

// Begin SCDB transaction
func (c *Conn) Begin() *Transaction {
	stream, _ := c.wrap().Transaction(context.Background())
	return &Transaction{
		txconn: stream,
	}
}

// Based on the key, it retrieves the value. If the retrieval fails, it returns an error, and if successful, it returns the value associated with the key.
func (t *Transaction) Get(ctx context.Context, key string) ([]byte, error) {
	if err := t.txconn.Send(&scdbpb.TxIn{
		Do:  "GET",
		Key: []byte(key),
	}); err != nil {
		return nil, err
	}
	response, err := t.txconn.Recv()
	if err != nil {
		return nil, err
	}
	if response.ErrorStatus {
		return nil, errors.New(response.Error)
	}
	return response.GetValue(), nil
}

// Stores a value using a key and value. If the storage fails, it returns an error, and if successful, it returns nil.
func (t *Transaction) Save(ctx context.Context, key string, value []byte) error {
	if err := t.txconn.Send(&scdbpb.TxIn{
		Do:    "SET",
		Key:   []byte(key),
		Value: value,
	}); err != nil {
		return err
	}
	response, err := t.txconn.Recv()
	if err != nil {
		return err
	}
	if response.ErrorStatus {
		return errors.New(response.Error)
	}
	return nil
}

// Deletes data based on the key. If successful, it returns nil, and if it fails, it returns an error.
func (t *Transaction) Del(ctx context.Context, key string) error {
	if err := t.txconn.Send(&scdbpb.TxIn{
		Do:  "DEL",
		Key: []byte(key),
	}); err != nil {
		return err
	}
	response, err := t.txconn.Recv()
	if err != nil {
		return err
	}
	if response.ErrorStatus {
		return errors.New(response.Error)
	}
	return nil
}

// Commit SCDB transaction
func (t *Transaction) Commit() error {
	defer t.txconn.CloseSend()
	if err := t.txconn.Send(&scdbpb.TxIn{
		Commit: true,
	}); err != nil {
		return err
	}
	response, err := t.txconn.Recv()
	if err != nil {
		return err
	}
	if response.ErrorStatus {
		return errors.New(response.Error)
	}
	return nil
}

// Rollback SCDB transaction
func (t *Transaction) Rollback() {
	defer t.txconn.CloseSend()
	if err := t.txconn.Send(&scdbpb.TxIn{
		Rollback: true,
	}); err != nil {
		log.Errorf("scdb transaction rollback error: %v", err)
	}
	response, err := t.txconn.Recv()
	if err != nil {
		log.Errorf("scdb transaction rollback error: %v", err)
	}
	if response.ErrorStatus {
		log.Errorf("scdb transaction rollback error: %s", response.ErrorStatus)
	}
}

// Closes the connection.
func (c *Conn) Close() error {
	return c.grpcclient.Close()
}
