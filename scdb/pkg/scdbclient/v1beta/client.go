package v1beta

import (
	"context"
	"errors"

	"github.com/sjy-dv/scdb/scdb/pkg/protobuf/scdbpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Conn struct {
	grpcclient *grpc.ClientConn
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
func (c *Conn) Save(ctx context.Context, key, value []byte) error {
	retval, err := c.wrap().Save(ctx, &scdbpb.KV{Key: key, Value: value})
	if err != nil {
		return err
	}
	if retval.ErrorStatus {
		return errors.New(retval.Error)
	}
	return nil
}

// Based on the key, it retrieves the value. If the retrieval fails, it returns an error, and if successful, it returns the value associated with the key.
func (c *Conn) Get(ctx context.Context, key []byte) ([]byte, error) {
	retval, err := c.wrap().Get(ctx, &scdbpb.KV{Key: key})
	if err != nil {
		return nil, err
	}
	if retval.ErrorStatus {
		return nil, errors.New(retval.Error)
	}
	return retval.GetValue(), nil
}

// Deletes data based on the key. If successful, it returns nil, and if it fails, it returns an error.
func (c *Conn) Del(ctx context.Context, key []byte) error {
	retval, err := c.wrap().Del(ctx, &scdbpb.KV{Key: key})
	if err != nil {
		return err
	}
	if retval.ErrorStatus {
		return errors.New(retval.Error)
	}
	return nil
}

// Closes the connection.
func (c *Conn) Close() error {
	return c.grpcclient.Close()
}
