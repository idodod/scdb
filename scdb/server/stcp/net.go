package stcp

import "github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"

type Server tcpcore.Server
type Client tcpcore.Conn
type Pool tcpcore.Pool

// Service is an interface that wraps the server, client and pool,
// for building and initialising services conveniently.
type Service interface {
	Server() Server
	Client() Client
	Pool() Pool
}

// NewService creates a new Service with options.
func NewService(opts ...tcpcore.Option) Service {
	return newService(opts...)
}
