package stcp

import (
	"github.com/sjy-dv/scdb/scdb/server/stcp/client"
	"github.com/sjy-dv/scdb/scdb/server/stcp/pool"
	"github.com/sjy-dv/scdb/scdb/server/stcp/server"
	"github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

type service struct {
	opts   tcpcore.Options
	server Server
	client Client
	pool   Pool
}

func newService(opts ...tcpcore.Option) Service {
	s := &service{
		opts: tcpcore.DefaultOptions(),
	}
	for _, opt := range opts {
		opt(&s.opts)
	}
	s.init()
	return s
}

func (s *service) init() {
	if s.opts.ServiceType.TCPServerType() {
		svr := server.NewServer()
		svr.WithOptions(s.opts)
		s.server = svr
	}
	if s.opts.ServiceType.TCPClientType() {
		c := client.NewClient()
		c.WithOptions(s.opts)
		s.client = c
	}
	if s.client == nil && s.opts.ServiceType.TCPAsyncClientType() {
		c := client.NewAsyncClient()
		c.WithOptions(s.opts)
		s.client = c
	}
	if s.opts.ServiceType.TCPPoolType() {
		p := pool.NewPool()
		p.WithOptions(s.opts)
		s.pool = p
	}
	if s.pool == nil && s.opts.ServiceType.TCPAsyncPoolType() {
		p := pool.NewAsyncPool()
		p.WithOptions(s.opts)
		s.pool = p
	}
}

// Server returns the server
func (s *service) Server() Server {
	return s.server
}

// Client returns the client
func (s *service) Client() Client {
	return s.client
}

// Server returns the pool
func (s *service) Pool() Pool {
	return s.pool
}
