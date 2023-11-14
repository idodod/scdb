package tcpcore

type Server interface {
	// Init initiates server with options
	Init(opts ...Option) error
	// Serve starts handling events for Server.
	Serve() error
	// Stop can stop the service whenever you want to
	// it is also called automatically when an interrupt signal arrives
	Stop()
	// ConnNum returns the number of currently active connections
	ConnNum() uint32
}
