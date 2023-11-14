package tcpcore

// connection pool
type Pool interface {
	// Init initiates pool with options
	Init(opts ...Option) error
	// Get gets a Conn from the pool, creates an Conn if necessary,
	// removes it from the Pool, and returns it to the caller.
	Get() (conn Conn, err error)

	// Put adds conn to the pool.
	// The conn returned by Get should be passed to Put once and only once,
	// whether it's closed or not
	Put(conn Conn)

	// Close closes the pool and all connections in the pool
	Close()
}
