package tcpcore

import "net"

type Conn interface {
	// Init initiates Conn with options
	Init(opts ...Option) error
	// Read reads data from the connection, only for sync Client.
	Read(buf []byte) (n int, err error)
	// ReadFull reads exactly len(buf) bytes from Conn into buf, only for sync Client.
	// It returns the number of bytes copied and an error if fewer bytes were read.
	// On return, n == len(buf) if and only if err == nil.
	ReadFull(buf []byte) (n int, err error)
	// WriteRead writes the request and reads the response, only for sync Client.
	// HeaderCodec(in Options) is used
	// returning msg body, without header
	WriteRead(req []byte) (body []byte, err error)
	// Write writes data to the connection.
	Write(data []byte) error
	// Close closes the connection.
	Close() error
	// Closed
	Closed() bool
	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr
	// SetTag sets a tag to Conn
	SetTag(tag string)
	// GetTag gets the tag
	GetTag() string
}
