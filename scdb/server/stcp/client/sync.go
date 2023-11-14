package client

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/sjy-dv/scdb/scdb/server/stcp/internal"
	tcp "github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

var _ tcp.Conn = &Client{}

type Client struct {
	opts   tcp.Options
	conn   net.Conn
	buffer *internal.ReaderBuffer
	closed int32
	tag    string
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) WithOptions(opts tcp.Options) {
	c.opts = opts
}

func (c *Client) Init(opts ...tcp.Option) error {
	for _, opt := range opts {
		opt(&c.opts)
	}
	conn, err := net.Dial("tcp", c.opts.Addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.buffer = internal.NewReaderBuffer(c.conn, int(c.opts.InitReadBufLen), int(c.opts.MaxReadBufLen))
	return nil
}

// Read
func (c *Client) Read(buf []byte) (n int, err error) {
	_ = c.conn.SetReadDeadline(c.getReadDeadLine())
	return c.conn.Read(buf)
}

// ReadFull
// On return, n == len(buf) if and only if err == nil.
func (c *Client) ReadFull(buf []byte) (n int, err error) {
	_ = c.conn.SetReadDeadline(c.getReadDeadLine())
	return io.ReadFull(c.conn, buf)
}

// WriteRead using HeaderCodec
// returning msg body, without header
func (c *Client) WriteRead(data []byte) (body []byte, err error) {
	data = c.opts.HeaderCodec.Encode(data)
	_ = c.conn.SetWriteDeadline(c.getWriteDeadLine())
	if _, err := c.conn.Write(data); err != nil {
		return nil, fmt.Errorf("write:%w", err)
	}

	_ = c.conn.SetReadDeadline(c.getReadDeadLine())
	for {
		if _, err := c.buffer.ReadFromReader(); err != nil {
			return nil, fmt.Errorf("read:%w", err)
		}
		bodyLen, headerLen := c.opts.HeaderCodec.Decode(c.buffer.Data())
		if headerLen == 0 {
			continue
		}
		msgLen := bodyLen + headerLen
		if msgLen > c.opts.MaxReadBufLen {
			return nil, tcp.ErrTooLarge
		}
		if uint32(c.buffer.Len()) < msgLen {
			continue
		}
		buf := make([]byte, bodyLen)
		c.buffer.Read(int(headerLen), int(bodyLen), buf)
		return buf, nil
	}
}

// Write using HeaderCodec
func (c *Client) Write(data []byte) error {
	data = c.opts.HeaderCodec.Encode(data)
	_ = c.conn.SetWriteDeadline(c.getWriteDeadLine())
	if _, err := c.conn.Write(data); err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Closed() bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return true
	}
	return false
}

func (c *Client) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Client) SetTag(tag string) {
	c.tag = tag
}

func (c *Client) GetTag() string {
	return c.tag
}

func (c *Client) getReadDeadLine() (t time.Time) {
	if c.opts.ReadTimeout > 0 {
		t = time.Now().Add(c.opts.ReadTimeout)
	}
	return
}

func (c *Client) getWriteDeadLine() (t time.Time) {
	if c.opts.WriteTimeout > 0 {
		t = time.Now().Add(c.opts.WriteTimeout)
	}
	return
}
