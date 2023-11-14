package server

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/server/stcp/internal"
	tcp "github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

type Conn struct {
	s         *Server
	conn      *net.TCPConn
	buffer    *internal.ReaderBuffer
	sendChan  chan []byte
	closeChan chan struct{}
	wwg       sync.WaitGroup
	rwg       sync.WaitGroup
	closed    int32
	tag       string
}

func newConn(ctx context.Context, s *Server, conn *net.TCPConn) *Conn {
	c := &Conn{
		s:         s,
		conn:      conn,
		sendChan:  make(chan []byte, 100),
		closeChan: make(chan struct{}),
	}
	c.buffer = internal.NewReaderBuffer(c.conn, int(s.opts.InitReadBufLen), int(s.opts.MaxReadBufLen))
	c.wwg.Add(1)
	go c.handleWriteLoop(ctx)
	c.rwg.Add(1)
	go c.handleReadLoop(ctx)
	return c
}

func (c *Conn) Init(opts ...tcp.Option) error {
	return nil
}

func (c *Conn) Read(buf []byte) (n int, err error) {
	return 0, tcp.ErrConnInvalidCall
}

func (c *Conn) ReadFull(buf []byte) (n int, err error) {
	return 0, tcp.ErrConnInvalidCall
}

func (c *Conn) WriteRead(req []byte) (body []byte, err error) {
	return nil, tcp.ErrConnInvalidCall
}

func (c *Conn) Write(data []byte) error {
	if len(data) > 0 {
		select {
		case <-c.closeChan:
			return tcp.ErrConnClosed
		case c.sendChan <- data:
		}
	}
	return nil
}

func (c *Conn) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	close(c.closeChan)
	c.wwg.Wait()
	for len(c.sendChan) > 0 {
		data := <-c.sendChan
		if err := c.write(data); err != nil {
			c.s.opts.Handler.OnWriteError(c, data, err)
		}
	}
	err = c.conn.Close()
	c.rwg.Wait()
	c.buffer.Release()
	c.s.opts.Handler.OnClosed(c)
	c.s.onConnClose()
	c.s = nil
	return
}

func (c *Conn) Closed() bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return true
	}
	return false
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetTag(tag string) {
	c.tag = tag
}

func (c *Conn) GetTag() string {
	return c.tag
}

func (c *Conn) getReadDeadLine() (t time.Time) {
	if c.s.opts.ReadTimeout > 0 {
		t = time.Now().Add(c.s.opts.ReadTimeout)
	}
	return
}

func (c *Conn) getWriteDeadLine() (t time.Time) {
	if c.s.opts.WriteTimeout > 0 {
		t = time.Now().Add(c.s.opts.WriteTimeout)
	}
	return
}

func (c *Conn) handleReadLoop(ctx context.Context) {
	defer func() {
		c.rwg.Done()
		c.Close()
	}()

	h := c.s.opts.Handler
	h.OnOpened(c)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeChan:
			return
		default:
		}

		if err := c.conn.SetReadDeadline(c.getReadDeadLine()); err != nil {
			log.Warnf("TCP conn SetReadDeadline error:[%v]", err)
		}
		if _, err := c.buffer.ReadFromReader(); err != nil {
			select {
			case <-c.closeChan:
				return
			default:
			}
			if err != io.EOF {
				log.Debugf("TCP conn read error:[%v]", err)
			}
			return
		}
		for c.buffer.Len() > 0 {
			bodyLen, headerLen := c.s.opts.HeaderCodec.Decode(c.buffer.Data())
			if headerLen == 0 {
				break
			}
			msgLen := bodyLen + headerLen
			if msgLen > c.s.opts.MaxReadBufLen {
				log.Errorf("msg len:%d greater than max:%d", msgLen, c.s.opts.MaxReadBufLen)
				return
			}
			if uint32(c.buffer.Len()) < msgLen {
				break
			}
			buf := make([]byte, bodyLen)
			c.buffer.Read(int(headerLen), int(bodyLen), buf)
			if bodyLen == c.s.heartLen {
				if c.s.isHeartBeat(buf) {
					_ = c.Write(buf)
					continue
				}
			}
			if err := h.OnReadMsg(c, buf); err != nil {
				log.Infof("TcpConn OnReadMsg error:[%v]", err)
				return
			}
		}
	}
}

func (c *Conn) handleWriteLoop(ctx context.Context) {
	defer func() {
		c.wwg.Done()
		c.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeChan:
			return
		case data, ok := <-c.sendChan:
			if !ok {
				return
			}
			if err := c.write(data); err != nil {
				c.s.opts.Handler.OnWriteError(c, data, err)
				return
			}
		}
	}
}

func (c *Conn) write(data []byte) (err error) {
	data = c.s.opts.HeaderCodec.Encode(data)
	_ = c.conn.SetWriteDeadline(c.getWriteDeadLine())
	_, err = c.conn.Write(data)
	return
}
