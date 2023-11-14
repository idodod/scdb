package pool

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sjy-dv/scdb/scdb/pkg/limiter"
	"github.com/sjy-dv/scdb/scdb/server/stcp/client"
	"github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

var _ tcpcore.Pool = &Pool{}

type poolConn struct {
	conn tcpcore.Conn
	t    int64
}

type Pool struct {
	opts      tcpcore.Options
	factory   func() (tcpcore.Conn, error)
	connChan  chan *poolConn
	closeChan chan struct{}
	limiter   limiter.Limiter
	closed    int32
}

func NewPool() *Pool {
	return &Pool{}
}

func (p *Pool) WithOptions(opts tcpcore.Options) {
	p.opts = opts
}

func (p *Pool) Init(opts ...tcpcore.Option) error {
	for _, opt := range opts {
		opt(&p.opts)
	}
	if p.opts.Addr == "" {
		return tcpcore.ErrPoolInvalidAddr
	}
	if p.opts.PoolMaxSize == 0 {
		p.opts.PoolMaxSize = 16
	}
	p.factory = func() (tcpcore.Conn, error) {
		c := client.NewClient()
		c.WithOptions(p.opts)
		if err := c.Init(); err != nil {
			return nil, err
		}
		return c, nil
	}
	p.connChan = make(chan *poolConn, p.opts.PoolMaxSize)
	p.closeChan = make(chan struct{})
	p.limiter = limiter.NewTimeoutLimiter(p.opts.PoolMaxSize, p.opts.PoolGetTimeout)
	go func() {
		select {
		case <-p.opts.Ctx.Done():
		case <-p.closeChan:
		}
		p.Close()
	}()
	for i := 0; i < int(p.opts.PoolInitSize); i++ {
		conn, err := p.createConn()
		if err != nil {
			p.Close()
			return fmt.Errorf("pool:%w", err)
		}
		p.connChan <- &poolConn{
			conn: conn,
			t:    time.Now().UnixNano(),
		}
	}
	return nil
}

func (p *Pool) createConn() (tcpcore.Conn, error) {
	conn, err := p.factory()
	if err != nil {
		return nil, err
	}
	conn.SetTag(p.opts.Tag)
	return conn, nil
}

func (p *Pool) Get() (conn tcpcore.Conn, err error) {
	if !p.limiter.Allow() {
		return nil, tcpcore.ErrPoolTimeout
	}
	defer func() {
		if err != nil {
			p.limiter.Revert()
		}
	}()

	for {
		select {
		case <-p.closeChan:
			return nil, tcpcore.ErrPoolClosed
		default:
		}

		select {
		case pc, ok := <-p.connChan:
			if !ok {
				return nil, tcpcore.ErrPoolClosed
			}
			if pc.conn.Closed() {
				continue
			}
			if p.opts.PoolIdleTimeout > 0 {
				if time.Now().UnixNano()-pc.t > p.opts.PoolIdleTimeout.Nanoseconds() {
					pc.conn.Close()
					continue
				}
			}
			if len(p.opts.HeartData) > 0 {
				if time.Now().UnixNano()-pc.t > p.opts.HeartInterval.Nanoseconds() {
					if _, err := pc.conn.WriteRead(p.opts.HeartData); err != nil {
						pc.conn.Close()
						continue
					}
				}
			}
			return pc.conn, nil
		default:
			return p.createConn()
		}
	}
}

func (p *Pool) Put(conn tcpcore.Conn) {
	if conn == nil {
		return
	}
	p.limiter.Revert()

	select {
	case <-p.closeChan:
		return
	default:
	}
	if conn.Closed() {
		return
	}
	p.connChan <- &poolConn{
		conn: conn,
		t:    time.Now().UnixNano(),
	}
}

func (p *Pool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	close(p.closeChan)
}
