package pool

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/sjy-dv/scdb/scdb/pkg/limiter"
	"github.com/sjy-dv/scdb/scdb/server/stcp/client"
	"github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

var _ tcpcore.Pool = &AsyncPool{}

type AsyncPool struct {
	opts      tcpcore.Options
	factory   func() (tcpcore.Conn, error)
	connChan  chan tcpcore.Conn
	closeChan chan struct{}
	limiter   limiter.Limiter
	cancel    context.CancelFunc
	closed    int32
}

func NewAsyncPool() *AsyncPool {
	return &AsyncPool{}
}

func (p *AsyncPool) WithOptions(opts tcpcore.Options) {
	p.opts = opts
}

func (p *AsyncPool) Init(opts ...tcpcore.Option) error {
	for _, opt := range opts {
		opt(&p.opts)
	}
	if p.opts.Addr == "" {
		return tcpcore.ErrPoolInvalidAddr
	}
	if p.opts.PoolMaxSize == 0 {
		p.opts.PoolMaxSize = 16
	}
	p.connChan = make(chan tcpcore.Conn, p.opts.PoolMaxSize)
	p.closeChan = make(chan struct{})
	p.limiter = limiter.NewTimeoutLimiter(p.opts.PoolMaxSize, p.opts.PoolGetTimeout)
	ctx, cancel := context.WithCancel(p.opts.Ctx)
	go func() {
		<-ctx.Done()
		p.Close()
	}()
	p.cancel = cancel
	p.opts.Ctx = ctx

	p.factory = func() (tcpcore.Conn, error) {
		c := client.NewAsyncClient()
		c.WithOptions(p.opts)
		if err := c.Init(); err != nil {
			return nil, err
		}
		return c, nil
	}
	for i := 0; i < int(p.opts.PoolInitSize); i++ {
		conn, err := p.createConn()
		if err != nil {
			p.Close()
			return fmt.Errorf("pool:%w", err)
		}
		p.connChan <- conn
	}
	return nil
}

func (p *AsyncPool) createConn() (tcpcore.Conn, error) {
	conn, err := p.factory()
	if err != nil {
		return nil, err
	}
	conn.SetTag(p.opts.Tag)
	return conn, nil
}

func (p *AsyncPool) Get() (conn tcpcore.Conn, err error) {
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
		case conn, ok := <-p.connChan:
			if !ok {
				return nil, tcpcore.ErrPoolClosed
			}
			if conn.Closed() {
				continue
			}
			return conn, nil
		default:
			return p.createConn()
		}
	}
}

func (p *AsyncPool) Put(conn tcpcore.Conn) {
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
	p.connChan <- conn
}

// Close
func (p *AsyncPool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	p.cancel()
	close(p.closeChan)
}
