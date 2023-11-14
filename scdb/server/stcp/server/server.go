package server

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sjy-dv/scdb/scdb/pkg/delay"
	"github.com/sjy-dv/scdb/scdb/pkg/limiter"
	"github.com/sjy-dv/scdb/scdb/pkg/log"
	tcp "github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

const DefaultAddr = "0.0.0.0:6727"

var _ tcp.Server = &Server{}

type Server struct {
	opts     tcp.Options
	listener net.Listener
	limiter  limiter.Limiter
	stopChan chan struct{}
	wg       sync.WaitGroup
	heartLen uint32
	connNum  uint32
	stopped  int32
}

func NewServer() *Server {
	return &Server{
		stopped: 1,
	}
}

func (s *Server) WithOptions(opts tcp.Options) {
	s.opts = opts
}

func (s *Server) Init(opts ...tcp.Option) error {
	for _, opt := range opts {
		opt(&s.opts)
	}
	if s.opts.Addr == "" {
		s.opts.Addr = DefaultAddr
	}
	l, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return err
	}
	s.listener = l
	if s.opts.ConnLimit > 0 {
		s.limiter = limiter.NewLimiter(s.opts.ConnLimit)
	}
	s.stopChan = make(chan struct{})
	s.heartLen = uint32(len(s.opts.HeartData))
	s.stopped = 0

	return nil
}

func (s *Server) Serve() error {
	if atomic.LoadInt32(&s.stopped) == 1 {
		return errors.New("server uninitialized")
	}
	s.wg.Add(1)
	go s.work()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-s.opts.Ctx.Done():
		s.Stop()
		return s.opts.Ctx.Err()
	case <-s.stopChan:
		s.wait()
	case sig := <-c:
		s.Stop()
		return errors.New("signal:" + sig.String())
	}
	return nil
}

func (s *Server) wait() {
	s.wg.Wait()
	for atomic.LoadUint32(&s.connNum) > 0 {
	}
}

func (s *Server) Stop() {
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return
	}
	close(s.stopChan)
	s.listener.Close()
	s.wait()
}

func (s *Server) ConnNum() uint32 {
	return atomic.LoadUint32(&s.connNum)
}

func (s *Server) onConnClose() {
	if s.limiter != nil {
		s.limiter.Revert()
	}
	atomic.AddUint32(&s.connNum, ^uint32(0))
}

// isHeartBeat called when len(data) == len(s.opts.HeartData)
func (s *Server) isHeartBeat(data []byte) bool {
	for i := 0; i < len(s.opts.HeartData); i++ {
		if s.opts.HeartData[i] != data[i] {
			return false
		}
	}
	return true
}

func (s *Server) work() {
	ctx, cancel := context.WithCancel(s.opts.Ctx)
	defer func() {
		cancel()
		s.wg.Done()
		s.Stop()
	}()

	td := delay.NewTempDelay(5*time.Millisecond, time.Second)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				d := td.GetDelay()
				log.Warnf("TCP server accept temporary error:[%v], delay:%v", ne, d)
				time.Sleep(d)
				continue
			}
			select {
			case <-s.stopChan:
				return
			default:
			}
			log.Errorf("TCP server accept error:[%v]", err)
			return
		}
		td.Reset()
		if s.limiter != nil && !s.limiter.Allow() {
			conn.Close()
			log.Warnf("TCP server accepted max num:%d, new conn rejected", s.opts.ConnLimit)
			continue
		}
		tcpConn := conn.(*net.TCPConn)

		// TCP keepalive
		if err = tcpConn.SetKeepAlive(true); err != nil {
			log.Warnf("TCP server conn:%s SetKeepAlive error:[%v]", tcpConn.RemoteAddr(), err)
		}
		if err = tcpConn.SetKeepAlivePeriod(time.Minute * 1); err != nil {
			log.Warnf("TCP server conn:%s SetKeepAlivePeriod error:[%v]", tcpConn.RemoteAddr(), err)
		}
		// setting keepalive retry count and interval
		if err := setKeepaliveParameters(tcpConn, 6, 10); err != nil {
			log.Warnf("TCP server conn:%s setKeepaliveParameters error:[%v]", tcpConn.RemoteAddr(), err)
		}
		// new conn
		newConn(ctx, s, tcpConn)
		atomic.AddUint32(&s.connNum, 1)
	}
}
