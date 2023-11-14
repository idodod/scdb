package tcpcore

import (
	"context"
	"time"

	"github.com/sjy-dv/scdb/scdb/pkg/codec"
)

const (
	MaxRWLen uint32 = 1 << 25
)

type Option func(o *Options)

type Options struct {
	Addr           string            // addr for service
	ServiceType    ServiceType       // service type, default SvcTypeTCPServer
	Handler        EventHandler      // event handler, default &NetEventHandler
	HeaderCodec    codec.HeaderCodec // default: &codec.CodecFixed32{}
	ReadTimeout    time.Duration     // default: 2m, zero value means I/O operations will not time out
	WriteTimeout   time.Duration     // default: 5s, zero value means I/O operations will not time out
	InitReadBufLen uint32            // default: 1024, init length of conn reading buf
	MaxReadBufLen  uint32            // default: MaxRWLen
	ConnLimit      uint32            // default: 0, unlimited, limit of conn num for Server

	// Context specifies a context for the service.
	// Can be used to signal shutdown of the service.
	Ctx context.Context

	// Tag a tag for gnet.Conn
	Tag string

	// HeartData heartbeat data, for asyncClient or gnet.Pool
	HeartData []byte
	// HeartInterval heartbeat interval, default: 30s
	HeartInterval time.Duration

	// PoolInitSize number of connections to establish when creating a pool
	PoolInitSize uint32
	// PoolMaxSize max number of connections in pool
	PoolMaxSize uint32
	// PoolGetTimeout timeout for getting a Conn from pool
	PoolGetTimeout time.Duration
	// PoolIdleTimeout Conn max idle duration,
	// if timeout occurs, Conn will be closed and removed from the pool
	// default:0, means conn will not time out.
	PoolIdleTimeout time.Duration
}

func DefaultOptions() Options {
	return Options{
		ServiceType:    SvcTypeTCPServer,
		Handler:        DefaultEventHandler(),
		HeaderCodec:    &codec.CodecFixed32{},
		ReadTimeout:    2 * time.Minute,
		WriteTimeout:   5 * time.Second,
		InitReadBufLen: 1024,
		MaxReadBufLen:  MaxRWLen,
		ConnLimit:      0,
		Ctx:            context.Background(),
		HeartData:      nil,
		HeartInterval:  30 * time.Second,
		PoolInitSize:   0,
		PoolMaxSize:    16,
		PoolGetTimeout: 3 * time.Second,
	}
}

// WithAddr
func WithAddr(addr string) Option {
	return func(o *Options) {
		o.Addr = addr
	}
}

// WithServiceType
func WithServiceType(t ServiceType) Option {
	return func(o *Options) {
		o.ServiceType = t
	}
}

// WithEventHandler
func WithEventHandler(handler EventHandler) Option {
	return func(o *Options) {
		o.Handler = handler
	}
}

// default: &codec.CodecFixed32{}
func WithHeaderCodec(codec codec.HeaderCodec) Option {
	return func(o *Options) {
		if codec != nil {
			o.HeaderCodec = codec
		}
	}
}

// timeout: A zero value for t means I/O operations will not time out.
// default: 2m
func WithReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = timeout
	}
}

// timeout: A zero value for t means I/O operations will not time out.
// default: 5s
func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.WriteTimeout = timeout
	}
}

// default: init 1024, max constant.MaxRWLen
func WithBufferLen(init, max uint32) Option {
	return func(o *Options) {
		if max > 0 {
			o.MaxReadBufLen = max
		}
		if init > o.MaxReadBufLen {
			init = o.MaxReadBufLen
		}
		if init > 0 {
			o.InitReadBufLen = init
		}
	}
}

// WithConnNumLimit limit of conn for Server
// default: 0, unlimited
func WithConnNumLimit(limit uint32) Option {
	return func(o *Options) {
		o.ConnLimit = limit
	}
}

// WithContext
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Ctx = ctx
	}
}

// WithTag
func WithTag(tag string) Option {
	return func(o *Options) {
		o.Tag = tag
	}
}

// WithHeartbeat for AsyncClient or Pool
// data: body data
func WithHeartbeat(data []byte, interval time.Duration) Option {
	return func(o *Options) {
		o.HeartData = data
		if interval > 0 {
			o.HeartInterval = interval
		}
	}
}

// WithPoolSize
func WithPoolSize(init, max uint32) Option {
	return func(o *Options) {
		if max > 0 {
			o.PoolMaxSize = max
		}
		if init > o.PoolMaxSize {
			init = o.PoolMaxSize
		}
		if init > 0 {
			o.PoolInitSize = init
		}
	}
}

// WithPoolGetTimeout Get a Conn from pool, failed when timeout occurs
func WithPoolGetTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.PoolGetTimeout = timeout
	}
}

// WithPoolIdleTimeout
func WithPoolIdleTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.PoolIdleTimeout = timeout
	}
}
