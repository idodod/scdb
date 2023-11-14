package tcpcore

import (
	"errors"
)

var (
	ErrTooLarge        = errors.New("data:too large")
	ErrConnClosed      = errors.New("conn:closed")
	ErrConnInvalidCall = errors.New("conn:invalid call")
	ErrPoolClosed      = errors.New("pool:closed")
	ErrPoolTimeout     = errors.New("pool:timeout")
	ErrPoolInvalidAddr = errors.New("pool:invalid addr")
)
