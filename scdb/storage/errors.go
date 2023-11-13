package storage

import "errors"

var (
	ErrValueTooLarge       = errors.New("the data size can't larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
	ErrMustStartWith       = errors.New("segment file extension must start with '.'")
	ErrLargeBlockCache     = errors.New("blockcache must be smaller than segment")
)
