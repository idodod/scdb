package delay

import (
	"time"
)

type Delay interface {
	GetDelay() time.Duration
	Reset()
}

// double delay in [min, max]
type tempDelay struct {
	d   time.Duration // d *= 2
	min time.Duration // default 5ms
	max time.Duration // default 1s
}

func NewTempDelay(min, max time.Duration) Delay {
	if min == 0 {
		min = 5 * time.Millisecond
	}
	if max == 0 {
		max = time.Second
	}
	if min > max {
		min = max
	}
	return &tempDelay{
		d:   0,
		min: min,
		max: max,
	}
}

func (d *tempDelay) GetDelay() time.Duration {
	switch d.d {
	case 0:
		d.d = d.min
	case d.max:
	default:
		d.d <<= 1
		if d.d > d.max {
			d.d = d.max
		}
	}
	return d.d
}

func (d *tempDelay) Reset() {
	d.d = 0
}
