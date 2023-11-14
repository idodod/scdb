package limiter

type defaultLimiter struct {
	c chan struct{}
}

func NewLimiter(n uint32) Limiter {
	return &defaultLimiter{
		c: make(chan struct{}, n),
	}
}

func (l *defaultLimiter) Allow() bool {
	select {
	case l.c <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *defaultLimiter) Revert() {
	<-l.c
}
