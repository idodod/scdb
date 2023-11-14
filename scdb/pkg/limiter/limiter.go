package limiter

type Limiter interface {
	Allow() bool
	Revert()
}
