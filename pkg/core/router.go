package core

type Router interface {
	Exists(msg *Msg) bool
}

type EmptyRouter struct{}

func (EmptyRouter) Exists(msg *Msg) bool {
	return true
}
