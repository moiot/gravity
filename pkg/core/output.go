package core

type Output interface {
	Execute(msgs []*Msg) error
	GetRouter() Router
	Close()
}

type SynchronousOutput interface {
	Start() error
	Output
}

type AsynchronousOutput interface {
	Start(msgAcker MsgAcker) error
	Output
}
