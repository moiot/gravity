package core

type Scheduler interface {
	MsgSubmitter
	MsgAcker
	Healthy() bool
	Start(output Output) error
	Close()
}
