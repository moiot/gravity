package core

type Emitter interface {
	// Emit use fs to modify messages and submit job to scheduler
	// msg is the message to send
	//
	Emit(msg *Msg) error
	Close() error
}
