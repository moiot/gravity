package core

type Emitter interface {
	// Emit use fs to modify messages and submit job to scheduler
	// msg is the message to send
	//
	// TODO better interface for Emit, so that we don't need InputStreamKey...
	//
	Emit(msg *Msg) error
}
