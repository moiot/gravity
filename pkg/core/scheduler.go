package core

import "github.com/moiot/gravity/pkg/sliding_window"

type Scheduler interface {
	MsgSubmitter
	MsgAcker
	Healthy() bool
	Watermarks() map[string]sliding_window.Watermark
	Start(output Output) error
	Close()
}
