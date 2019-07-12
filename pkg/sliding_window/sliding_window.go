package sliding_window

import (
	"time"
)

var HealthyThreshold float64 = 60

type WindowItem interface {
	SequenceNumber() int64
	BeforeWindowMoveForward()
	EventTime() time.Time
	ProcessTime() time.Time
}

// (Output) Watermark is defined as the minimum process time of input(which may be blocked on enqueue) and active items in window.
type Watermark struct {
	ProcessTime time.Time
	EventTime   time.Time
}

func (w Watermark) Healthy() bool {
	return time.Since(w.ProcessTime).Seconds() < HealthyThreshold
}

type Window interface {
	AddWindowItem(item WindowItem)
	AckWindowItem(sequence int64)
	Size() int
	WaitingQueueLen() int
	Close()
	Watermark() Watermark
}
