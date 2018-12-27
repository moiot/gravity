package checker

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/protocol/dcp"
	"github.com/sirupsen/logrus"
)

type buffer struct {
	log        *logrus.Entry
	tag        string
	buffer     []*dcp.Message
	inputChan  chan *dcp.Message
	OutputChan chan *segment
}

func newBuffer(tag string) *buffer {
	buffer := &buffer{
		logrus.WithField("buffer", tag),
		tag,
		make([]*dcp.Message, 0, 128),
		make(chan *dcp.Message),
		make(chan *segment),
	}
	go buffer.handler()
	return buffer
}

func (b *buffer) Write(msg *dcp.Message) {
	if msg.Tag != b.tag {
		panic(errors.Errorf("unmatched tag, expected %s, actual %s", b.tag, msg.Tag))
	}
	b.inputChan <- msg
}

func (b *buffer) Close() {
	b.log.Info("closing buffer")
	close(b.inputChan)
	close(b.OutputChan)
}

func (b *buffer) handler() {
	for msg := range b.inputChan {
		b.log.Debugf("buffer handle message %v, buffer %v", msg, b.buffer)
		barrier, isBarrier := msg.Body.(*dcp.Message_Barrier)
		if len(b.buffer) == 0 && !isBarrier {
			panic(errors.Errorf("buffer[%s] is empty, expect barrier message but actual %s", b.tag, msg))
		}

		if len(b.buffer) > 0 && isBarrier {
			if barrier.Barrier != b.buffer[0].GetBarrier()+1 {
				b.log.Panicf("jumping barrier, previous %d, current %d", b.buffer[0].GetBarrier(), barrier.Barrier)
			}
			b.OutputChan <- newSegment(b.tag, b.buffer[0].GetBarrier(), barrier.Barrier, b.buffer[1:])
			b.buffer = b.buffer[:0]
		}

		b.buffer = append(b.buffer, msg)
	}

	b.log.Info("exit buffer handler")
}
