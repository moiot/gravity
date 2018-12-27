package emitter

import (
	"sync"
	"time"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
)

const FirstSequenceNumber = 1

type SequenceGenerator struct {
	sequenceNumber int64
}

func (generator *SequenceGenerator) Next() int64 {
	generator.sequenceNumber++
	return generator.sequenceNumber
}

func NewSequenceGenerator() *SequenceGenerator {
	g := SequenceGenerator{sequenceNumber: FirstSequenceNumber - 1}
	return &g
}

type defaultEmitter struct {
	sequenceGenerators map[string]*SequenceGenerator
	mutex              sync.Mutex

	fs           []core.IFilter
	msgSubmitter core.MsgSubmitter
}

func (e *defaultEmitter) Emit(msg *core.Msg) error {
	if msg.InputStreamKey == nil {
		return errors.Errorf("[emitter] InputStreamKey nil")
	}

	if msg.OutputStreamKey == nil {
		return errors.Errorf("[emitter] OutputStreamKey nil")
	}

	if msg.InputSequence != nil {
		return errors.Errorf("[emitter] InputSequence not nil: %v", *msg.InputSequence)
	}

	msg.MsgEmitTime = time.Now()

	// use fs to modify messages
	for _, filter := range e.fs {
		continueNext, err := filter.Filter(msg)
		if err != nil {
			return errors.Trace(err)
		}

		if !continueNext {
			return nil
		}

	}

	// generate sequence number and submit it to scheduler
	e.mutex.Lock()
	inputStreamKey := *(msg.InputStreamKey)
	if _, ok := e.sequenceGenerators[inputStreamKey]; !ok {
		e.sequenceGenerators[inputStreamKey] = NewSequenceGenerator()
	}
	e.mutex.Unlock()

	sn := e.sequenceGenerators[inputStreamKey].Next()
	msg.InputSequence = &sn

	msg.MsgSubmitTime = time.Now()
	if err := e.msgSubmitter.SubmitMsg(msg); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewEmitter(fs []core.IFilter, msgSubmitter core.MsgSubmitter) (core.Emitter, error) {
	emitter := defaultEmitter{fs: fs, msgSubmitter: msgSubmitter, sequenceGenerators: make(map[string]*SequenceGenerator)}
	return &emitter, nil
}
