package emitter

import (
	"strings"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/metrics"

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
	msg.EnterEmitter = time.Now()
	metrics.Input2EmitterCounter.WithLabelValues(core.PipelineName).Add(1)
	metrics.InputHistogram.WithLabelValues(core.PipelineName).Observe(msg.EnterEmitter.Sub(msg.EnterInput).Seconds())

	if msg.InputStreamKey == nil {
		return errors.Errorf("[emitter] InputStreamKey nil")
	}

	if msg.InputSequence != nil {
		return errors.Errorf("[emitter] InputSequence not nil: %v", *msg.InputSequence)
	}

	// use fs to modify messages
	for _, filter := range e.fs {
		continueNext, err := filter.Filter(msg)
		if err != nil {
			return errors.Trace(err)
		}

		if !continueNext {
			close(msg.Done)
			return nil
		}

	}

	// generate sequence number and submit it to scheduler
	inputStreamKey := *(msg.InputStreamKey)
	e.mutex.Lock()
	gen, ok := e.sequenceGenerators[inputStreamKey]
	if !ok {
		gen = NewSequenceGenerator()
		e.sequenceGenerators[inputStreamKey] = gen
	}
	e.mutex.Unlock()

	sn := gen.Next()
	msg.InputSequence = &sn
	metrics.Emitter2SchedulerCounter.WithLabelValues(core.PipelineName).Add(1)
	if err := e.msgSubmitter.SubmitMsg(msg); err != nil {
		return errors.Trace(err)
	}
	msg.LeaveEmitter = time.Now()
	metrics.EmitterHistogram.WithLabelValues(core.PipelineName).Observe(msg.LeaveEmitter.Sub(msg.EnterEmitter).Seconds())
	return nil
}

func (e *defaultEmitter) Close() error {
	var es []string

	for _, f := range e.fs {
		if err := f.Close(); err != nil {
			es = append(es, errors.ErrorStack(err))
		}
	}

	if len(es) > 0 {
		return errors.Errorf(strings.Join(es, " "))
	}
	return nil
}

func NewEmitter(fs []core.IFilter, msgSubmitter core.MsgSubmitter) (core.Emitter, error) {
	emitter := defaultEmitter{fs: fs, msgSubmitter: msgSubmitter, sequenceGenerators: make(map[string]*SequenceGenerator)}
	return &emitter, nil
}
