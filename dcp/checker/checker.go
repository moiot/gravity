package checker

import (
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/protocol/dcp"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	SourceTag      string
	TargetTags     []string
	TimeoutSeconds int
}

type checker struct {
	sourceTag    string
	tagIndex     map[string]int
	buffers      []*buffer
	shutdownChan chan struct{}
	timeout      time.Duration

	ResultChan chan Result
}

func New(config *Config) *checker {
	if len(config.TargetTags) < 1 {
		panic(errors.Errorf("there should be at least one tag to compare"))
	}

	tagIndex := map[string]int{
		config.SourceTag: 0,
	}
	buffers := make([]*buffer, 0, len(config.TargetTags)+1)
	buffers = append(buffers, newBuffer(config.SourceTag))
	for i, t := range config.TargetTags {
		tagIndex[t] = i + 1
		buffers = append(buffers, newBuffer(t))
	}

	s := checker{
		config.SourceTag,
		tagIndex,
		buffers,
		make(chan struct{}, 1),
		time.Second * time.Duration(config.TimeoutSeconds),
		make(chan Result, 10),
	}

	go s.mainLoop()

	return &s
}

func (s *checker) Shutdown() {
	s.shutdownChan <- struct{}{}
	close(s.shutdownChan)
	log.Infof("checker[%v] closed", s.tagIndex)
}

func (s *checker) mainLoop() {
entry:
	for {
		select {
		case <-s.shutdownChan:
			for _, b := range s.buffers {
				b.Close()
			}
			close(s.ResultChan)
			break entry
			//TODO graceful Shutdown
		default:
			srcSegment, ok := <-s.buffers[0].OutputChan
			if !ok {
				break entry
			}
			log.Debug("receive src segment", srcSegment)

			for _, b := range s.buffers[1:] {
				select {
				case targetSeg := <-b.OutputChan:
					equal := srcSegment.equal(targetSeg)
					log.Infof("equal %t comparing %s with %s offset %d", equal, srcSegment.tag, targetSeg.tag, srcSegment.startOffset)
					if !equal {
						s.ResultChan <- &Diff{
							srcSegment,
							targetSeg,
						}
					} else {
						s.ResultChan <- &Same{
							srcSegment,
							targetSeg,
						}
					}

				case <-time.After(s.timeout):
					log.Info("timeout for ", srcSegment, " target ", b.tag)
					s.ResultChan <- &Timeout{
						sourceSeg: srcSegment,
						targetTag: b.tag,
					}
				}
			}

		}
	}

	log.Info("exit checker main loop")
}

func (s *checker) Consume(msg *dcp.Message) {
	log.Debug("checker consume msg ", msg.String())
	idx, ok := s.tagIndex[msg.Tag]
	if !ok {
		log.Panic("unknown tag ", msg.Tag)
	}

	s.buffers[idx].Write(msg)
}
