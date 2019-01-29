package helper

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/position_store"
)

type TwoStageInputPlugin struct {
	full            core.Input
	incremental     core.Input
	transitionMutex sync.Mutex
	closed          bool

	positionStore *twoStagePositionStore
}

func NewTwoStageInputPlugin(full, incremental core.Input) (core.Input, error) {

	if full.Stage() != config.Batch {
		return nil, errors.Errorf("expect input stage full, actually %s", full.Stage())
	}

	if incremental.Stage() != config.Stream {
		return nil, errors.Errorf("expect input stage incremental, actually %s", full.Stage())
	}

	return &TwoStageInputPlugin{
		full:        full,
		incremental: incremental,
	}, nil
}

func (i *TwoStageInputPlugin) Wait() {
	if i.Stage() == config.Stream {
		i.transitionMutex.Lock() // lock needed to ensure wait is called after start
		i.incremental.Wait()
		i.transitionMutex.Unlock()
		log.Info("[TwoStageInputPlugin.Wait] finish of incremental wait")
	} else {
		i.full.Wait()
		for {
			i.transitionMutex.Lock()

			if i.closed {
				log.Info("[TwoStageInputPlugin.Wait] finish of closed")
				break
			}

			if i.Stage() == config.Stream {
				i.incremental.Wait()
				log.Info("[TwoStageInputPlugin.Wait] finish of incremental wait after full")
				break
			}

			i.transitionMutex.Unlock()
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (i *TwoStageInputPlugin) SendDeadSignal() error {
	if i.Stage() == config.Stream {
		return i.incremental.SendDeadSignal()
	} else {
		return i.full.SendDeadSignal()
	}
}

func (i *TwoStageInputPlugin) Identity() uint32 {
	if i.Stage() == config.Stream {
		return i.incremental.Identity()
	} else {
		return i.full.Identity()
	}
}

func (i *TwoStageInputPlugin) NewPositionStore() (position_store.PositionStore, error) {
	s := twoStagePositionStore{}
	s.current = &atomic.Value{}

	fullPositionStore, err := i.full.NewPositionStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.full = fullPositionStore

	stage := fullPositionStore.Stage()

	if stage == config.Stream {
		if incrementalPositionStore, err := i.incremental.NewPositionStore(); err != nil {
			return nil, errors.Trace(err)
		} else {
			s.incremental = incrementalPositionStore
		}
	}

	// init current stage and position store
	s.current.Store(stage)
	i.positionStore = &s
	return &s, nil
}

func (i *TwoStageInputPlugin) PositionStore() position_store.PositionStore {
	return i.positionStore
}

func (i *TwoStageInputPlugin) Stage() config.InputMode {
	return i.positionStore.current.Load().(config.InputMode)
}

func (i *TwoStageInputPlugin) Done() chan position_store.Position {
	if i.Stage() == config.Stream {
		return i.incremental.Done()
	} else {
		return i.full.Done()
	}
}

func (i *TwoStageInputPlugin) Start(emitter core.Emitter, router core.Router) error {
	if i.Stage() == config.Stream {
		log.Info("[TwoStageInputPlugin.Start] with inc")
		return i.incremental.Start(emitter, router)
	} else {
		log.Info("[TwoStageInputPlugin.Start] with full")
		if err := i.full.Start(emitter, router); err != nil {
			return errors.Trace(err)
		}

		go func() {

			pos, ok := <-i.full.Done()
			if !ok {
				log.Info("[TwoStageInputPlugin] full stage done")
				return
			}

			i.transitionMutex.Lock()
			defer i.transitionMutex.Unlock()

			if i.closed {
				log.Info("[TwoStageInputPlugin] full stage closed")
				return
			}

			log.Infof("[TwoStageInputPlugin] full stage done with %s", pos)

			i.positionStore.current.Store(config.Stream)
			i.full.Close()
			i.positionStore.full.Close()

			pos.Stage = config.Stream

			// setup incremental position store first
			incPositionStore, err := i.incremental.NewPositionStore()
			if err != nil {
				log.Fatalf("[TwoStageInputPlugin] failed to create incrmental position store: %v", errors.ErrorStack(err))
			}
			i.positionStore.incremental = incPositionStore

			i.positionStore.incremental.Update(pos)
			if err := i.positionStore.incremental.Start(); err != nil {
				log.Fatalf("[TwoStageInputPlugin] failed to start incremental position store: %v", errors.ErrorStack(err))
			}

			// start incremental plugin
			err = i.incremental.Start(emitter, router)
			if err != nil {
				log.Fatalf("[TwoStageInputPlugin] fail to start incremental. %s", err)
			}
			log.Infof("[TwoStageInputPlugin] incremental stage started")
		}()
		return nil
	}
}

func (i *TwoStageInputPlugin) Close() {
	i.transitionMutex.Lock()
	defer i.transitionMutex.Unlock()

	if i.closed {
		return
	}

	i.closed = true
	if i.Stage() == config.Stream {
		i.incremental.Close()
	} else {
		i.full.Close()
	}
}

type twoStagePositionStore struct {
	incremental position_store.PositionStore
	full        position_store.PositionStore
	current     *atomic.Value
}

func (s *twoStagePositionStore) Start() error {
	if s.Stage() == config.Stream {
		return s.incremental.Start()
	} else {
		return s.full.Start()
	}
}

func (s *twoStagePositionStore) Close() {
	if s.Stage() == config.Stream {
		s.incremental.Close()
	} else {
		s.full.Close()
	}
}

func (s *twoStagePositionStore) Stage() config.InputMode {
	return s.current.Load().(config.InputMode)
}

func (s *twoStagePositionStore) Position() position_store.Position {
	if s.Stage() == config.Stream {
		return s.incremental.Position()
	} else {
		return s.full.Position()
	}
}

func (s *twoStagePositionStore) Update(pos position_store.Position) {
	if s.Stage() == config.Stream {
		s.incremental.Update(pos)
	} else {
		s.full.Update(pos)
	}
}

func (s *twoStagePositionStore) Clear() {
	if s.Stage() == config.Stream {
		s.incremental.Clear()
	} else {
		s.full.Clear()
	}
}
