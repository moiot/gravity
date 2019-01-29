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

	positionCache *twoStagePositionCache
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

func (i *TwoStageInputPlugin) NewPositionCache() (position_store.PositionCacheInterface, error) {
	caches := twoStagePositionCache{}
	caches.current = &atomic.Value{}

	fullPositionCache, err := i.full.NewPositionCache()
	if err != nil {
		return nil, errors.Trace(err)
	}
	caches.full = fullPositionCache

	position, _, err := fullPositionCache.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	stage := position.Stage
	if stage == config.Stream {
		if incrementalPositionStore, err := i.incremental.NewPositionCache(); err != nil {
			return nil, errors.Trace(err)
		} else {
			caches.incremental = incrementalPositionStore
		}
	}

	// init current stage and position store
	caches.current.Store(stage)
	i.positionCache = &caches
	return &caches, nil
}

func (i *TwoStageInputPlugin) Start(emitter core.Emitter, positionCache position_store.PositionCacheInterface) error {
	if i.Stage() == config.Stream {
		log.Info("[TwoStageInputPlugin.Start] with inc")
		return i.incremental.Start(emitter, positionCache)
	} else {
		log.Info("[TwoStageInputPlugin.Start] with full")
		if err := i.full.Start(emitter, positionCache); err != nil {
			return errors.Trace(err)
		}

		go func() {

			pos, ok := <-i.full.Done(positionCache)
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

			i.positionCache.current.Store(config.Stream)
			i.full.Close()
			i.positionCache.full.Close()

			pos.Stage = config.Stream

			// setup incremental position store first
			incPositionStore, err := i.incremental.NewPositionCache()
			if err != nil {
				log.Fatalf("[TwoStageInputPlugin] failed to create incrmental position store: %v", errors.ErrorStack(err))
			}
			i.positionCache.incremental = incPositionStore

			if err := i.positionCache.incremental.Start(); err != nil {
				log.Fatalf("[TwoStageInputPlugin] failed to start incremental position store: %v", errors.ErrorStack(err))
			}

			i.positionCache.incremental.Put(pos)
			if err := i.positionCache.incremental.Flush(); err != nil {
				log.Fatalf("[TwoStageInputPlugin] failed to change position stage")
			}

			// start incremental plugin
			err = i.incremental.Start(emitter, positionCache)
			if err != nil {
				log.Fatalf("[TwoStageInputPlugin] fail to start incremental. %s", err)
			}
			log.Infof("[TwoStageInputPlugin] incremental stage started")
		}()
		return nil
	}
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

func (i *TwoStageInputPlugin) Stage() config.InputMode {
	return i.positionCache.current.Load().(config.InputMode)
}

func (i *TwoStageInputPlugin) Done(positionCache position_store.PositionCacheInterface) chan position_store.Position {
	if i.Stage() == config.Stream {
		return i.incremental.Done(positionCache)
	} else {
		return i.full.Done(positionCache)
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

type twoStagePositionCache struct {
	incremental position_store.PositionCacheInterface
	full        position_store.PositionCacheInterface
	current     *atomic.Value
}

func (s *twoStagePositionCache) Start() error {
	if s.Stage() == config.Stream {
		return s.incremental.Start()
	} else {
		return s.full.Start()
	}
}

func (s *twoStagePositionCache) Close() {
	if s.Stage() == config.Stream {
		s.incremental.Close()
	} else {
		s.full.Close()
	}
}

func (s *twoStagePositionCache) Put(position position_store.Position) error {
	if s.Stage() == config.Stream {
		return errors.Trace(s.incremental.Put(position))
	} else {
		return errors.Trace(s.full.Put(position))
	}
}

func (s *twoStagePositionCache) Get() (position_store.Position, bool, error) {
	if s.Stage() == config.Stream {
		return s.incremental.Get()
	} else {
		return s.full.Get()
	}
}

func (s *twoStagePositionCache) Flush() error {
	if s.Stage() == config.Stream {
		return errors.Trace(s.incremental.Flush())
	} else {
		return errors.Trace(s.full.Flush())
	}
}

func (s *twoStagePositionCache) Clear() error {
	if s.Stage() == config.Stream {
		return errors.Trace(s.incremental.Clear())
	} else {
		return errors.Trace(s.full.Clear())
	}
}

func (s *twoStagePositionCache) Stage() config.InputMode {
	return s.current.Load().(config.InputMode)
}
