package position_store

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	log "github.com/sirupsen/logrus"
)

type Position struct {
	Name       string
	Stage      config.InputMode
	Value      string
	UpdateTime time.Time
}

type PositionCacheInterface interface {
	Start() error
	Close()
	Put(position Position)
	Get() Position
	Flush() error
	Clear() error
}

type defaultPositionCache struct {
	pipelineName string
	dirty        bool
	repo         PositionRepo

	position Position
	sync.Mutex

	closeC chan struct{}
	wg     sync.WaitGroup
}

func (cache *defaultPositionCache) Start() error {
	cache.wg.Add(1)
	go func() {
		defer cache.wg.Done()
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := cache.Flush(); err != nil {
					log.Fatalf("[defaultPositionCache] ticker flush failed: %v", errors.ErrorStack(err))
				}
			case <-cache.closeC:
				if err := cache.Flush(); err != nil {
					log.Fatalf("[defaultPositionCache] close flush failed: %v", errors.ErrorStack(err))
				}
				return
			}
		}
	}()
	return nil
}

func (cache *defaultPositionCache) Close() {
	log.Infof("[defaultPositionCache] closing")
	close(cache.closeC)
	cache.wg.Wait()
	log.Infof("[defaultPositionCache] closed")
}

func (cache *defaultPositionCache) Put(position Position) {
	cache.Lock()
	defer cache.Unlock()
	cache.position = position
	cache.dirty = true
}

func (cache *defaultPositionCache) Get() Position {
	cache.Lock()
	defer cache.Unlock()
	return cache.position
}

func (cache *defaultPositionCache) Flush() error {
	cache.Lock()
	defer cache.Unlock()

	if !cache.dirty {
		return nil
	}

	err := cache.repo.Put(cache.pipelineName, cache.position)
	if err != nil {
		return errors.Trace(err)
	}
	cache.dirty = false
	return nil
}

func (cache *defaultPositionCache) Clear() error {
	cache.Lock()
	defer cache.Unlock()
	position := Position{
		Name:  cache.pipelineName,
		Stage: config.Unknown,
		Value: "",
	}

	cache.position = position
	cache.dirty = false
	return errors.Trace(cache.repo.Delete(cache.pipelineName))
}

func NewPositionCache(pipelineName string, repo PositionRepo) (PositionCacheInterface, error) {
	store := defaultPositionCache{pipelineName: pipelineName, repo: repo, closeC: make(chan struct{})}

	// Load initial data from repo
	position, err := repo.Get(pipelineName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store.position = position

	return &store, nil
}
