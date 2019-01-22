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

type PositionCache struct {
	pipelineName string
	dirty        bool
	repo         PositionRepo

	position Position
	sync.Mutex

	closeC chan struct{}
	wg     sync.WaitGroup
}

func (cache *PositionCache) Start() error {
	cache.wg.Add(1)
	go func() {
		defer cache.wg.Done()
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := cache.Flush(); err != nil {
					log.Fatalf("[PositionCache] ticker flush failed: %v", errors.ErrorStack(err))
				}
			case <-cache.closeC:
				if err := cache.Flush(); err != nil {
					log.Fatalf("[PositionCache] close flush failed: %v", errors.ErrorStack(err))
				}
				return
			}
		}
	}()
	return nil
}

func (cache *PositionCache) Close() {
	log.Infof("[PositionCache] closing")
	close(cache.closeC)
	cache.wg.Wait()
	log.Infof("[PositionCache] closed")
}

func (cache *PositionCache) Put(position Position) {
	cache.Lock()
	defer cache.Unlock()
	cache.position = position
	cache.dirty = true
}

func (cache *PositionCache) Get() Position {
	cache.Lock()
	defer cache.Unlock()
	return cache.position
}

func (cache *PositionCache) Flush() error {
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

func (cache *PositionCache) Clear() error {
	cache.Lock()
	defer cache.Unlock()
	position := Position{
		Name:  cache.pipelineName,
		Stage: config.Unknown,
		Value: "",
	}

	cache.position = position
	cache.dirty = false
	return errors.Trace(cache.repo.Put(cache.pipelineName, position))
}

func NewPositionCache(pipelineName string, repo PositionRepo) (*PositionCache, error) {
	store := PositionCache{pipelineName: pipelineName, repo: repo, closeC: make(chan struct{})}

	// Load initial data from repo
	position, err := repo.Get(pipelineName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store.position = position

	return &store, nil
}
