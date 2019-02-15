package position_store

import (
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

var DefaultFlushPeriod = 5 * time.Second

type PositionCacheInterface interface {
	Start() error
	Close()
	Put(position Position) error

	// Get will get a value from cache, if there is no value inside the cache
	// it will try to get it from position repo
	Get() (position Position, exist bool, err error)

	// GetWithValueString will serialize the Value to ValueString, if there is no value inside the cache
	// it won't try to get it from position repo
	GetWithValueString() (position Position, exist bool, err error)
	Flush() error
	Clear() error
}

type defaultPositionCache struct {
	flushDuration time.Duration
	pipelineName  string
	exist         bool
	dirty         bool
	repo          PositionRepo

	closeMutex sync.Mutex
	closed     bool

	position      Position
	positionMutex sync.Mutex

	closeC chan struct{}
	wg     sync.WaitGroup

	valueEncoder PositionValueEncoder
	valueDecoder PositionValueDecoder
}

func (cache *defaultPositionCache) Start() error {
	if cache.valueEncoder == nil || cache.valueDecoder == nil {
		return errors.Errorf("empty value encoder decoder")
	}

	cache.wg.Add(1)
	go func() {
		defer cache.wg.Done()
		ticker := time.NewTicker(cache.flushDuration)
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
	cache.closeMutex.Lock()
	defer cache.closeMutex.Unlock()

	if cache.closed {
		return
	}

	log.Infof("[defaultPositionCache] closing")
	close(cache.closeC)
	cache.wg.Wait()
	cache.repo.Close()
	log.Infof("[defaultPositionCache] closed")
	cache.closed = true
}

func (cache *defaultPositionCache) Put(position Position) error {
	cache.positionMutex.Lock()
	defer cache.positionMutex.Unlock()

	position.Name = cache.pipelineName

	if err := position.ValidateWithValue(); err != nil {
		return errors.Trace(err)
	}

	if !cache.exist {
		err := cache.encodePositionValueString(&position)
		if err != nil {
			return errors.Trace(err)
		}
		if err := cache.repo.Put(cache.pipelineName, position); err != nil {
			return errors.Trace(err)
		}
		cache.dirty = false
		cache.exist = true
	} else {
		cache.dirty = true
	}
	cache.position = position
	return nil
}

func (cache *defaultPositionCache) Get() (Position, bool, error) {
	cache.positionMutex.Lock()
	defer cache.positionMutex.Unlock()

	if !cache.exist {
		position, exist, err := cache.repo.Get(cache.pipelineName)
		if err != nil {
			return Position{}, exist, errors.Trace(err)
		}

		if exist {
			cache.exist = true

			err := cache.decodePositionValueString(&position)
			if err != nil {
				return Position{}, cache.exist, errors.Trace(err)
			}
			cache.position = position
			return position, cache.exist, nil
		} else {
			return Position{}, false, nil
		}
	}

	return cache.position, true, nil
}

func (cache *defaultPositionCache) GetWithValueString() (position Position, exist bool, err error) {
	cache.positionMutex.Lock()
	defer cache.positionMutex.Unlock()
	if !cache.exist {
		return Position{}, false, nil
	}

	err = cache.encodePositionValueString(&cache.position)
	if err != nil {
		return Position{}, true, errors.Trace(err)
	}
	return cache.position, true, nil
}

func (cache *defaultPositionCache) Flush() error {
	cache.positionMutex.Lock()
	defer cache.positionMutex.Unlock()

	if !cache.dirty {
		return nil
	}

	err := cache.encodePositionValueString(&cache.position)
	if err != nil {
		return errors.Trace(err)
	}

	err = cache.repo.Put(cache.pipelineName, cache.position)
	if err != nil {
		return errors.Trace(err)
	}
	cache.dirty = false
	return nil
}

// Clear stops flush position and then delete the position record.
func (cache *defaultPositionCache) Clear() error {
	cache.closeMutex.Lock()
	defer cache.closeMutex.Unlock()

	if cache.closed {
		return nil
	}

	close(cache.closeC)
	cache.wg.Wait()

	if err := cache.repo.Delete(cache.pipelineName); err != nil {
		return errors.Trace(err)
	}

	cache.repo.Close()

	cache.dirty = false
	cache.exist = false
	cache.closed = true
	return nil
}

func (cache *defaultPositionCache) get() {

}

func (cache *defaultPositionCache) encodePositionValueString(p *Position) error {
	s, err := cache.valueEncoder(p.Value)
	if err != nil {
		return errors.Trace(err)
	}
	p.ValueString = s
	return nil
}

func (cache *defaultPositionCache) decodePositionValueString(p *Position) error {
	v, err := cache.valueDecoder(p.ValueString)
	if err != nil {
		return errors.Trace(err)
	}
	p.Value = v
	return nil
}

func NewPositionCache(pipelineName string, repo PositionRepo, encoder PositionValueEncoder, decoder PositionValueDecoder, flushDuration time.Duration) (PositionCacheInterface, error) {
	store := defaultPositionCache{
		pipelineName:  pipelineName,
		repo:          repo,
		flushDuration: flushDuration,
		valueEncoder:  encoder,
		valueDecoder:  decoder,
		closeC:        make(chan struct{})}

	// Load initial data from repo
	position, exist, err := repo.Get(pipelineName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if exist {
		err := store.decodePositionValueString(&position)
		if err != nil {
			return nil, errors.Trace(err)
		}
		store.position = position
	}

	store.exist = exist

	return &store, nil
}
