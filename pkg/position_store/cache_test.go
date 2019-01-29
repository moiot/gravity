package position_store

import (
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPositionCache_New(t *testing.T) {
	r := require.New(t)

	t.Run("when repo dont have any data", func(tt *testing.T) {
		repo := NewMemoRepo()
		cache, err := NewPositionCache(t.Name(), repo, DefaultFlushPeriod)
		r.NoError(err)

		_, exists, err := cache.Get()
		r.False(exists)
		r.NoError(err)
	})

	t.Run("when repo has some data", func(tt *testing.T) {
		repo := NewMemoRepo()
		err := repo.Put(t.Name(), Position{Value: "test", Stage: config.Stream})
		r.NoError(err)

		cache, err := NewPositionCache(t.Name(), repo, DefaultFlushPeriod)
		r.NoError(err)

		p, exists, err := cache.Get()
		r.True(exists)
		r.Nil(err)
		r.Equal("test", p.Value)

	})

}

func TestPositionCache_GetPut(t *testing.T) {
	r := require.New(t)

	t.Run("when position is not valid", func(tt *testing.T) {
		repo := NewMemoRepo()

		cache, err := NewPositionCache(t.Name(), repo, DefaultFlushPeriod)
		r.NoError(err)

		err = cache.Put(Position{Value: ""})
		r.NotNil(err)

		_, exists, err := cache.Get()
		r.False(exists)
		r.NoError(err)
	})

	t.Run("when position is valid", func(tt *testing.T) {
		repo := NewMemoRepo()

		cache, err := NewPositionCache(t.Name(), repo, DefaultFlushPeriod)
		r.NoError(err)

		err = cache.Put(Position{Value: "test2", Stage: config.Stream})
		r.NoError(err)

		p, exists, err := cache.Get()
		r.Equal("test2", p.Value)
		r.True(exists)
		r.NoError(err)

		err = cache.Put(Position{Value: "test3", Stage: config.Stream})
		r.NoError(err)
		p, exists, err = cache.Get()
		r.NoError(err)
		r.True(exists)
		r.Equal("test3", p.Value)

	})
}

func TestDefaultPositionCache_Flush(t *testing.T) {
	r := require.New(t)

	t.Run("it does not flush when time has not come", func(tt *testing.T) {
		repo := NewMemoRepo()
		cache, err := NewPositionCache(t.Name(), repo, 5*time.Second)
		r.NoError(err)

		r.NoError(cache.Start())

		err = cache.Put(Position{Value: "test", Stage: config.Stream})
		r.NoError(err)

		p, exists, err := cache.Get()
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		// the first PUT will flush data to repo
		p, exists, err = repo.Get(t.Name())
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		// the second PUT will not flush data to repo until flush time comes
		r.NoError(cache.Put(Position{Value: "test2", Stage: config.Stream}))
		p, exists, err = cache.Get()
		r.NoError(err)
		r.True(exists)
		r.Equal("test2", p.Value)
		p, exists, err = repo.Get(t.Name())
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		cache.Close()
	})

	t.Run("it flush to repo when time comes", func(tt *testing.T) {
		repo := NewMemoRepo()
		cache, err := NewPositionCache(t.Name(), repo, 1*time.Second)
		r.NoError(err)

		r.NoError(cache.Start())

		err = cache.Put(Position{Value: "test", Stage: config.Stream})
		r.NoError(err)

		p, exists, err := cache.Get()
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		// the first PUT will flush data to repo
		p, exists, err = repo.Get(t.Name())
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		// the second PUT will not flush data to repo until flush time comes
		r.NoError(cache.Put(Position{Value: "test2", Stage: config.Stream}))
		p, exists, err = cache.Get()
		r.NoError(err)
		r.True(exists)
		r.Equal("test2", p.Value)
		p, exists, err = repo.Get(t.Name())
		r.NoError(err)
		r.True(exists)
		r.Equal("test", p.Value)

		time.Sleep(5 * time.Second)
		p, _, _ = repo.Get(t.Name())
		r.Equal("test2", p.Value)

		cache.Close()
	})
}
