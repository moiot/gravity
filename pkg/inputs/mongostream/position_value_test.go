package mongostream

import (
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/juju/errors"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/utils"
)

func TestSetupInitialPosition(t *testing.T) {
	repo := position_repos.NewMemRepo("test")
	r := require.New(t)

	t.Run("when the initial position is empty", func(tt *testing.T) {
		tt.Run("when the start spec is empty", func(ttt *testing.T) {
			// it sets the current spec to MongoPosition(0), and keep the start spec empty
			pipelineName := utils.TestCaseMd5Name(ttt)
			cache, err := position_cache.NewPositionCache(
				pipelineName,
				repo,
				OplogPositionValueEncoder,
				OplogPositionValueDecoder,
				1*time.Second)
			r.NoError(err)

			r.NoError(SetupInitialPosition(cache, nil))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(config.Stream, position.Stage)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, ok := position.Value.(OplogPositionValue)
			r.True(ok)
			r.Nil(oplogPositionValue.StartPosition)
			r.EqualValues(config.MongoPosition(0), oplogPositionValue.CurrentPosition)
		})

		t.Run("when the start spec is not empty", func(ttt *testing.T) {
			// it sets the start position and current position to start spec
			pipelineName := utils.TestCaseMd5Name(ttt)

			cache, err := position_cache.NewPositionCache(
				pipelineName,
				repo,
				OplogPositionValueEncoder,
				OplogPositionValueDecoder,
				1*time.Second)
			r.NoError(err)

			startSpec := config.MongoPosition(100)
			r.NoError(SetupInitialPosition(cache, &startSpec))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)

			oplogPositionValue, ok := position.Value.(OplogPositionValue)
			r.True(ok)
			r.EqualValues(config.MongoPosition(100), *oplogPositionValue.StartPosition)
			r.EqualValues(config.MongoPosition(100), oplogPositionValue.CurrentPosition)
		})

	})

	t.Run("when the initial position is not empty", func(tt *testing.T) {
		tt.Run("when the start spec is not the same with position in repo", func(ttt *testing.T) {
			pipelineName := utils.TestCaseMd5Name(ttt)
			start := config.MongoPosition(100)
			current := config.MongoPosition(200)
			r.NoError(initPosition(repo, pipelineName, start, current))

			cache, err := position_cache.NewPositionCache(
				pipelineName,
				repo,
				OplogPositionValueEncoder,
				OplogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)

			newStart := config.MongoPosition(400)
			r.NoError(SetupInitialPosition(cache, &newStart))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, ok := position.Value.(OplogPositionValue)
			r.True(ok)

			r.EqualValues(config.MongoPosition(400), oplogPositionValue.CurrentPosition)
			r.EqualValues(config.MongoPosition(400), *oplogPositionValue.StartPosition)
		})

		t.Run("when the start spec is the same with position in repo", func(ttt *testing.T) {
			pipelineName := utils.TestCaseMd5Name(ttt)
			start := config.MongoPosition(100)
			current := config.MongoPosition(200)
			r.NoError(initPosition(repo, pipelineName, start, current))

			cache, err := position_cache.NewPositionCache(
				pipelineName,
				repo,
				OplogPositionValueEncoder,
				OplogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)

			newStart := config.MongoPosition(100)
			r.NoError(SetupInitialPosition(cache, &newStart))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, ok := position.Value.(OplogPositionValue)
			r.True(ok)

			r.EqualValues(config.MongoPosition(100), *oplogPositionValue.StartPosition)
			r.EqualValues(config.MongoPosition(200), oplogPositionValue.CurrentPosition)

			// Test clear position
			r.NoError(cache.Clear())

			_, exists, err = cache.Get()
			r.NoError(err)
			r.False(exists)

			r.NoError(SetupInitialPosition(cache, &newStart))
			position, exists, err = cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(config.Stream, position.Stage)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, ok = position.Value.(OplogPositionValue)
			r.True(ok)
			r.EqualValues(newStart, *oplogPositionValue.StartPosition)

		})

	})
}

func initPosition(repo position_repos.PositionRepo, pipelineName string, start config.MongoPosition, current config.MongoPosition) error {
	positionValue := OplogPositionValue{
		StartPosition:   &start,
		CurrentPosition: current,
	}

	m := position_repos.PositionMeta{
		Name:  pipelineName,
		Stage: config.Stream,
	}

	s, err := OplogPositionValueEncoder(&positionValue)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(repo.Put(pipelineName, m, s))
}
