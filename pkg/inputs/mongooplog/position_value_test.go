package mongooplog

import (
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestSetupInitialPosition(t *testing.T) {
	repo := position_store.NewMemoRepo()
	r := require.New(t)

	t.Run("when the initial position is empty", func(tt *testing.T) {
		tt.Run("when the start spec is empty", func(ttt *testing.T) {
			// it sets the current spec to MongoPosition(0), and keep the start spec empty
			pipelineName := utils.TestCaseMd5Name(ttt)
			cache, err := position_store.NewPositionCache(pipelineName, repo, 1*time.Second)
			r.NoError(err)

			r.NoError(SetupInitialPosition(cache, nil))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(config.Stream, position.Stage)
			r.Equal(pipelineName, position.Name)

			oplogPosition, err := Deserialize(position.Value)
			r.NoError(err)
			r.Nil(oplogPosition.StartPosition)
			r.EqualValues(config.MongoPosition(0), *(oplogPosition.CurrentPosition))
		})

		t.Run("when the start spec is not empty", func(ttt *testing.T) {
			// it sets the start position and current position to start spec
			pipelineName := utils.TestCaseMd5Name(ttt)

			cache, err := position_store.NewPositionCache(pipelineName, repo, 1*time.Second)
			r.NoError(err)

			startSpec := config.MongoPosition(100)
			r.NoError(SetupInitialPosition(cache, &startSpec))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)

			oplogPosition, err := Deserialize(position.Value)
			r.NoError(err)
			r.EqualValues(config.MongoPosition(100), *(oplogPosition.StartPosition))
			r.EqualValues(config.MongoPosition(100), *(oplogPosition.CurrentPosition))
		})

	})

	t.Run("when the initial position is not empty", func(tt *testing.T) {
		tt.Run("when the start spec is not the same with position in repo", func(ttt *testing.T) {
			pipelineName := utils.TestCaseMd5Name(ttt)
			start := config.MongoPosition(100)
			current := config.MongoPosition(200)
			r.NoError(initPosition(repo, pipelineName, &start, &current))

			cache, err := position_store.NewPositionCache(pipelineName, repo, 5*time.Second)
			r.NoError(err)

			newStart := config.MongoPosition(400)
			r.NoError(SetupInitialPosition(cache, &newStart))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, err := Deserialize(position.Value)
			r.NoError(err)

			r.EqualValues(config.MongoPosition(400), *(oplogPositionValue.CurrentPosition))
			r.EqualValues(config.MongoPosition(400), *(oplogPositionValue.StartPosition))
		})

		t.Run("when the start spec is the same with position in repo", func(ttt *testing.T) {
			pipelineName := utils.TestCaseMd5Name(ttt)
			start := config.MongoPosition(100)
			current := config.MongoPosition(200)
			r.NoError(initPosition(repo, pipelineName, &start, &current))

			cache, err := position_store.NewPositionCache(pipelineName, repo, 5*time.Second)
			r.NoError(err)

			newStart := config.MongoPosition(100)
			r.NoError(SetupInitialPosition(cache, &newStart))

			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			r.Equal(pipelineName, position.Name)

			oplogPositionValue, err := Deserialize(position.Value)
			r.NoError(err)

			r.EqualValues(config.MongoPosition(200), *(oplogPositionValue.CurrentPosition))
			r.EqualValues(config.MongoPosition(100), *(oplogPositionValue.StartPosition))

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

			v, err := Deserialize(position.Value)
			r.NoError(err)
			r.EqualValues(newStart, *(v.StartPosition))

		})

	})
}

func initPosition(repo position_store.PositionRepo, pipelineName string, start *config.MongoPosition, current *config.MongoPosition) error {
	positionValue := OplogPositionValue{
		StartPosition:   start,
		CurrentPosition: current,
	}

	v, err := Serialize(&positionValue)
	if err != nil {
		return errors.Trace(err)
	}

	position := position_store.Position{
		Name:  pipelineName,
		Stage: config.Stream,
		Value: v,
	}
	return errors.Trace(repo.Put(pipelineName, position))
}
