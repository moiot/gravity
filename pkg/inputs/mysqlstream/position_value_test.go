package mysqlstream

import (
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/stretchr/testify/require"
)

func initRepo(repo position_store.PositionRepo, pipelineName string, startGTID string, currentGTID string) error {

	positionValue := &helper.BinlogPositionsValue{
		CurrentPosition: &utils.MySQLBinlogPosition{BinlogGTID: currentGTID},
		StartPosition:   &utils.MySQLBinlogPosition{BinlogGTID: startGTID},
	}

	m := position_store.Position{
		Name:  pipelineName,
		Stage: config.Stream,
	}

	s, err := helper.BinlogPositionValueEncoder(positionValue)
	if err != nil {
		return errors.Trace(err)
	}
	m.ValueString = s

	return errors.Trace(repo.Put(pipelineName, m))
}

func TestSetupInitialPosition(t *testing.T) {
	r := require.New(t)

	repo := position_store.NewMemoRepo()

	t.Run("when there isn't any position in position repo", func(tt *testing.T) {
		tt.Run("when start spec is nil", func(ttt *testing.T) {

			cache, err := position_store.NewPositionCache(
				utils.TestCaseMd5Name(ttt),
				repo,
				helper.BinlogPositionValueEncoder,
				helper.BinlogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)

			db := mysql_test.MustSetupSourceDB(utils.TestCaseMd5Name(ttt))
			err = SetupInitialPosition(db, cache, nil)
			r.NoError(err)

			// it should init the current position to current master's position, and
			// start position should not be changed
			position, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)

			positionValue, ok := position.Value.(helper.BinlogPositionsValue)
			r.True(ok)
			r.Nil(positionValue.StartPosition)
			r.NotNil(positionValue.CurrentPosition)
		})

		tt.Run("when start spec is not nil", func(ttt *testing.T) {
			//
			// it should use the start spec as the start position and current position.
			//
			pipelineName := utils.TestCaseMd5Name(ttt)
			startGTID := "abc:123"
			currentGTID := "abc:789"
			err := initRepo(repo, pipelineName, startGTID, currentGTID)
			r.NoError(err)

			cache, err := position_store.NewPositionCache(
				pipelineName,
				repo,
				helper.BinlogPositionValueEncoder,
				helper.BinlogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)

			db := mysql_test.MustSetupSourceDB(pipelineName)
			newGTID := "abc:999"
			specStart := utils.MySQLBinlogPosition{
				BinlogGTID: newGTID,
			}

			err = SetupInitialPosition(db, cache, &specStart)
			r.NoError(err)

			p, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)
			newPositionValue, ok := p.Value.(helper.BinlogPositionsValue)
			r.True(ok)

			r.Equal(newGTID, newPositionValue.StartPosition.BinlogGTID)
			r.Equal(newGTID, newPositionValue.CurrentPosition.BinlogGTID)
		})
	})

	t.Run("when there is position in position repo", func(tt *testing.T) {
		t.Run("when start spec is the same with position in repo", func(ttt *testing.T) {
			// it should not change anything.
			pipelineName := utils.TestCaseMd5Name(ttt)
			startGTID := "abc:123"
			currentGTID := "abc:456"

			r.NoError(initRepo(repo, pipelineName, startGTID, currentGTID))

			specStart := utils.MySQLBinlogPosition{BinlogGTID: startGTID}

			db := mysql_test.MustSetupSourceDB(pipelineName)
			cache, err := position_store.NewPositionCache(
				pipelineName,
				repo,
				helper.BinlogPositionValueEncoder,
				helper.BinlogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)
			r.NoError(SetupInitialPosition(db, cache, &specStart))

			p, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)

			newPositionValue, ok := p.Value.(helper.BinlogPositionsValue)
			r.True(ok)
			r.Equal(startGTID, newPositionValue.StartPosition.BinlogGTID)
			r.Equal(currentGTID, newPositionValue.CurrentPosition.BinlogGTID)
		})

		t.Run("when start spec is not the same with position in repo", func(ttt *testing.T) {
			// it should use the start spec as the start position and current position.
			pipelineName := utils.TestCaseMd5Name(ttt)
			startGTID := "abc:123"
			currentGTID := "abc:456"

			r.NoError(initRepo(repo, pipelineName, startGTID, currentGTID))

			newGTID := "abc:789"
			specStart := utils.MySQLBinlogPosition{BinlogGTID: newGTID}

			db := mysql_test.MustSetupSourceDB(pipelineName)
			cache, err := position_store.NewPositionCache(
				pipelineName,
				repo,
				helper.BinlogPositionValueEncoder,
				helper.BinlogPositionValueDecoder,
				5*time.Second)
			r.NoError(err)
			r.NoError(SetupInitialPosition(db, cache, &specStart))

			p, exists, err := cache.Get()
			r.NoError(err)
			r.True(exists)

			newPositionValue, ok := p.Value.(helper.BinlogPositionsValue)
			r.True(ok)
			r.Equal(newGTID, newPositionValue.StartPosition.BinlogGTID)
			r.Equal(newGTID, newPositionValue.CurrentPosition.BinlogGTID)
		})

	})

}
