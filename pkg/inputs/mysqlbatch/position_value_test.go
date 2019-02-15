package mysqlbatch

import (
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestSetupInitialPosition(t *testing.T) {
	r := require.New(t)
	repo := position_store.NewMemoRepo()

	t.Run("when position does not exist", func(tt *testing.T) {
		// it get a start binlog position and save it
		pipelineName := utils.TestCaseMd5Name(tt)
		cache, err := position_store.NewPositionCache(
			pipelineName,
			repo,
			EncodeBatchPositionValue,
			DecodeBatchPositionValue,
			5*time.Second)
		r.NoError(err)

		db := mysql_test.MustSetupSourceDB(pipelineName)

		err = SetupInitialPosition(cache, db)
		r.NoError(err)

		p, exists, err := cache.Get()
		r.NoError(err)
		r.True(exists)

		batchPositionValue, ok := p.Value.(*BatchPositionValue)
		r.True(ok)

		r.NotNil(batchPositionValue.Start)
		r.NotEmpty(batchPositionValue.Start.BinlogGTID)
	})

	t.Run("when position exists", func(tt *testing.T) {
		// it does nothing
		pipelineName := utils.TestCaseMd5Name(tt)

		batchPositionValue := BatchPositionValue{
			Start: &utils.MySQLBinlogPosition{BinlogGTID: "abc:123"},
		}

		s, err := EncodeBatchPositionValue(&batchPositionValue)
		r.NoError(err)
		r.NoError(repo.Put(pipelineName, &position_store.PositionWithValueString{Name: pipelineName, Stage: string(config.Batch), Value: s}))

		cache, err := position_store.NewPositionCache(
			pipelineName,
			repo,
			EncodeBatchPositionValue,
			DecodeBatchPositionValue,
			5*time.Second)
		r.NoError(err)

		db := mysql_test.MustSetupSourceDB(pipelineName)
		err = SetupInitialPosition(cache, db)
		r.NoError(err)

		p, exists, err := cache.Get()
		r.NoError(err)
		r.True(exists)

		newPositionValue, ok := p.Value.(*BatchPositionValue)
		r.True(ok)
		r.Equal("abc:123", newPositionValue.Start.BinlogGTID)
	})

}
