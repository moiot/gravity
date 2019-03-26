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

		batchPositionValue, ok := p.Value.(BatchPositionValueV1)
		r.True(ok)

		r.NotNil(batchPositionValue.Start)
		r.NotEmpty(batchPositionValue.Start.BinlogGTID)
	})

	t.Run("when position exists", func(tt *testing.T) {
		// it does nothing
		pipelineName := utils.TestCaseMd5Name(tt)

		batchPositionValue := BatchPositionValueV1{
			Start: utils.MySQLBinlogPosition{BinlogGTID: "abc:123"},
		}

		s, err := EncodeBatchPositionValue(batchPositionValue)
		r.NoError(err)
		r.NoError(repo.Put(pipelineName, position_store.PositionMeta{Name: pipelineName, Stage: config.Batch}, s))

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

		newPositionValue, ok := p.Value.(BatchPositionValueV1)
		r.True(ok)
		r.Equal("abc:123", newPositionValue.Start.BinlogGTID)
	})

}

func TestDecodeBatchPositionValueMigration(t *testing.T) {
	r := require.New(t)

	// encode v1beta1 and decode it, we should get v1
	beta := BatchPositionValueV1Beta1{
		Start: utils.MySQLBinlogPosition{
			BinLogFileName: "test",
			BinlogGTID:     "test_gtid",
		},
		TableStates: map[string]TableStats{
			"test": {
				Max: &TablePosition{Value: 1, Column: "a"},
			},
		},
	}
	s, err := myJson.MarshalToString(beta)
	r.NoError(err)

	v, err := DecodeBatchPositionValue(s)
	r.NoError(err)
	v1, ok := v.(BatchPositionValueV1)
	r.True(ok)
	r.Equal(SchemaVersionV1, v1.SchemaVersion)
	r.Equal("a", v1.TableStates["test"].Max[0].Column)
}
