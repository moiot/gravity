package position_store

import (
	"testing"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/stretchr/testify/require"
)

func TestMysqlPositionRepo_GetPut(t *testing.T) {
	r := require.New(t)

	dbCfg := mysql_test.SourceDBConfig()

	repo, err := NewMySQLRepo(dbCfg, "")
	r.NoError(err)

	// delete it first
	r.NoError(repo.Delete(t.Name()))

	_, _, exist, err := repo.Get(t.Name())
	r.NoError(err)

	r.False(exist)

	// put first value
	meta := PositionMeta{
		Name:  t.Name(),
		Stage: config.Stream,
	}
	r.NoError(repo.Put(t.Name(), meta, "test"))

	meta, v, exist, err := repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal("test", v)
	r.Equal(config.Stream, meta.Stage)

	// put another value
	r.NoError(repo.Put(t.Name(), meta, "test2"))

	meta, v, exist, err = repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal("test2", v)

	// put an invalid value
	err = repo.Put(t.Name(), meta, "")
	r.NotNil(err)
}
