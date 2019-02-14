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

	repo.SetEncoderDecoder(StringEncoder, StringDecoder)

	// delete it first
	r.NoError(repo.Delete(t.Name()))

	_, exist, err := repo.Get(t.Name())
	r.NoError(err)

	r.False(exist)

	// put first value
	position := Position{
		Name:  t.Name(),
		Stage: config.Stream,
		Value: "test",
	}
	r.NoError(repo.Put(t.Name(), position))

	p, exist, err := repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal("test", p.Value)
	r.Equal(config.Stream, p.Stage)

	// put another value
	position.Value = "test2"
	r.NoError(repo.Put(t.Name(), position))

	p2, exist, err := repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal(p2.Value, "test2")

	// put an invalid value
	position.Value = nil
	err = repo.Put(t.Name(), position)
	r.NotNil(err)
}
