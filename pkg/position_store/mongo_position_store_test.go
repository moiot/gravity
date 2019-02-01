package position_store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mongo_test"
	. "github.com/moiot/gravity/pkg/position_store"
)

func TestMongoPositionStore(t *testing.T) {
	mongo_test.InitReplica()

	r := require.New(t)

	mongoCfg := mongo_test.TestConfig()

	store, err := NewMongoPositionStore(t.Name(), &mongoCfg, nil)
	r.NoError(err)

	store.Clear()

	r.NoError(store.Start())

	r.Empty(store.Get())

	const pos = config.MongoPosition(123)
	store.Put(pos)
	r.Equal(pos, store.Get())

	store.Close()

	store, err = NewMongoPositionStore(t.Name(), &mongoCfg, nil)
	r.NoError(err)
	r.Equal(pos, store.Get())
}
