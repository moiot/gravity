package position_store

import (
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo_test"
	"github.com/stretchr/testify/require"

	"gopkg.in/mgo.v2/bson"
)

func TestMongoPositionRepo_Get(t *testing.T) {
	r := require.New(t)

	mongoConfig := mongo_test.TestConfig()
	mongoSession, err := mongo.CreateMongoSession(&mongoConfig)
	r.NoError(err)

	repo, err := NewMongoPositionRepo(mongoSession)
	r.NoError(err)

	t.Run("empty record", func(tt *testing.T) {
		_, _, exist, err := repo.Get(tt.Name())
		r.NoError(err)
		r.False(exist)
	})

	t.Run("insert one record", func(tt *testing.T) {
		err := repo.Put(tt.Name(), PositionMeta{Stage: config.Stream}, "test")
		r.NoError(err)

		_, v, exists, err := repo.Get(tt.Name())
		r.NoError(err)
		r.True(exists)
		r.Equal("test", v)
	})

	t.Run("compatible with old position schema", func(tt *testing.T) {
		type OplogPositionsValue struct {
			StartPosition   config.MongoPosition `json:"start_position" bson:"start_position"`
			CurrentPosition config.MongoPosition `json:"current_position" bson:"current_position"`
		}
		var myJson = jsoniter.Config{SortMapKeys: true}.Froze()
		collection := mongoSession.DB(mongoPositionDB).C(mongoPositionCollection)
		// delete old data first
		_, err := collection.RemoveAll(bson.M{"name": tt.Name()})
		r.NoError(err)

		_, err = collection.Upsert(
			bson.M{"name": tt.Name()}, bson.M{
				"$set": bson.M{
					"stage":            string(config.Stream),
					"current_position": 1,
					"start_position":   1,
					"last_update":      time.Now().Format(time.RFC3339Nano),
				},
			})
		r.NoError(err)

		meta, v, exists, err := repo.Get(tt.Name())
		r.NoError(err)
		r.True(exists)

		oplogPositionValue := OplogPositionsValue{}
		r.NoError(myJson.UnmarshalFromString(v, &oplogPositionValue))

		r.EqualValues(1, oplogPositionValue.CurrentPosition)
		r.EqualValues(1, oplogPositionValue.StartPosition)

		// update again
		mp := config.MongoPosition(10)
		oplogPositionValue.CurrentPosition = mp
		s, err := myJson.MarshalToString(oplogPositionValue)
		r.NoError(err)

		r.NoError(repo.Put(tt.Name(), meta, s))

		_, s, exists, err = repo.Get(tt.Name())
		r.NoError(err)
		r.True(exists)

		newOplogValue := OplogPositionsValue{}
		r.NoError(myJson.UnmarshalFromString(s, &newOplogValue))

		r.EqualValues(config.MongoPosition(10), newOplogValue.CurrentPosition)

	})
}
