package position_store

import (
	"time"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	oldPositionDB           = "drc"
	mongoPositionDB         = "_gravity"
	mongoPositionCollection = "gravity_positions"
)

type mongoPositionRepo struct {
	session *mgo.Session
}

func (repo *mongoPositionRepo) Get(pipelineName string) (Position, bool, error) {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	position := Position{}
	err := collection.Find(bson.M{"name": pipelineName}).One(&position)
	if err != nil {
		if err == mgo.ErrNotFound {
			return Position{}, false, nil
		}
		return Position{}, false, errors.Trace(err)
	}
	return position, true, nil
}

// TODO compatible with previous version.
func (repo *mongoPositionRepo) Put(pipelineName string, position Position) error {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	_, err := collection.Upsert(bson.M{"name": pipelineName}, bson.M{
		"$set": bson.M{
			"stage":       string(position.Stage),
			"position":    position.Value,
			"last_update": time.Now().Format(time.RFC3339Nano),
		},
	})
	return errors.Trace(err)
}

func (repo *mongoPositionRepo) Delete(pipelineName string) error {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	return errors.Trace(collection.Remove(bson.M{"name": pipelineName}))
}

func NewMongoPositionRepo(session *mgo.Session) (PositionRepo, error) {
	session.SetMode(mgo.Primary, true)
	collection := session.DB(mongoPositionDB).C(mongoPositionCollection)
	err := collection.EnsureIndex(mgo.Index{
		Key:    []string{"name"},
		Unique: true,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mongoPositionRepo{session: session}, nil
}
