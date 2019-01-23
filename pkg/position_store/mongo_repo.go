package position_store

import (
	"time"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type mongoPositionRepo struct {
	session *mgo.Session
}

func (repo *mongoPositionRepo) Get(pipelineName string) (Position, error) {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	position := Position{}
	err := collection.Find(bson.M{"name": pipelineName}).One(&position)
	if err != nil {
		return Position{}, errors.Trace(err)
	}
	return position, nil
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

func NewMongoRepo(session *mgo.Session) (PositionRepo, error) {
	return &mongoPositionRepo{session: session}, nil
}
