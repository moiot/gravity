package position_store

import (
	"time"

	"github.com/json-iterator/go"
	"github.com/moiot/gravity/pkg/config"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	mongoPositionDB         = "_gravity"
	mongoPositionCollection = "gravity_positions"
	Version                 = "1.0"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

//
// MongoPosition and PositionEntity is here to keep backward compatible with previous
// position format
//
type MongoPosition struct {
	StartPosition   config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition config.MongoPosition `json:"current_position" bson:"current_position"`
}
type PositionEntity struct {
	Name          string `json:"name" bson:"name"`
	Stage         string `json:"stage" bson:"stage"`
	MongoPosition `json:",inline" bson:",inline"`
	LastUpdate    string `json:"last_update" bson:"last_update"`
}

type mongoPositionRepo struct {
	session *mgo.Session
}

func (repo *mongoPositionRepo) Get(pipelineName string) (*PositionRepoModel, bool, error) {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	model := PositionRepoModel{}
	err := collection.Find(bson.M{"name": pipelineName}).One(&model)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, errors.Trace(err)
	}

	// backward compatible with old model schema
	if model.Version == "" {
		oldPosition := PositionEntity{}
		if err := collection.Find(bson.M{"name": pipelineName}).One(&oldPosition); err != nil {
			return nil, true, errors.Trace(err)
		}

		s, err := myJson.MarshalToString(&oldPosition.MongoPosition)
		if err != nil {
			return nil, true, errors.Trace(err)
		}

		return &PositionRepoModel{Name: pipelineName, Stage: oldPosition.Stage, Value: s}, true, nil

	}

	return &model, true, nil
}

func (repo *mongoPositionRepo) Put(pipelineName string, model *PositionRepoModel) error {

	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	_, err := collection.Upsert(
		bson.M{"name": pipelineName}, bson.M{
			"$set": bson.M{
				"version":     Version,
				"stage":       string(model.Stage),
				"value":       model.Value,
				"last_update": time.Now().Format(time.RFC3339Nano),
			},
		})
	return errors.Trace(err)
}

func (repo *mongoPositionRepo) Delete(pipelineName string) error {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	return errors.Trace(collection.Remove(bson.M{"name": pipelineName}))
}

func (repo *mongoPositionRepo) Close() error {
	repo.session.Close()
	return nil
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
