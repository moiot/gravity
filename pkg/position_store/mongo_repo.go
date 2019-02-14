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
	session      *mgo.Session
	valueEncoder PositionValueEncoder
	valueDecoder PositionValueDecoder
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

	// backward compatible with old position schema
	if position.Version == "" {
		oldPosition := PositionEntity{}
		if err := collection.Find(bson.M{"name": pipelineName}).One(&oldPosition); err != nil {
			return Position{}, true, errors.Trace(err)
		}

		s, err := myJson.MarshalToString(&oldPosition.MongoPosition)
		if err != nil {
			return Position{}, true, errors.Trace(err)
		}

		vInterface, err := repo.valueDecoder(s)
		if err != nil {
			return Position{}, true, errors.Trace(err)
		}

		return Position{Name: pipelineName, Stage: config.InputMode(oldPosition.Stage), Value: vInterface}, true, nil

	}
	s, ok := position.Value.(string)
	if !ok {
		return Position{}, true, errors.Errorf("serialized value is not a string")
	}

	vInterface, err := repo.valueDecoder(s)
	position.Value = vInterface
	return position, true, nil
}

func (repo *mongoPositionRepo) GetWithRawValue(pipelineName string) (Position, bool, error) {
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

func (repo *mongoPositionRepo) Put(pipelineName string, position Position) error {
	s, err := repo.valueEncoder(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	_, err = collection.Upsert(
		bson.M{"name": pipelineName}, bson.M{
			"$set": bson.M{
				"version":     Version,
				"stage":       string(position.Stage),
				"value":       s,
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

func (repo *mongoPositionRepo) SetEncoderDecoder(encoder PositionValueEncoder, decoder PositionValueDecoder) {
	repo.valueEncoder = encoder
	repo.valueDecoder = decoder
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
