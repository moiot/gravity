package mongooplog

import (
	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type OplogPositionsValue struct {
	StartPosition   *config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition *config.MongoPosition `json:"current_position" bson:"current_position"`
}

func Deserialize(v string) (*OplogPositionsValue, error) {
	positions := OplogPositionsValue{}
	if err := myJson.UnmarshalFromString(v, &positions); err != nil {
		return nil, errors.Trace(err)
	}
	return &positions, nil
}

func Serialize(oplogPositionValue *OplogPositionsValue) (string, error) {

	s, err := myJson.MarshalToString(oplogPositionValue)
	if err != nil {
		return "", errors.Trace(err)
	}
	return s, nil
}

func InitPositionCache(cache *position_store.PositionCache, startPositionInSpec *config.MongoPosition) error {
	positionValues, err := GetPosition(cache)
	if err != nil {
		return errors.Trace(err)
	}
	if positionValues.StartPosition != nil && startPositionInSpec != nil {
		if *positionValues.StartPosition != *startPositionInSpec {
			positionValues.StartPosition = startPositionInSpec
			positionValues.CurrentPosition = startPositionInSpec

			if err := PutPositions(cache, positionValues); err != nil {
				return errors.Trace(err)
			}

			if err := cache.Flush(); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func GetPosition(cache *position_store.PositionCache) (*OplogPositionsValue, error) {
	position := cache.Get()
	oplogPositionValue, err := Deserialize(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return oplogPositionValue, nil
}

func PutCurrentPosition(cache *position_store.PositionCache, positionValue *config.MongoPosition) error {
	position := cache.Get()
	oplogPositionValue, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	oplogPositionValue.CurrentPosition = positionValue
	v, err := Serialize(oplogPositionValue)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	cache.Put(position)
	return nil
}

func PutPositions(cache *position_store.PositionCache, positionValues *OplogPositionsValue) error {
	position := cache.Get()

	v, err := Serialize(positionValues)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	cache.Put(position)
	return nil
}
