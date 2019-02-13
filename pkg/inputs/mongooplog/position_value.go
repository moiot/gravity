package mongooplog

import (
	"time"

	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type OplogPositionValue struct {
	StartPosition   *config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition *config.MongoPosition `json:"current_position" bson:"current_position"`
}

func Deserialize(v string) (*OplogPositionValue, error) {
	positions := OplogPositionValue{}
	if err := myJson.UnmarshalFromString(v, &positions); err != nil {
		return nil, errors.Trace(err)
	}
	return &positions, nil
}

func Serialize(oplogPositionValue *OplogPositionValue) (string, error) {

	s, err := myJson.MarshalToString(oplogPositionValue)
	if err != nil {
		return "", errors.Trace(err)
	}
	return s, nil
}

func SetupInitialPosition(cache position_store.PositionCacheInterface, startPositionInSpec *config.MongoPosition) error {
	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		var currentPosition *config.MongoPosition
		if startPositionInSpec == nil {
			p := config.MongoPosition(0)
			currentPosition = &p
		} else {
			currentPosition = startPositionInSpec
		}

		positionValue := OplogPositionValue{
			CurrentPosition: currentPosition,
			StartPosition:   startPositionInSpec,
		}
		v, err := Serialize(&positionValue)
		if err != nil {
			return errors.Trace(err)
		}
		position := position_store.Position{
			Stage:      config.Stream,
			Value:      v,
			UpdateTime: time.Now(),
		}

		if err := cache.Put(position); err != nil {
			return errors.Trace(err)
		}

		return errors.Trace(cache.Flush())
	}

	positionValues, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	// reset runtimePositions
	if startPositionInSpec != nil {
		if positionValues.StartPosition == nil {
			positionValues.StartPosition = startPositionInSpec
			positionValues.CurrentPosition = startPositionInSpec
		} else {
			if *positionValues.StartPosition != *startPositionInSpec {
				positionValues.StartPosition = startPositionInSpec
				positionValues.CurrentPosition = startPositionInSpec
			}
		}
	}

	v, err := Serialize(positionValues)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	if err := cache.Put(position); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cache.Flush())
}

func GetPositionValue(cache position_store.PositionCacheInterface) (*OplogPositionValue, error) {

	position, exist, err := cache.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !exist {
		return nil, errors.Errorf("empty position")
	}

	oplogPositionValue, err := Deserialize(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return oplogPositionValue, nil
}

func UpdateCurrentPositionValue(cache position_store.PositionCacheInterface, positionValue *config.MongoPosition) error {

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

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

	return errors.Trace(cache.Put(position))
}
