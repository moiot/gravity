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
	StartPosition   config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition config.MongoPosition `json:"current_position" bson:"current_position"`
}

func OplogPositionValueEncoder(v interface{}) (string, error) {
	s, err := myJson.MarshalToString(v)
	if err != nil {
		return "", errors.Trace(err)
	}
	return s, nil
}

func OplogPositionValueDecoder(v string) (interface{}, error) {
	positions := OplogPositionValue{}
	if err := myJson.UnmarshalFromString(v, &positions); err != nil {
		return nil, errors.Trace(err)
	}
	return positions, nil
}

func SetupInitialPosition(cache position_store.PositionCacheInterface, startPositionInSpec config.MongoPosition) error {
	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		var currentPosition config.MongoPosition
		if startPositionInSpec.Empty() {
			p := config.MongoPosition(0)
			currentPosition = p
		} else {
			currentPosition = startPositionInSpec
		}

		positionValue := OplogPositionValue{
			CurrentPosition: currentPosition,
			StartPosition:   startPositionInSpec,
		}

		position := position_store.Position{
			Stage:      config.Stream,
			Value:      positionValue,
			UpdateTime: time.Now(),
		}

		if err := cache.Put(position); err != nil {
			return errors.Trace(err)
		}

		return errors.Trace(cache.Flush())
	}

	positionValue, ok := position.Value.(OplogPositionValue)
	if !ok {
		return errors.Errorf("invalid position type")
	}

	// reset runtimePositions
	if !startPositionInSpec.Empty() {
		if positionValue.StartPosition.Empty() {
			positionValue.StartPosition = startPositionInSpec
			positionValue.CurrentPosition = startPositionInSpec
		} else {
			if positionValue.StartPosition != startPositionInSpec {
				positionValue.StartPosition = startPositionInSpec
				positionValue.CurrentPosition = startPositionInSpec
			}
		}
	}

	position.Value = positionValue
	if err := cache.Put(position); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cache.Flush())
}

func GetPositionValue(cache position_store.PositionCacheInterface) (OplogPositionValue, error) {

	position, exist, err := cache.Get()
	if err != nil {
		return OplogPositionValue{}, errors.Trace(err)
	}

	if !exist {
		return OplogPositionValue{}, errors.Errorf("empty position")
	}

	oplogPositionValue, ok := position.Value.(OplogPositionValue)
	if !ok {
		return OplogPositionValue{}, errors.Errorf("invalid position value type")
	}

	return oplogPositionValue, nil
}

func UpdateCurrentPositionValue(cache position_store.PositionCacheInterface, positionValue config.MongoPosition) error {

	position, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		return errors.Errorf("empty position")
	}

	oplogPositionValue, ok := position.Value.(OplogPositionValue)
	if !ok {
		return errors.Errorf("invalid position value type")
	}

	oplogPositionValue.CurrentPosition = positionValue

	position.Value = oplogPositionValue

	return errors.Trace(cache.Put(position))
}
