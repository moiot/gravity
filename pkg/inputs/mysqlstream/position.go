package mysqlstream

import (
	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type BinlogPositions struct {
	CurrentPosition *utils.MySQLBinlogPosition `json:"current_position"`
	StartPosition   *utils.MySQLBinlogPosition `json:"start_position"`
}

func Serialize(position *BinlogPositions) (string, error) {
	s, err := myJson.MarshalToString(position)
	if err != nil {
		return "", errors.Trace(err)
	}

	return s, nil
}

func Deserialize(value string) (*BinlogPositions, error) {
	position := BinlogPositions{}
	if err := myJson.UnmarshalFromString(value, &position); err != nil {
		return nil, errors.Trace(err)
	}
	return &position, nil
}

func InitPositionCache(positionCache *position_store.PositionCache, startPositionSpec *utils.MySQLBinlogPosition) error {
	position := positionCache.Get()
	runTimePositions, err := Deserialize(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO finish this
	if startPositionSpec != nil {
		if runTimePositions.StartPosition == nil {
			runTimePositions.StartPosition = startPositionSpec
		} else {
			if runTimePositions.StartPosition.BinlogGTID != startPositionSpec.BinlogGTID {
				// reset position
				runTimePositions.StartPosition = startPositionSpec
				runTimePositions.CurrentPosition = startPositionSpec
			}
		}
	} else {

	}

	v, err := Serialize(runTimePositions)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	positionCache.Put(position)
	return errors.Trace(positionCache.Flush())
}
