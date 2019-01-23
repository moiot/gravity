package mysqlstream

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/inputs/helper"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

func InitPositionCache(positionCache *position_store.PositionCache, startPositionSpec *utils.MySQLBinlogPosition) error {
	position := positionCache.Get()
	runTimePositions, err := helper.DeserializeBinlogPositions(position.Value)
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

	v, err := helper.SerializeBinlogPositions(runTimePositions)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	positionCache.Put(position)
	return errors.Trace(positionCache.Flush())
}

func GetCurrentGTID(cache *position_store.PositionCache) (string, error) {
	currentPosition, err := GetCurrentPosition(cache)
	if err != nil {
		return "", errors.Trace(err)
	}
	return currentPosition.BinlogGTID, nil
}

func GetCurrentPosition(cache *position_store.PositionCache) (*utils.MySQLBinlogPosition, error) {
	position := cache.Get()
	positions, err := helper.DeserializeBinlogPositions(position.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if positions.CurrentPosition != nil {
		return nil, errors.Errorf("empty currentPosition")
	}

	return positions.CurrentPosition, nil
}

func GetBinlogPositions(cache *position_store.PositionCache) (*utils.MySQLBinlogPosition, *utils.MySQLBinlogPosition, error) {
	position := cache.Get()
	positions, err := helper.DeserializeBinlogPositions(position.Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if positions.CurrentPosition != nil {
		return nil, nil, errors.Errorf("empty currentPosition")
	}

	if positions.StartPosition != nil {
		return nil, nil, errors.Errorf("empty start position")
	}

	return positions.StartPosition, positions.CurrentPosition, nil
}

func ToGoMySQLPosition(p utils.MySQLBinlogPosition) (gomysql.Position, gomysql.MysqlGTIDSet, error) {
	gtidSet, err := gomysql.ParseMysqlGTIDSet(p.BinlogGTID)
	if err != nil {
		return gomysql.Position{}, gomysql.MysqlGTIDSet{}, errors.Trace(err)
	}
	mysqlGTIDSet := *gtidSet.(*gomysql.MysqlGTIDSet)
	return gomysql.Position{Name: p.BinLogFileName, Pos: p.BinLogFilePos}, mysqlGTIDSet, nil
}
