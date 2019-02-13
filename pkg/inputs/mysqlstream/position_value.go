package mysqlstream

import (
	"database/sql"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/utils"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

func SetupInitialPosition(db *sql.DB, positionCache position_store.PositionCacheInterface, startPositionSpec *utils.MySQLBinlogPosition) error {
	position, exist, err := positionCache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		dbUtil := utils.NewMySQLDB(db)
		binlogFilePos, gtid, err := dbUtil.GetMasterStatus()
		if err != nil {
			return errors.Trace(err)
		}

		p := &utils.MySQLBinlogPosition{
			BinLogFileName: binlogFilePos.Name,
			BinLogFilePos:  binlogFilePos.Pos,
			BinlogGTID:     gtid.String(),
		}
		// Do not initialize start position.
		// StartPosition is a user configured parameter.
		binlogPositions := helper.BinlogPositionsValue{
			CurrentPosition: p,
		}

		v, err := helper.SerializeBinlogPositionValue(&binlogPositions)
		if err != nil {
			return errors.Trace(err)
		}

		position := position_store.Position{
			Stage:      config.Stream,
			Value:      v,
			UpdateTime: time.Now(),
		}
		if err := positionCache.Put(position); err != nil {
			return errors.Trace(err)
		}

		return errors.Trace(positionCache.Flush())

	}

	runTimePositions, err := helper.DeserializeBinlogPositionValue(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	// reset runTimePositions
	if startPositionSpec != nil {
		if runTimePositions.StartPosition == nil {
			runTimePositions.StartPosition = startPositionSpec
			runTimePositions.CurrentPosition = startPositionSpec
		} else {
			if runTimePositions.StartPosition.BinlogGTID != startPositionSpec.BinlogGTID {
				runTimePositions.StartPosition = startPositionSpec
				runTimePositions.CurrentPosition = startPositionSpec
			}
		}
	}

	v, err := helper.SerializeBinlogPositionValue(runTimePositions)
	if err != nil {
		return errors.Trace(err)
	}
	position.Value = v
	if err := positionCache.Put(position); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(positionCache.Flush())
}

func GetCurrentPositionValue(cache position_store.PositionCacheInterface) (*utils.MySQLBinlogPosition, error) {
	_, current, err := getBinlogPositionsValue(cache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return current, nil
}

func UpdateCurrentPositionValue(cache position_store.PositionCacheInterface, currentPosition *utils.MySQLBinlogPosition) error {
	start, _, err := getBinlogPositionsValue(cache)
	if err != nil {
		return errors.Trace(err)
	}

	v, err := helper.SerializeBinlogPositionValue(&helper.BinlogPositionsValue{StartPosition: start, CurrentPosition: currentPosition})
	if err != nil {
		return errors.Trace(err)
	}

	position := position_store.Position{
		Stage: config.Stream,
		Value: v,
	}

	if err := cache.Put(position); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func ToGoMySQLPosition(p utils.MySQLBinlogPosition) (gomysql.Position, gomysql.MysqlGTIDSet, error) {
	gtidSet, err := gomysql.ParseMysqlGTIDSet(p.BinlogGTID)
	if err != nil {
		return gomysql.Position{}, gomysql.MysqlGTIDSet{}, errors.Trace(err)
	}
	mysqlGTIDSet := *gtidSet.(*gomysql.MysqlGTIDSet)
	return gomysql.Position{Name: p.BinLogFileName, Pos: p.BinLogFilePos}, mysqlGTIDSet, nil
}

func getBinlogPositionsValue(cache position_store.PositionCacheInterface) (*utils.MySQLBinlogPosition, *utils.MySQLBinlogPosition, error) {
	position, exist, err := cache.Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if !exist {
		return nil, nil, errors.New("empty position")
	}

	positions, err := helper.DeserializeBinlogPositionValue(position.Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if positions.CurrentPosition == nil {
		return nil, nil, errors.Errorf("empty currentPosition")
	}

	return positions.StartPosition, positions.CurrentPosition, nil
}
