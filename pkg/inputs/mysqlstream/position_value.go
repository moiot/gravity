package mysqlstream

import (
	"database/sql"
	"time"

	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/utils"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

func SetupInitialPosition(db *sql.DB, positionCache position_cache.PositionCacheInterface, startPositionSpec *config.MySQLBinlogPosition) error {
	position, exist, err := positionCache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		binlogPositionValue := helper.BinlogPositionsValue{}

		if startPositionSpec != nil {
			start := *startPositionSpec
			current := *startPositionSpec
			binlogPositionValue.StartPosition = &start
			binlogPositionValue.CurrentPosition = &current
		} else {
			dbUtil := utils.NewMySQLDB(db)
			binlogFilePos, gtid, err := dbUtil.GetMasterStatus()
			if err != nil {
				return errors.Trace(err)
			}

			p := config.MySQLBinlogPosition{
				BinLogFileName: binlogFilePos.Name,
				BinLogFilePos:  binlogFilePos.Pos,
				BinlogGTID:     gtid.String(),
			}
			// Do not initialize start position.
			// StartPosition is a user configured parameter.
			binlogPositionValue.CurrentPosition = &p
		}

		position := position_repos.Position{
			PositionMeta: position_repos.PositionMeta{
				Stage:      config.Stream,
				UpdateTime: time.Now(),
			},

			Value: binlogPositionValue,
		}
		if err := positionCache.Put(position); err != nil {
			return errors.Trace(err)
		}

		return errors.Trace(positionCache.Flush())

	}

	runTimePositions, ok := position.Value.(helper.BinlogPositionsValue)
	if !ok {
		return errors.Errorf("invalid position value type")
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

	position.Value = runTimePositions
	if err := positionCache.Put(position); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(positionCache.Flush())
}

func GetCurrentPositionValue(cache position_cache.PositionCacheInterface) (config.MySQLBinlogPosition, error) {
	_, current, err := getBinlogPositionsValue(cache)
	if err != nil {
		return config.MySQLBinlogPosition{}, errors.Trace(err)
	}
	return *current, nil
}

func UpdateCurrentPositionValue(cache position_cache.PositionCacheInterface, currentPosition config.MySQLBinlogPosition) error {
	start, _, err := getBinlogPositionsValue(cache)
	if err != nil {
		return errors.Trace(err)
	}

	binlogPositionValue := helper.BinlogPositionsValue{
		StartPosition:   start,
		CurrentPosition: &currentPosition,
	}

	position := position_repos.Position{
		PositionMeta: position_repos.PositionMeta{
			Stage: config.Stream,
		},

		Value: binlogPositionValue,
	}

	if err := cache.Put(position); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func ToGoMySQLPosition(p config.MySQLBinlogPosition) (gomysql.Position, gomysql.MysqlGTIDSet, error) {
	gtidSet, err := gomysql.ParseMysqlGTIDSet(p.BinlogGTID)
	if err != nil {
		return gomysql.Position{}, gomysql.MysqlGTIDSet{}, errors.Trace(err)
	}
	mysqlGTIDSet := *gtidSet.(*gomysql.MysqlGTIDSet)
	return gomysql.Position{Name: p.BinLogFileName, Pos: p.BinLogFilePos}, mysqlGTIDSet, nil
}

func getBinlogPositionsValue(cache position_cache.PositionCacheInterface) (*config.MySQLBinlogPosition, *config.MySQLBinlogPosition, error) {
	position, exist, err := cache.Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if !exist {
		return nil, nil, errors.New("empty position")
	}

	binlogPositionValue, ok := position.Value.(helper.BinlogPositionsValue)
	if !ok {
		return nil, nil, errors.Errorf("invalid position type")
	}

	if binlogPositionValue.CurrentPosition == nil {
		return nil, nil, errors.Errorf("empty currentPosition")
	}

	return binlogPositionValue.StartPosition, binlogPositionValue.CurrentPosition, nil
}
