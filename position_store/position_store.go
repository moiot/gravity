package position_store

import (
	"database/sql"
	"fmt"

	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/json-iterator/go"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/utils/retry"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

var (
	oldTable                     = `cluster_gravity_binlog_position`
	positionTableName            = `gravity_binlog_position`
	positionFullTableName        = fmt.Sprintf("%s.%s", config.GravityDBName, positionTableName)
	createPositionTableStatement = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		name VARCHAR(255) NOT NULL,
		stage VARCHAR(20) NOT NULL DEFAULT '%s',
		position MEDIUMTEXT,
        created_at DATETIME NOT NULL DEFAULT NOW(),
        updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW(),
		PRIMARY KEY(name)
	)
`, positionFullTableName, stages.InputStageIncremental)

	addStateStmt = fmt.Sprintf("ALTER TABLE %s ADD COLUMN stage VARCHAR(20) NOT NULL DEFAULT '%s';", positionFullTableName, stages.InputStageIncremental)
)

type MySQLPositionStore interface {
	Start() error
	Close()
	Get() utils.MySQLBinlogPosition
	Put(position utils.MySQLBinlogPosition)
	FSync()
}

type MongoPositionStore interface {
	Start() error
	Close()
	Get() config.MongoPosition
	Put(position config.MongoPosition)
}

type MySQLTablePositionStore interface {
	GetStartBinlogPos() (utils.MySQLBinlogPosition, bool)
	PutStartBinlogPos(position utils.MySQLBinlogPosition)

	GetMaxMin(sourceName string) (max MySQLTablePosition, min MySQLTablePosition, ok bool)
	PutMaxMin(sourceName string, max MySQLTablePosition, min MySQLTablePosition)

	GetCurrent(sourceName string) (MySQLTablePosition, bool)
	PutCurrent(sourceName string, pos MySQLTablePosition)

	Start() error
	Close()
}

type ISerializablePosition interface {
	Get() interface{}
	GetRaw() string
	Put(pos interface{})
	PutRaw(pos string)
	Stage() stages.InputStage
}

func PrepareMetaRepo(db *sql.DB, annotation string) error {
	_, err := db.Exec(fmt.Sprintf("%sCREATE DATABASE IF NOT EXISTS %s", annotation, config.GravityDBName))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%sDROP TABLE IF EXISTS %s.%s", annotation, config.GravityDBName, oldTable))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%s%s", annotation, createPositionTableStatement))
	if err != nil {
		return errors.Trace(err)
	}

	err = retry.Do(func() error {
		row := db.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'stage'", config.GravityDBName, positionTableName)
		var cnt int
		err := row.Scan(&cnt)
		if err != nil {
			return errors.Trace(err)
		}

		if cnt == 1 {
			log.Debug("[MysqlMySQLPositionStore.PrepareMetaRepo] state column already exists")
			return nil
		}

		_, err = db.Exec(addStateStmt)
		return err

	}, 3, retry.DefaultSleep)

	return errors.Trace(err)
}
