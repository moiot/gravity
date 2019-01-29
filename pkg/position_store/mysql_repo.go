package position_store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/utils/retry"
	log "github.com/sirupsen/logrus"
)

var (
	oldTable                     = `cluster_gravity_binlog_position`
	positionTableName            = `gravity_positions`
	positionFullTableName        = fmt.Sprintf("%s.%s", consts.GravityDBName, positionTableName)
	createPositionTableStatement = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		name VARCHAR(255) NOT NULL,
		stage VARCHAR(20) NOT NULL DEFAULT '%s',
		position MEDIUMTEXT,
        created_at DATETIME NOT NULL DEFAULT NOW(),
        updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW(),
		PRIMARY KEY(name)
	)
`, positionFullTableName, config.Unknown)

	addStateStmt = fmt.Sprintf("ALTER TABLE %s ADD COLUMN stage VARCHAR(20) NOT NULL DEFAULT '%s';", positionFullTableName, config.Stream)
)

func IsPositionStoreEvent(schemaName string, tableName string) bool {
	return (schemaName == consts.GravityDBName || schemaName == "drc") && tableName == positionTableName
}

type mysqlPositionRepo struct {
	db         *sql.DB
	annotation string
}

func (repo *mysqlPositionRepo) Get(pipelineName string) (Position, error) {
	var value string
	var stage string
	var lastUpdate time.Time

	row := repo.db.QueryRow(fmt.Sprintf(
		"%sSELECT position, stage, updated_at FROM %s WHERE name = ?",
		repo.annotation, positionFullTableName), pipelineName)
	if err := row.Scan(&value, &stage, &lastUpdate); err != nil {
		return Position{}, errors.Trace(err)
	}

	return Position{Name: pipelineName, Stage: config.InputMode(stage), Value: value, UpdateTime: lastUpdate}, nil
}

func (repo *mysqlPositionRepo) Put(pipelineName string, position Position) error {
	stmt := fmt.Sprintf(
		"%sINSERT INTO %s(name, stage, position) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE stage = ?, position = ?",
		repo.annotation, positionFullTableName)
	_, err := repo.db.Exec(stmt, pipelineName, position.Stage, position.Value, position.Stage, position.Value)
	return errors.Trace(err)
}

func NewMySQLRepo(pipelineName string, dbConfig *utils.DBConfig, annotation string) (PositionRepo, error) {
	db, err := utils.CreateDBConnection(dbConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	repo := mysqlPositionRepo{db: db, annotation: annotation}

	_, err = db.Exec(fmt.Sprintf("%sCREATE DATABASE IF NOT EXISTS %s", annotation, consts.GravityDBName))
	if err != nil {
		return nil, errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%sDROP TABLE IF EXISTS %s.%s", annotation, consts.GravityDBName, oldTable))
	if err != nil {
		return nil, errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%s%s", annotation, createPositionTableStatement))
	if err != nil {
		return nil, errors.Trace(err)
	}

	// db migration logic when "stage" does not exist.
	err = retry.Do(func() error {
		row := db.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'stage'", consts.GravityDBName, positionTableName)
		var cnt int
		err := row.Scan(&cnt)
		if err != nil {
			return errors.Trace(err)
		}

		if cnt == 1 {
			log.Debug("[mysqlPositionRepo] stage column already exists")
			return nil
		}

		_, err = db.Exec(addStateStmt)
		return err

	}, 3, retry.DefaultSleep)

	return &repo, nil
}
