package position_store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/utils"
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

func (repo *mysqlPositionRepo) Get(pipelineName string) (*PositionWithValueString, bool, error) {
	value, stage, lastUpdate, exists, err := repo.getRaw(pipelineName)
	if err != nil {
		return nil, exists, errors.Trace(err)
	}

	if exists {
		m := PositionWithValueString{Name: pipelineName, Stage: stage, Value: value, UpdateTime: lastUpdate}

		if err := m.Validate(); err != nil {
			return nil, true, errors.Trace(err)
		}
		return &m, true, nil
	}

	return nil, false, nil
}

func (repo *mysqlPositionRepo) getRaw(pipelineName string) (value string, stage string, lastUpdate time.Time, exists bool, err error) {
	row := repo.db.QueryRow(fmt.Sprintf(
		"%sSELECT position, stage, updated_at FROM %s WHERE name = ?",
		repo.annotation, positionFullTableName), pipelineName)

	if err := row.Scan(&value, &stage, &lastUpdate); err != nil {
		if err == sql.ErrNoRows {
			return "", "", lastUpdate, false, nil
		}
		return "", "", lastUpdate, true, errors.Trace(err)
	}
	return value, stage, lastUpdate, true, nil
}

func (repo *mysqlPositionRepo) Put(pipelineName string, m *PositionWithValueString) error {
	if err := m.Validate(); err != nil {
		return errors.Trace(err)
	}

	stmt := fmt.Sprintf(
		"%sINSERT INTO %s(name, stage, position) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE stage = ?, position = ?",
		repo.annotation, positionFullTableName)

	_, err := repo.db.Exec(stmt, pipelineName, m.Stage, m.Value, m.Stage, m.Value)
	return errors.Trace(err)
}

func (repo *mysqlPositionRepo) Delete(pipelineName string) error {
	stmt := fmt.Sprintf("%sDELETE FROM %s WHERE name = ?", repo.annotation, positionFullTableName)
	_, err := repo.db.Exec(stmt, pipelineName)
	return errors.Trace(err)
}

func (repo *mysqlPositionRepo) Close() error {
	return errors.Trace(repo.db.Close())
}

func NewMySQLRepo(dbConfig *utils.DBConfig, annotation string) (PositionRepo, error) {
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

	return &repo, nil
}
