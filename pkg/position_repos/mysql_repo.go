/*
 *
 * // Copyright 2019 , Beijing Mobike Technology Co., Ltd.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package position_repos

import (
	"database/sql"
	"fmt"

	"github.com/mitchellh/mapstructure"

	"time"

	"github.com/moiot/gravity/pkg/registry"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/utils"
)

const MySQLRepoName = "mysql-repo"

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

//
// [input.config.position-repo]
// type = "mongo-repo"
// [input.config.position-repo.config]
// annotation = ...
// [input.config.position-repo.config.source]
// host = ...
// port = ...
//
type mysqlPositionRepo struct {
	dbCfg      utils.DBConfig
	db         *sql.DB
	annotation string
}

func NewMySQLRepoConfig(annotation string, source *utils.DBConfig) *config.GenericPluginConfig {
	cfg := config.GenericPluginConfig{
		Type: MySQLRepoName,
		Config: map[string]interface{}{
			"annotation": annotation,
			"source":     source,
		},
	}
	return &cfg
}

func init() {
	registry.RegisterPlugin(registry.PositionRepo, MySQLRepoName, &mysqlPositionRepo{}, false)
}

func (repo *mysqlPositionRepo) Configure(pipelineName string, data map[string]interface{}) error {
	annotation, ok := data["annotation"]
	if ok {
		repo.annotation = annotation.(string)
	}

	source, ok := data["source"]
	if !ok {
		return errors.Errorf("no source configured")
	}

	if err := mapstructure.Decode(source, &repo.dbCfg); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (repo *mysqlPositionRepo) Init() error {
	db, err := utils.CreateDBConnection(&repo.dbCfg)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%sCREATE DATABASE IF NOT EXISTS %s", repo.annotation, consts.GravityDBName))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%sDROP TABLE IF EXISTS %s.%s", repo.annotation, consts.GravityDBName, oldTable))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(fmt.Sprintf("%s%s", repo.annotation, createPositionTableStatement))
	if err != nil {
		return errors.Trace(err)
	}
	repo.db = db
	return nil
}

func (repo *mysqlPositionRepo) Get(pipelineName string) (PositionMeta, string, bool, error) {
	value, stage, lastUpdate, exists, err := repo.getRaw(pipelineName)
	if err != nil {
		return PositionMeta{}, "", exists, errors.Trace(err)
	}

	if exists {
		meta := PositionMeta{Name: pipelineName, Stage: config.InputMode(stage), UpdateTime: lastUpdate}

		if err := meta.Validate(); err != nil {
			return PositionMeta{}, "", true, errors.Trace(err)
		}

		if value == "" {
			return PositionMeta{}, "", true, errors.Errorf("empty value")
		}

		return meta, value, true, nil
	}

	return PositionMeta{}, "", false, nil
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

func (repo *mysqlPositionRepo) Put(pipelineName string, meta PositionMeta, v string) error {
	meta.Name = pipelineName
	if err := meta.Validate(); err != nil {
		return errors.Trace(err)
	}

	if v == "" {
		return errors.Errorf("empty value")
	}

	stmt := fmt.Sprintf(
		"%sINSERT INTO %s(name, stage, position) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE stage = ?, position = ?",
		repo.annotation, positionFullTableName)

	_, err := repo.db.Exec(stmt, pipelineName, meta.Stage, v, meta.Stage, v)
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
