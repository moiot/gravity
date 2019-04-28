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

package sql_execution_engine

import (
	"database/sql"

	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
)

const MySQLInsertOnDuplicateKeyUpdate = "mysql-insert-on-duplicate-key-update"

type mysqlInsertOnDuplicateKeyUpdateEngineConfig struct {
	InternalTxnTaggerCfg `mapstructure:",squash"`
}

type mysqlInsertOnDuplicateKeyUpdateEngine struct {
	pipelineName string
	cfg          *mysqlInsertOnDuplicateKeyUpdateEngineConfig
	db           *sql.DB
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLInsertOnDuplicateKeyUpdate, &mysqlInsertOnDuplicateKeyUpdateEngine{}, false)
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := mysqlInsertOnDuplicateKeyUpdateEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	engine.cfg = &cfg
	engine.pipelineName = pipelineName
	return nil
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Init(db *sql.DB) error {
	engine.db = db

	if engine.cfg.TagInternalTxn {
		return errors.Trace(utils.InitInternalTxnTags(db))
	}

	return nil
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	if len(msgBatch) == 0 {
		return nil
	}

	if len(msgBatch) > 1 {
		return errors.Errorf("batch size > 1 not supported")
	}

	var query string
	var args []interface{}
	var err error

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		query, args, err = GenerateSingleDeleteSQL(msgBatch[0], targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
	}

	query, args, err = GenerateInsertOnDuplicateKeyUpdate(msgBatch, targetTableDef)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(ExecWithInternalTxnTag(engine.pipelineName, &engine.cfg.InternalTxnTaggerCfg, engine.db, query, args))
}
