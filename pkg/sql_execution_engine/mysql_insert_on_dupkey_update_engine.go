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

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
)

const MySQLInsertOnDuplicateKeyUpdate = "mysql-insert-on-duplicate-key-update"

type mysqlInsertOnDuplicateKeyUpdateEngine struct {
	pipelineName string
	db           *sql.DB
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLInsertOnDuplicateKeyUpdate, &mysqlInsertOnDuplicateKeyUpdateEngine{}, false)
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Configure(pipelineName string, data map[string]interface{}) error {
	engine.pipelineName = pipelineName
	return nil
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Init(db *sql.DB) error {
	engine.db = db
	return nil
}

func (engine *mysqlInsertOnDuplicateKeyUpdateEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	if len(msgBatch) == 0 {
		return nil
	}
	return nil
}
