package sql_execution_engine

import (
	"database/sql"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
)

const MySQLInsertIgnore = "mysql-insert-ignore"

type mysqlInsertIgnoreEngine struct {
	pipelineName string
	db           *sql.DB
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLInsertIgnore, &mysqlInsertIgnoreEngine{}, false)
}

func (engine *mysqlInsertIgnoreEngine) Configure(pipelineName string, data map[string]interface{}) error {
	engine.pipelineName = pipelineName
	return nil
}

func (engine *mysqlInsertIgnoreEngine) Init(db *sql.DB) error {
	engine.db = db
	return nil
}

func (engine *mysqlInsertIgnoreEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	if len(msgBatch) == 0 {
		return nil
	}

	query, args, err := GenerateInsertIgnoreSQL(msgBatch, targetTableDef)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = engine.db.Exec(query, args...)
	return errors.Trace(err)
}
