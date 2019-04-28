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

const MySQLInsertIgnore = "mysql-insert-ignore"

type mysqlInsertIgnoreEngineConfig struct {
	InternalTxnTaggerCfg `mapstructure:",squash"`
}

type mysqlInsertIgnoreEngine struct {
	pipelineName string
	cfg          *mysqlInsertIgnoreEngineConfig
	db           *sql.DB
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLInsertIgnore, &mysqlInsertIgnoreEngine{}, false)
}

func (engine *mysqlInsertIgnoreEngine) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := mysqlInsertIgnoreEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}
	engine.cfg = &cfg
	engine.pipelineName = pipelineName
	return nil
}

func (engine *mysqlInsertIgnoreEngine) Init(db *sql.DB) error {
	engine.db = db

	if engine.cfg.TagInternalTxn {
		return errors.Trace(utils.InitInternalTxnTags(db))
	}

	return nil
}

func (engine *mysqlInsertIgnoreEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	if len(msgBatch) == 0 {
		return nil
	}

	var query string
	var args []interface{}
	var err error

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		if len(msgBatch) > 1 {
			return errors.Errorf("batch size > 1 for delete")
		}
		query, args, err = GenerateSingleDeleteSQL(msgBatch[0], targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		query, args, err = GenerateInsertIgnoreSQL(msgBatch, targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(ExecWithInternalTxnTag(engine.pipelineName, &engine.cfg.InternalTxnTaggerCfg, engine.db, query, args))
}
