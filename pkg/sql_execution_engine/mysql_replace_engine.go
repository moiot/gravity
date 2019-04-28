package sql_execution_engine

import (
	"database/sql"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/schema_store"
)

type MysqlReplaceEngineConfig struct {
	InternalTxnTaggerCfg `mapstructure:",squash"`
}

type mysqlReplaceEngine struct {
	pipelineName string
	cfg          *MysqlReplaceEngineConfig
	db           *sql.DB
}

const MySQLReplaceEngine = "mysql-replace-engine"

var DefaultMySQLReplaceEngineConfig = map[string]interface{}{
	MySQLReplaceEngine: DefaultInternalTxnTaggerCfg,
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLReplaceEngine, &mysqlReplaceEngine{}, false)
}

func (engine *mysqlReplaceEngine) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := MysqlReplaceEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	engine.cfg = &cfg
	engine.pipelineName = pipelineName
	return nil
}

func (engine *mysqlReplaceEngine) Init(db *sql.DB) error {
	engine.db = db

	if engine.cfg.TagInternalTxn {
		return errors.Trace(utils.InitInternalTxnTags(db))
	}

	return nil
}

func (engine *mysqlReplaceEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	currentBatchSize := len(msgBatch)

	if currentBatchSize == 0 {
		return nil
	}

	if currentBatchSize > 1 && msgBatch[0].DmlMsg.Operation == core.Delete {
		return errors.Errorf("[mysql_replace_engine] only support single delete")
	}

	var query string
	var args []interface{}
	var err error

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		query, args, err = GenerateSingleDeleteSQL(msgBatch[0], targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		query, args, err = GenerateReplaceSQLWithMultipleValues(msgBatch, targetTableDef)
		if err != nil {
			data, pks := DebugDmlMsg(msgBatch)
			return errors.Annotatef(err, "query: %v, pb data: %v, pks: %v", query, data, pks)
		}
	}

	return errors.Trace(ExecWithInternalTxnTag(engine.pipelineName, &engine.cfg.InternalTxnTaggerCfg, engine.db, query, args))
}
