package sql_execution_engine

import (
	"database/sql"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/schema_store"
)

type mysqlReplaceEngineConfig struct {
	TagInternalTxn bool   `mapstructure:"tag-internal-txn" json:"tag-internal-txn"`
	SQLAnnotation  string `mapstructure:"sql-annotation" json:"sql-annotation"`
}

type mysqlReplaceEngine struct {
	pipelineName string
	cfg          *mysqlReplaceEngineConfig
	db           *sql.DB
}

const MySQLReplaceEngine = "mysql-replace-engine"

var DefaultMySQLReplaceEngineConfig = map[string]interface{}{
	MySQLReplaceEngine: map[string]interface{}{
		"tag-internal-txn": false,
		"sql-annotation":   "",
	},
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, MySQLReplaceEngine, &mysqlReplaceEngine{}, false)
}

func (engine *mysqlReplaceEngine) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := mysqlReplaceEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	engine.cfg = &cfg
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

	if engine.cfg.SQLAnnotation != "" {
		query = SQLWithAnnotation(query, engine.cfg.SQLAnnotation)
	}

	//
	// Do not open a txn explicitly when TagInternalExn is false
	//
	if !engine.cfg.TagInternalTxn {
		result, err := engine.db.Exec(query, args...)
		if err != nil {
			return errors.Trace(err)
		}

		logOperation(msgBatch, query, args, result)
		return nil
	}

	//
	// TagInternalTxn is ON
	//
	txn, err := engine.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	result, err := txn.Exec(utils.GenerateTxnTagSQL(engine.pipelineName))
	if err != nil {
		return errors.Trace(err)
	}

	result, err = txn.Exec(query, args...)
	if err != nil {
		txn.Rollback()
		return errors.Annotatef(err, "query: %v, args: %+v", query, args)
	}

	// log all the delete information
	logOperation(msgBatch, query, args, result)

	return errors.Trace(txn.Commit())
}

// Only log delete operation for now.
func logOperation(batch []*core.Msg, query string, args []interface{}, result sql.Result) {
	if len(batch) == 0 {
		return
	}

	if batch[0].DmlMsg.Operation != core.Delete {
		return
	}

	nrDeleted, err := result.RowsAffected()
	if err != nil {
		log.Warnf("[mysqlReplaceEngine]: %v", err.Error())
	}

	log.Infof("[mysqlReplaceEngine] singleDelete %s. args: %+v. rows affected: %d", query, args, nrDeleted)
}
