package sql_execution_engine

import (
	"database/sql"

	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"fmt"

	"github.com/moiot/gravity/schema_store"
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

	var sql string
	var args []interface{}
	var err error

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		sql, args, err = GenerateSingleDeleteSQL(msgBatch[0], targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		sql, args, err = GenerateReplaceSQLWithMultipleValues(msgBatch, targetTableDef)
		if err != nil {
			data, pks := DebugDmlMsg(msgBatch)
			return errors.Annotatef(err, "sql: %v, pb data: %v, pks: %v", sql, data, pks)
		}
	}

	if engine.cfg.SQLAnnotation != "" {
		sql = fmt.Sprintf("%s%s", engine.cfg.SQLAnnotation, sql)
	}

	txn, err := engine.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	if engine.cfg.TagInternalTxn {
		_, err := txn.Exec(utils.GenerateTxnTagSQL())
		if err != nil {
			return errors.Trace(err)
		}
	}

	result, err := txn.Exec(sql, args...)
	if err != nil {
		txn.Rollback()
		return errors.Annotatef(err, "sql: %v, args: %+v", sql, args)
	}

	// log all the delete information
	if msgBatch[0].DmlMsg.Operation == core.Delete {
		nrDeleted, err := result.RowsAffected()
		if err != nil {
			log.Warnf("[mysqlReplaceEngine]: %v", err.Error())
		}

		log.Infof("[mysqlReplaceEngine] singleDelete %s. args: %+v. rows affected: %d", sql, args, nrDeleted)
	}

	return errors.Trace(txn.Commit())
}
