package sql_execution_engine

import (
	"database/sql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"fmt"

	"github.com/moiot/gravity/schema_store"
)

type mysqlReplaceEngine struct {
	maxBatchSize int
	db           *sql.DB
}

func NewMySQLReplaceEngine(db *sql.DB) SQlExecutionEngine {
	return &mysqlReplaceEngine{db: db}
}

func (engine *mysqlReplaceEngine) Execute(msgBatch []*core.Msg, targetTableDef *schema_store.Table) error {
	currentBatchSize := len(msgBatch)

	if currentBatchSize == 0 {
		return nil
	}

	if currentBatchSize > 1 && msgBatch[0].DmlMsg.Operation == core.Delete {
		return errors.Errorf("[mysql_replace_engine] only support single delete")
	}

	if currentBatchSize > 1 && msgBatch[0].DdlMsg != nil {
		return errors.Errorf("[mysql_replace_engine] only support single ddl")
	}

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		return engine.execSingleDelete(msgBatch[0], targetTableDef)
	}

	if msgBatch[0].DdlMsg != nil {
		return engine.execSingleDDL(msgBatch[0])
	}

	return engine.execBatchUsingSingleReplace(msgBatch, targetTableDef)

}

func (engine *mysqlReplaceEngine) execSingleDDL(msg *core.Msg) error {
	log.Infof("[mysql_replace_engine] exec ddl: %v", msg.DdlMsg.Statement)
	sql := msg.DdlMsg.Statement
	dbName := msg.Database
	tx, err := engine.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	if dbName != "" {
		if _, err := tx.Exec(fmt.Sprintf("USE %s", dbName)); err != nil {
			return errors.Trace(err)
		}

	}
	_, err = tx.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(tx.Commit())
}

func (engine *mysqlReplaceEngine) execSingleDelete(msg *core.Msg, tableDef *schema_store.Table) error {
	sql, arg, err := GenerateSingleDeleteSQL(msg, tableDef)
	if err != nil {
		return errors.Trace(err)
	}

	r, err := engine.db.Exec(sql, arg...)
	if err == nil {
		deleted, err := r.RowsAffected()
		if err != nil {
			log.Warn(err)
		} else {
			log.Infof("[mysqlReplaceEngine.execSingleDelete] %s. args: %v. rows affected: %d", sql, arg, deleted)
		}
	}
	return errors.Trace(err)
}

func (engine *mysqlReplaceEngine) execBatchUsingSingleReplace(msgBatch []*core.Msg, tableDef *schema_store.Table) error {
	var sql string
	var arg []interface{}
	var err error

	if engine.maxBatchSize == 1 {
		sql, arg, err = GenerateSingleReplaceSQL(msgBatch[0], tableDef)
		if err != nil {
			data, pks := DebugDmlMsg(msgBatch)
			return errors.Annotatef(err, "sql: %v, pb data: %v, pks: %v", sql, data, pks)
		}
	} else {
		sql, arg, err = GenerateReplaceSQLWithMultipleValues(msgBatch, tableDef)
		if err != nil {
			data, pks := DebugDmlMsg(msgBatch)
			return errors.Annotatef(err, "sql: %v, pb data: %v, pks: %v", sql, data, pks)
		}
	}

	_, err = engine.db.Exec(sql, arg...)
	return errors.Annotatef(err, "sql: %v, args: %v", sql, arg)
}
