package sql_execution_engine

import (
	"database/sql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/schema_store"
)

type manualSQLEngine struct {
	sqlTemplate       string
	sqlArgsExpression []string
	db                *sql.DB
}

func (engine *manualSQLEngine) Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error {
	if len(msgBatch) == 0 {
		return nil
	}

	if len(msgBatch) > 1 {
		return errors.Errorf("[manualSQLEngine] only support batch size 1")
	}

	msg := msgBatch[0]

	if msg.Type != core.MsgDML {
		return errors.Errorf("[manualSQLEngine] only support dml")
	}

	sqlArgs := make([]interface{}, len(engine.sqlArgsExpression))
	for i, field := range engine.sqlArgsExpression {
		sqlArgs[i] = msg.DmlMsg.Data[field]
	}

	_, err := engine.db.Exec(engine.sqlTemplate, sqlArgs...)
	if err != nil {
		log.Errorf("[manualSQLEngine] error sql: %s, sqlArgs: %v, err: %v", engine.sqlTemplate, sqlArgs, err.Error())
		return errors.Trace(err)
	}
	return nil
}

func NewManualSQLEngine(db *sql.DB, config MySQLExecutionEngineConfig) SQlExecutionEngine {
	engine := manualSQLEngine{
		db:                db,
		sqlTemplate:       config.SQLTemplate,
		sqlArgsExpression: config.SQLArgExpr,
	}
	return &engine
}
