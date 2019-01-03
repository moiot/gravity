package sql_execution_engine

import (
	"database/sql"
	"fmt"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"strings"

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

	// assume that the sqlTemplate be something like this:
	// UPDATE %s.%s SET ...
	query := fmt.Sprintf(engine.sqlTemplate, tableDef.Schema, tableDef.Name)
	_, err := engine.db.Exec(query, sqlArgs...)
	if err != nil {
		log.Errorf("[manualSQLEngine] error sql: %s, sqlArgs: %v, err: %v", engine.sqlTemplate, sqlArgs, err.Error())
		return errors.Trace(err)
	}
	return nil
}

func ValidateSQLTemplate(template string) error {
	if strings.Contains(template,"`%s`.`%s`") {
		return nil
	} else {
		return errors.Errorf("invalid sql template: %s", template)
	}
}

func NewManualSQLEngine(db *sql.DB, config MySQLExecutionEngineConfig) SQlExecutionEngine {
	engine := manualSQLEngine{
		db:                db,
		sqlTemplate:       config.SQLTemplate,
		sqlArgsExpression: config.SQLArgExpr,
	}
	return &engine
}
