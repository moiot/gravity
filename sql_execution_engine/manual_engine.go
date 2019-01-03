package sql_execution_engine

import (
	"database/sql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"text/template"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/schema_store"
)

type sqlTemplateContext struct {
	TargetTable *schema_store.Table
	Msg *core.Msg
}

type manualSQLEngine struct {
	sqlTemplate       *template.Template
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

	templateContext := sqlTemplateContext{
		TargetTable: tableDef,
		Msg: msg,
	}

	stringBuilder := strings.Builder{}
	if err := engine.sqlTemplate.Execute(&stringBuilder, templateContext); err != nil {
		return errors.Trace(err)
	}

	query := stringBuilder.String()
	_, err := engine.db.Exec(query, sqlArgs...)
	if err != nil {
		log.Errorf("[manualSQLEngine] error sql: %s, sqlArgs: %v, err: %v", query, sqlArgs, err.Error())
		return errors.Trace(err)
	}
	return nil
}


func NewManualSQLEngine(db *sql.DB, config MySQLExecutionEngineConfig) SQlExecutionEngine {
	t, err := template.New("sqlTemplate").Parse(config.SQLTemplate)
	if err != nil {
		log.Fatalf("[manualSQLEngine] failed to parse template: %v", config.SQLTemplate)
	}

	engine := manualSQLEngine{
		db:                db,
		sqlTemplate:       t,
		sqlArgsExpression: config.SQLArgExpr,
	}
	return &engine
}
