package sql_execution_engine

import (
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/schema_store"
)

const ManualEngine = "manual-engine"

type sqlTemplateContext struct {
	TargetTable *schema_store.Table
	Msg         *core.Msg
}

type manualSQLEngineConfig struct {
	TagInternalTxn bool   `mapstructure:"tag-internal-txn" json:"tag-internal-txn"`
	SQLAnnotation  string `mapstructure:"sql-annotation" json:"sql-annotation"`

	SQLTemplate string   `mapstructure:"sql-template" json:"sql-template"`
	SQLArgExpr  []string `mapstructure:"sql-arg-expr" json:"sql-arg-expr"`
}

type manualSQLEngine struct {
	pipelineName string
	cfg          *manualSQLEngineConfig

	sqlTemplate *template.Template
	db          *sql.DB
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, ManualEngine, &manualSQLEngine{}, false)
}

func (engine *manualSQLEngine) Configure(pipelineName string, data map[string]interface{}) error {
	engine.pipelineName = pipelineName

	cfg := manualSQLEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	engine.cfg = &cfg

	t, err := template.New("sqlTemplate").Parse(cfg.SQLTemplate)
	if err != nil {
		return errors.Trace(err)
	}

	engine.sqlTemplate = t

	return nil
}

func (engine *manualSQLEngine) Init(db *sql.DB) error {
	engine.db = db

	if engine.cfg.TagInternalTxn {
		return utils.InitInternalTxnTags(db)
	}
	return nil
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

	sqlArgs := make([]interface{}, len(engine.cfg.SQLArgExpr))
	for i, field := range engine.cfg.SQLArgExpr {
		sqlArgs[i] = msg.DmlMsg.Data[field]
	}

	templateContext := sqlTemplateContext{
		TargetTable: tableDef,
		Msg:         msg,
	}

	stringBuilder := strings.Builder{}
	if err := engine.sqlTemplate.Execute(&stringBuilder, templateContext); err != nil {
		return errors.Trace(err)
	}

	query := stringBuilder.String()

	if engine.cfg.SQLAnnotation != "" {
		query = fmt.Sprintf("%s%s", engine.cfg.SQLAnnotation, query)
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

	_, err = txn.Exec(query, sqlArgs...)
	if err != nil {
		txn.Rollback()
		return errors.Annotatef(err, "[manualSQLEngine] exec error sql: %v, args: %+v", query, sqlArgs)
	}

	err = txn.Commit()
	if err != nil {
		log.Errorf("[manualSQLEngine] commit error sql: %s, sqlArgs: %v, err: %v", query, sqlArgs, err.Error())
		return errors.Trace(err)
	}
	return nil
}
