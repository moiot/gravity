package mysqlbatch

import (
	"database/sql"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"

	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mysql"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/position_store"
	"github.com/moiot/gravity/schema_store"
)

// NewMsg creates a job, it converts sql.NullString to interface{}
// based on the column type.
// If the column type is time, then we parse the time
func NewMsg(
	rowPtrs []interface{},
	columnTypes []*sql.ColumnType,
	sourceTableDef *schema_store.Table,
	callbackFunc core.AfterMsgCommitFunc,
	position position_store.MySQLTablePosition) *core.Msg {

	columnDataMap := mysql.SQLDataPtrs2Val(rowPtrs, columnTypes)
	msg := core.Msg{
		Host:      "",
		Database:  sourceTableDef.Schema,
		Table:     sourceTableDef.Name,
		Timestamp: time.Now(),
	}

	dmlMsg := &core.DMLMsg{}
	dmlMsg.Operation = core.Insert
	dmlMsg.Data = columnDataMap

	// pk related
	pkColumns := sourceTableDef.PrimaryKeyColumns

	pkDataMap, err := mysql.GenPrimaryKeys(pkColumns, columnDataMap)
	if err != nil {
		log.Warnf("failed to generate primary keys, worker route will always be the same, err: %v", err)
	}

	var pkColumnsString []string
	for i := range pkColumns {
		pkColumnsString = append(pkColumnsString, pkColumns[i].Name)
	}
	dmlMsg.Pks = pkDataMap

	msg.DmlMsg = dmlMsg
	msg.Type = core.MsgDML
	msg.InputStreamKey = utils.NewStringPtr(utils.TableIdentity(sourceTableDef.Schema, sourceTableDef.Name))
	msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
	msg.Done = make(chan struct{})
	msg.AfterCommitCallback = callbackFunc
	msg.InputContext = position
	msg.Metrics = core.Metrics{
		MsgCreateTime: time.Now(),
	}
	return &msg
}

func NewCreateTableMsg(parser *parser.Parser, table *schema_store.Table, createTblStmt string) *core.Msg {
	stmt, err := parser.ParseOneStmt(createTblStmt, "", "")
	if err != nil {
		log.Fatal(errors.Trace(err))
	}
	msg := core.Msg{
		Host:      "",
		Database:  table.Schema,
		Table:     table.Name,
		Timestamp: time.Now(),
		DdlMsg: &core.DDLMsg{
			Statement: createTblStmt,
			AST:       stmt.(ast.DDLNode),
		},
	}

	msg.Type = core.MsgDDL
	msg.InputStreamKey = utils.NewStringPtr(utils.TableIdentity(table.Schema, table.Name))
	msg.OutputStreamKey = utils.NewStringPtr("")
	msg.Done = make(chan struct{})
	msg.Metrics = core.Metrics{
		MsgCreateTime: time.Now(),
	}
	return &msg
}

func NewCloseInputStreamMsg(sourceTableDef *schema_store.Table) *core.Msg {
	msg := core.Msg{
		Type:            core.MsgCloseInputStream,
		InputStreamKey:  utils.NewStringPtr(utils.TableIdentity(sourceTableDef.Schema, sourceTableDef.Name)),
		OutputStreamKey: utils.NewStringPtr(""),
		Done:            make(chan struct{}),
	}
	return &msg
}
