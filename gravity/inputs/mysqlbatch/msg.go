package mysqlbatch

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/pkg/core"

	"database/sql"

	"github.com/moiot/gravity/pkg/mysql"
	"github.com/moiot/gravity/schema_store"
)

// NewMsg creates a job, it converts sql.NullString to interface{}
// based on the column type.
// If the column type is time, then we parse the time
func NewMsg(
	rowPtrs []interface{},
	columnTypes []*sql.ColumnType,
	sourceTableDef *schema_store.Table,
	callbackFunc core.AfterMsgCommitFunc) *core.Msg {

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
	dmlMsg.PkColumns = pkColumnsString
	dmlMsg.Pks = pkDataMap

	msg.DmlMsg = dmlMsg
	msg.Type = core.MsgDML
	msg.InputStreamKey = utils.NewStringPtr(utils.TableIdentity(sourceTableDef.Schema, sourceTableDef.Name))
	msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
	msg.Done = make(chan struct{})
	msg.AfterCommitCallback = callbackFunc
	msg.Metrics = core.Metrics{
		MsgCreateTime: time.Now(),
	}
	return &msg
}
