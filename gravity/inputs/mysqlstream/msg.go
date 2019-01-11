package mysqlstream

import (
	"strings"
	"time"

	"github.com/pingcap/parser/ast"

	"github.com/moiot/gravity/pkg/mysql"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/schema_store"
)

type binlogOp string

const (
	insert           binlogOp = "insert"
	update           binlogOp = "update"
	updatePrimaryKey binlogOp = "updatePrimaryKey"
	del              binlogOp = "delete"
	ddl              binlogOp = "ddl"
	xid              binlogOp = "xid"
	barrier          binlogOp = "barrier"
)

type inputContext struct {
	op       binlogOp
	position utils.MySQLBinlogPosition
}

func NewInsertMsgs(
	host string,
	database string,
	table string,
	ts int64,
	ev *replication.RowsEvent,
	tableDef *schema_store.Table) ([]*core.Msg, error) {

	msgs := make([]*core.Msg, len(ev.Rows))
	columns := tableDef.Columns
	pkColumns := tableDef.PrimaryKeyColumns

	pkColumnNames := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		pkColumnNames[i] = c.Name
	}

	for rowIndex, dataRow := range ev.Rows {

		if len(dataRow) != len(columns) {
			log.Warnf("insert %s.%s columns and data mismatch in length: %d vs %d, table %v",
				ev.Table.Schema, ev.Table.Table, len(columns), len(dataRow), tableDef)
		}
		msg := core.Msg{
			Type:         core.MsgDML,
			Host:         host,
			Database:     database,
			Table:        table,
			Timestamp:    time.Unix(ts, 0),
			InputContext: inputContext{op: insert},
			Metrics: core.Metrics{
				MsgCreateTime: time.Now(),
			},
		}

		dmlMsg := &core.DMLMsg{}
		dmlMsg.Operation = core.Insert

		data := make(map[string]interface{})
		for i := 0; i < len(dataRow); i++ {
			data[columns[i].Name] = deserialize(dataRow[i], columns[i])
		}
		dmlMsg.Data = data
		pks, err := mysql.GenPrimaryKeys(pkColumns, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dmlMsg.Pks = pks
		msg.DmlMsg = dmlMsg
		msg.Done = make(chan struct{})
		msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
		msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
		msgs[rowIndex] = &msg
	}
	return msgs, nil
}

func NewUpdateMsgs(
	host string,
	database string,
	table string,
	ts int64,
	ev *replication.RowsEvent,
	tableDef *schema_store.Table) ([]*core.Msg, error) {

	var msgs []*core.Msg
	columns := tableDef.Columns
	pkColumns := tableDef.PrimaryKeyColumns
	pkColumnNames := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		pkColumnNames[i] = c.Name
	}
	for rowIndex := 0; rowIndex < len(ev.Rows); rowIndex += 2 {
		oldDataRow := ev.Rows[rowIndex]
		newDataRow := ev.Rows[rowIndex+1]

		if len(oldDataRow) != len(newDataRow) {
			return nil, errors.Errorf("update %s.%s data mismatch in length: %d vs %d",
				tableDef.Schema, tableDef.Name, len(oldDataRow), len(newDataRow))
		}

		if len(oldDataRow) != len(columns) {
			log.Warnf("update %s.%s columns and data mismatch in column length: %d vs, old data length: %d",
				tableDef.Schema, tableDef.Name, len(columns), len(oldDataRow))
		}

		data := make(map[string]interface{})
		old := make(map[string]interface{})
		pkUpdate := false
		for i := 0; i < len(oldDataRow); i++ {
			data[columns[i].Name] = deserialize(newDataRow[i], columns[i])
			old[columns[i].Name] = deserialize(oldDataRow[i], columns[i])

			if columns[i].IsPrimaryKey && data[columns[i].Name] != old[columns[i].Name] {
				pkUpdate = true
			}
		}

		if !pkUpdate {
			msg := core.Msg{
				Type:         core.MsgDML,
				Host:         host,
				Database:     database,
				Table:        table,
				Timestamp:    time.Unix(ts, 0),
				InputContext: inputContext{op: update},
				Metrics: core.Metrics{
					MsgCreateTime: time.Now(),
				},
			}

			dmlMsg := &core.DMLMsg{}
			dmlMsg.Operation = core.Update
			pks, err := mysql.GenPrimaryKeys(pkColumns, data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dmlMsg.Pks = pks

			dmlMsg.Data = data
			dmlMsg.Old = old

			msg.DmlMsg = dmlMsg
			msg.Done = make(chan struct{})
			msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
			msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
			msgs = append(msgs, &msg)
		} else {
			// first delete old row
			msgDelete := core.Msg{
				Type:         core.MsgDML,
				Host:         host,
				Database:     database,
				Table:        table,
				Timestamp:    time.Unix(ts, 0),
				InputContext: inputContext{op: updatePrimaryKey},
				Metrics: core.Metrics{
					MsgCreateTime: time.Now(),
				},
			}
			dmlMsg1 := &core.DMLMsg{}
			dmlMsg1.Operation = core.Delete

			pks, err := mysql.GenPrimaryKeys(pkColumns, old)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dmlMsg1.Pks = pks
			dmlMsg1.Data = old
			msgDelete.DmlMsg = dmlMsg1
			msgDelete.Done = make(chan struct{})
			msgDelete.InputStreamKey = utils.NewStringPtr(inputStreamKey)
			msgDelete.OutputStreamKey = utils.NewStringPtr(msgDelete.GetPkSign())
			msgs = append(msgs, &msgDelete)

			// then insert new row
			msgInsert := core.Msg{
				Type:         core.MsgDML,
				Host:         host,
				Database:     database,
				Table:        table,
				Timestamp:    time.Unix(ts, 0),
				InputContext: inputContext{op: updatePrimaryKey},
				Metrics: core.Metrics{
					MsgCreateTime: time.Now(),
				},
			}
			dmlMsg2 := &core.DMLMsg{}
			dmlMsg2.Operation = core.Insert

			pks, err = mysql.GenPrimaryKeys(pkColumns, data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dmlMsg2.Pks = pks

			dmlMsg2.Data = data
			msgInsert.DmlMsg = dmlMsg2
			msgInsert.Done = make(chan struct{})
			msgInsert.InputStreamKey = utils.NewStringPtr(inputStreamKey)
			msgInsert.OutputStreamKey = utils.NewStringPtr(msgInsert.GetPkSign())
			msgs = append(msgs, &msgInsert)
		}
	}
	return msgs, nil
}
func deserialize(raw interface{}, column schema_store.Column) interface{} {
	// fix issue: https://github.com/siddontang/go-mysql/issues/242
	if raw == nil {
		return nil
	}

	ct := strings.ToLower(column.ColType)
	if ct == "text" || ct == "json" {
		return string(raw.([]uint8))
	}

	// https://github.com/siddontang/go-mysql/issues/338
	// binlog itself doesn't specify whether it's signed or not
	if column.IsUnsigned {
		switch t := raw.(type) {
		case int8:
			return uint8(t)
		case int16:
			return uint16(t)
		case int32:
			return uint32(t)
		case int64:
			return uint64(t)
		case int:
			return uint(t)
		default:
			// nothing to do
		}
	}

	return raw
}

func NewDeleteMsgs(
	host string,
	database string,
	table string,
	ts int64,
	ev *replication.RowsEvent,
	tableDef *schema_store.Table) ([]*core.Msg, error) {

	msgs := make([]*core.Msg, len(ev.Rows))
	columns := tableDef.Columns
	pkColumns := tableDef.PrimaryKeyColumns
	pkColumnNames := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		pkColumnNames[i] = c.Name
	}

	for rowIndex, row := range ev.Rows {
		if len(row) != len(columns) {
			return nil, errors.Errorf("delete %s.%s columns and data mismatch in length: %d vs %d",
				tableDef.Schema, tableDef.Name, len(columns), len(row))
		}
		msg := core.Msg{
			Type:         core.MsgDML,
			Host:         host,
			Database:     database,
			Table:        table,
			Timestamp:    time.Unix(ts, 0),
			InputContext: inputContext{op: del},
			Metrics: core.Metrics{
				MsgCreateTime: time.Now(),
			},
		}

		dmlMsg := &core.DMLMsg{}
		dmlMsg.Operation = core.Delete

		data := make(map[string]interface{})
		for i := 0; i < len(columns); i++ {
			data[columns[i].Name] = deserialize(row[i], columns[i])
		}
		dmlMsg.Data = data
		pks, err := mysql.GenPrimaryKeys(pkColumns, data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dmlMsg.Pks = pks
		msg.DmlMsg = dmlMsg
		msg.Done = make(chan struct{})
		msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
		msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
		msgs[rowIndex] = &msg
	}

	return msgs, nil

}

func NewDDLMsg(
	callback core.AfterMsgCommitFunc,
	dbName string,
	table string,
	ast ast.StmtNode,
	ddlSQL string,
	ts int64,
	position utils.MySQLBinlogPosition) *core.Msg {

	return &core.Msg{
		Type:                core.MsgDDL,
		Timestamp:           time.Unix(ts, 0),
		Database:            dbName,
		Table:               table,
		DdlMsg:              &core.DDLMsg{Statement: ddlSQL, AST: ast},
		Done:                make(chan struct{}),
		InputContext:        inputContext{op: ddl, position: position},
		InputStreamKey:      utils.NewStringPtr(inputStreamKey),
		OutputStreamKey:     utils.NewStringPtr(""),
		AfterCommitCallback: callback,
		Metrics: core.Metrics{
			MsgCreateTime: time.Now(),
		},
	}
}

func NewBarrierMsg(ts int64, callback core.AfterMsgCommitFunc) *core.Msg {
	return &core.Msg{
		Type:                core.MsgCtl,
		Timestamp:           time.Unix(ts, 0),
		Done:                make(chan struct{}),
		InputContext:        inputContext{op: barrier},
		InputStreamKey:      utils.NewStringPtr(inputStreamKey),
		OutputStreamKey:     utils.NewStringPtr(""),
		AfterCommitCallback: callback,
		Metrics: core.Metrics{
			MsgCreateTime: time.Now(),
		},
	}
}

func NewXIDMsg(ts int64, callback core.AfterMsgCommitFunc, position utils.MySQLBinlogPosition) *core.Msg {
	return &core.Msg{
		Type:                core.MsgCtl,
		Timestamp:           time.Unix(ts, 0),
		Done:                make(chan struct{}),
		InputContext:        inputContext{op: xid, position: position},
		InputStreamKey:      utils.NewStringPtr(inputStreamKey),
		OutputStreamKey:     utils.NewStringPtr(""),
		AfterCommitCallback: callback,
		Metrics: core.Metrics{
			MsgCreateTime: time.Now(),
		},
	}
}
