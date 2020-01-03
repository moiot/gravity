package tidb_kafka

import (
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/protocol/tidb"
	"github.com/moiot/gravity/pkg/utils"
)

const inputStreamKey = "tidbbinlog"

func ParseTimeStamp(tso uint64) uint64 {
	// https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/tso_command.go#L49
	// timstamp in seconds format
	return (tso >> 18) / 1000
}

func parseDDL(p *parser.Parser, binlog tidb.Binlog) (db, table []string, node []ast.StmtNode) {
	stmt, err := p.ParseOneStmt(string(binlog.DdlData.DdlQuery), "", "")
	if err != nil {
		log.Errorf("sql parser: %s. error: %v", string(binlog.DdlData.DdlQuery), err.Error())
		return []string{""}, []string{""}, []ast.StmtNode{nil}
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		db = append(db, v.Name)
		table = append(table, "")
		node = append(node, stmt)
	case *ast.DropDatabaseStmt:
		db = append(db, v.Name)
		table = append(table, "")
		node = append(node, stmt)
	case *ast.CreateTableStmt:
		db = append(db, v.Table.Schema.String())
		table = append(table, v.Table.Name.String())
		node = append(node, stmt)
	case *ast.DropTableStmt:
		for i := range v.Tables {
			db = append(db, v.Tables[i].Schema.String())
			table = append(table, v.Tables[i].Name.String())
			dropTableStmt := *v
			dropTableStmt.Tables = nil
			dropTableStmt.Tables = append(dropTableStmt.Tables, v.Tables[i])
			node = append(node, &dropTableStmt)
		}
	case *ast.AlterTableStmt:
		db = append(db, v.Table.Schema.String())
		table = append(table, v.Table.Name.String())
		node = append(node, stmt)
	case *ast.TruncateTableStmt:
		db = append(db, v.Table.Schema.String())
		table = append(table, v.Table.Name.String())
		node = append(node, stmt)
	case *ast.RenameTableStmt:
		db = append(db, v.OldTable.Schema.String())
		table = append(table, v.OldTable.Name.String())
		node = append(node, stmt)
	default:
		db = append(db, "")
		table = append(table, "")
		node = append(node, stmt)
	}
	if len(db) == 1 && db[0] == "" && binlog.DdlData.SchemaName != nil {
		db[0] = *binlog.DdlData.SchemaName
	}
	return
}

func NewBarrierMsg() *core.Msg {
	return &core.Msg{
		Type:           core.MsgCtl,
		Timestamp:      time.Now(),
		Done:           make(chan struct{}),
		InputStreamKey: utils.NewStringPtr(inputStreamKey),
		Phase: core.Phase{
			Start: time.Now(),
		},
	}
}
