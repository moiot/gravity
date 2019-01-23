package mysqlstream

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

func IsEventBelongsToMyself(event *replication.RowsEvent, gravityID uint32) bool {
	if id, ok := event.Rows[0][0].(int32); ok {
		if id == int32(gravityID) {
			return true
		}
		return false
	}
	panic("type conversion failed for internal table")
}

func extractSchemaNameFromDDLQueryEvent(p *parser.Parser, ev *replication.QueryEvent) (db, table string, node ast.StmtNode) {
	stmt, err := p.ParseOneStmt(string(ev.Query), "", "")
	if err != nil {
		log.Errorf("sql parser: %s. error: %v", string(ev.Query), err.Error())
		return string(ev.Schema), "", nil
	}

	node = stmt

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		db = v.Name
	case *ast.DropDatabaseStmt:
		db = v.Name
	case *ast.CreateTableStmt:
		db = v.Table.Schema.String()
		table = v.Table.Name.String()
	case *ast.DropTableStmt:
		if len(v.Tables) > 1 {
			log.Fatalf("only support single drop table right now: %v", string(ev.Query))
		}
		db = v.Tables[0].Schema.String()
		table = v.Tables[0].Name.String()
	case *ast.AlterTableStmt:
		db = v.Table.Schema.String()
		table = v.Table.Name.String()
	case *ast.TruncateTableStmt:
		db = v.Table.Schema.String()
		table = v.Table.Name.String()
	}
	if db == "" {
		db = string(ev.Schema)
	}
	return
}
