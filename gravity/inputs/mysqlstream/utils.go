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

func extractSchemaNameFromDDLQueryEvent(p *parser.Parser, ev *replication.QueryEvent) (db, table string, node ast.DDLNode) {
	stmt, err := p.ParseOneStmt(string(ev.Query), "", "")
	if err != nil {
		log.Fatalf("sql parser error: %v", err.Error())
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		return v.Name, "", v
	case *ast.DropDatabaseStmt:
		return v.Name, "", v
	case *ast.CreateTableStmt:
		return v.Table.Schema.String(), v.Table.Name.String(), v
	case *ast.DropTableStmt:
		if len(v.Tables) > 1 {
			log.Fatalf("only support single drop table right now: %v", string(ev.Query))
		}
		return v.Tables[0].Schema.String(), v.Tables[0].Name.String(), v
	case *ast.AlterTableStmt:
		return v.Table.Schema.String(), v.Table.Name.String(), v
	case *ast.TruncateTableStmt:
		return v.Table.Schema.String(), v.Table.Name.String(), v
	}
	return string(ev.Schema), "", stmt.(ast.DDLNode)
}
