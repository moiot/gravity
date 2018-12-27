package mysqlstream

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
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

func extractSchemaNameFromDDLQueryEvent(ev *replication.QueryEvent) string {
	eventSchema := string(ev.Schema)
	if eventSchema != "" {
		return eventSchema
	}

	sql := string(ev.Query)
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		log.Fatalf("sql parser error: %v", err.Error())
	}

	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		return v.Name
	case *ast.DropDatabaseStmt:
		return v.Name
	case *ast.CreateTableStmt:
		return v.Table.Schema.String()
	case *ast.DropTableStmt:
		if len(v.Tables) > 1 {
			log.Fatalf("only support single drop table right now: %v", sql)
		}

		return v.Tables[0].Schema.String()
	case *ast.AlterTableStmt:
		return v.Table.Schema.String()
	case *ast.TruncateTableStmt:
		return v.Table.Schema.String()
	case *ast.AnalyzeTableStmt:
		return v.TableNames[0].Name.String()
	default:
		log.Fatalf("stmt not supported: %v", sql)
	}
	return ""
}
