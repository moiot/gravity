package mysqlstream

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

func IsEventBelongsToMyself(event *replication.RowsEvent, pipelineName string) bool {
	if id, ok := event.Rows[0][0].(string); ok {
		if id == pipelineName {
			return true
		}
		return false
	}
	panic("type conversion failed for internal table")
}

func extractSchemaNameFromDDLQueryEvent(p *parser.Parser, ev *replication.QueryEvent) (db, table []string, node []ast.StmtNode) {
	stmt, err := p.ParseOneStmt(string(ev.Query), "", "")
	if err != nil {
		log.Errorf("sql parser: %s. error: %v", string(ev.Query), err.Error())
		return []string{string(ev.Schema)}, []string{""}, []ast.StmtNode{nil}
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
	default:
		db = append(db, "")
		table = append(table, "")
		node = append(node, stmt)
	}
	if len(db) == 1 && db[0] == "" {
		db[0] = string(ev.Schema)
	}
	return
}
