package mysql

import (
	"testing"

	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/parser/ast"
)

func TestRewriteCreateTableIfNotExists(t *testing.T) {
	ddl := "CREATE TABLE `t` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`fl` float(5,1) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	p := parser.New()
	stmt, err := p.ParseOneStmt(ddl, "", "")
	if err != nil {
		t.Fatalf("sql parser error: %v", err.Error())
	}
	ret := RestoreCreateTblStmt(stmt.(*ast.CreateTableStmt))
	require.Equal(t, "CREATE TABLE IF NOT EXISTS `t` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`fl` float(5,1) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", ret)
}
