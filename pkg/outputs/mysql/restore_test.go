package mysql

import (
	"testing"

	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/parser/ast"
)

func TestRestoreCreateTblStmt(t *testing.T) {
	cases := []struct {
		name        string
		ifNotExists bool
		input       string
		expect      string
	}{
		{
			name:        "add ifNotExists",
			ifNotExists: true,
			input:       "CREATE TABLE `t` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`fl` float(5,1) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			expect:      "CREATE TABLE IF NOT EXISTS `t` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`fl` float(5,1) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "multi line",
			input: `CREATE TABLE t5 (
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  thirdparty tinyint(2) NOT NULL DEFAULT '0' COMMENT '第三方编号',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='第三方调用记录'`,
			expect: "CREATE TABLE `t5` (" + `
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  thirdparty tinyint(2) NOT NULL DEFAULT '0' COMMENT '第三方编号',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='第三方调用记录'`,
		},
	}

	r := require.New(t)
	p := parser.New()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(c.input, "", "")
			r.NoError(err)
			tableStmt := stmt.(*ast.CreateTableStmt)
			tableStmt.IfNotExists = c.ifNotExists
			ret := RestoreCreateTblStmt(tableStmt)
			r.Equal(c.expect, ret)
		})
	}
}

func TestRestoreAlterTblStmt(t *testing.T) {
	cases := []struct {
		name      string
		tableName string
		input     string
		expect    string
	}{
		{
			name:      "rewrite table name",
			tableName: "t2",
			input:     "alter table test.t add column ii int(10)",
			expect:    "ALTER TABLE `test`.`t2` add column ii int(10)",
		},
		{
			name: "multi line",
			input: `alter table test.t 
add column ii int(10)`,
			expect: "ALTER TABLE `test`.`t` " + `
add column ii int(10)`,
		},
	}

	r := require.New(t)
	p := parser.New()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(c.input, "", "")
			r.NoError(err)
			alterTableStmt := stmt.(*ast.AlterTableStmt)
			if c.tableName != "" {
				alterTableStmt.Table.Name.O = c.tableName
			}
			ret := RestoreAlterTblStmt(alterTableStmt)
			r.Equal(c.expect, ret)
		})
	}
}
