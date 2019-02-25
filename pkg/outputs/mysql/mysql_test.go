package mysql

import (
	"bytes"
	"testing"

	"github.com/pingcap/parser/ast"

	"github.com/pingcap/parser/format"

	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

func TestConfigure(t *testing.T) {
	r := require.New(t)

	t.Run("default plugin exists", func(tt *testing.T) {
		p, err := registry.GetPlugin(registry.OutputPlugin, OutputMySQL)
		r.NoError(err)

		r.NoError(p.Configure("test", map[string]interface{}{
			"target": map[string]interface{}{
				"host":     "localhost",
				"username": "root",
			},
		}))

		o, ok := p.(*MySQLOutput)
		r.True(ok)

		r.NotNil(o.sqlExecutionEnginePlugin)
		r.Nil(o.sqlExecutor)
	})

	t.Run("sql plugin without any configuration", func(tt *testing.T) {
		p, err := registry.GetPlugin(registry.OutputPlugin, OutputMySQL)
		r.NoError(err)

		r.NoError(p.Configure("test", map[string]interface{}{
			"target": map[string]interface{}{
				"host":     "localhost",
				"username": "root",
			},
			"sql-engine-config": &config.GenericConfig{
				Type: "mysql-insert-ignore",
			},
		}))

		o, ok := p.(*MySQLOutput)
		r.True(ok)

		r.NotNil(o.sqlExecutionEnginePlugin)

		r.Nil(o.sqlExecutor)
	})

}

func TestSplitBatch(t *testing.T) {
	assert := assert.New(t)

	cases := []struct {
		in  []*core.Msg
		out [][]*core.Msg
	}{
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Delete}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Delete}}}},
		},
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Update}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}},
		},
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Delete}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Delete}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}},
		},
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Delete}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Delete}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}, &core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}},
		},
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Update}}, {DmlMsg: &core.DMLMsg{Operation: core.Delete}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Delete}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}},
		},
		{
			[]*core.Msg{{DmlMsg: &core.DMLMsg{Operation: core.Update}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}, {DmlMsg: &core.DMLMsg{Operation: core.Delete}}, {DmlMsg: &core.DMLMsg{Operation: core.Update}}},
			[][]*core.Msg{{&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}, &core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Delete}}}, {&core.Msg{DmlMsg: &core.DMLMsg{Operation: core.Update}}}},
		},
	}

	for i, c := range cases {
		assert.EqualValuesf(c.out, splitMsgBatchWithDelete(c.in), "index", i)
	}
}

func TestDDL(t *testing.T) {
	s := "ALTER TABLE `foo`.`bar` ADD COLUMN `zoo` VARCHAR(25) DEFAULT NULL COMMENT 'zoo', ADD INDEX `idx_zoo`(`zoo`)"

	expected := []string{
		"ALTER TABLE `foo`.`bar` ADD COLUMN `zoo` VARCHAR(25) DEFAULT NULL COMMENT 'zoo'",
		"ALTER TABLE `foo`.`bar` ADD INDEX `idx_zoo`(`zoo`)",
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(s, "", "")
	require.NoError(t, err)

	alter := stmt.(*ast.AlterTableStmt)
	for i, spec := range alter.Specs {
		n := &ast.AlterTableStmt{
			Table: alter.Table,
			Specs: []*ast.AlterTableSpec{spec},
		}
		b := bytes.NewBufferString("")
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, b)
		require.NoError(t, n.Restore(ctx))
		require.Equal(t, expected[i], b.String())
	}
}
