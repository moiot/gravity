package mysql

import (
	"bytes"
	"fmt"
	"testing"

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
		p, err := registry.GetPlugin(registry.OutputPlugin, Name)
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
		p, err := registry.GetPlugin(registry.OutputPlugin, Name)
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
	s := `create table t(dt datetime(6) not null default current_timestamp(6) on update current_timestamp(6))`
	p := parser.New()
	stmt, err := p.ParseOneStmt(s, "", "")
	require.NoError(t, err)
	b := bytes.NewBufferString("")
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, b)
	require.NoError(t, stmt.Restore(ctx))
	fmt.Println(b.String())
}
