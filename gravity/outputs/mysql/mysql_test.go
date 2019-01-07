package mysql

import (
	"testing"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

func TestConfigure(t *testing.T) {
	r := require.New(t)

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
