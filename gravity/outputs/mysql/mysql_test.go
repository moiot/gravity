package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

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
