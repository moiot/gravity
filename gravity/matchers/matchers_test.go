package matchers

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

func TestMatchers(t *testing.T) {
	assert := assert.New(t)

	data := map[string]interface{}{
		SchemaMatcherName: "test_db",
		TableMatcherName:  "test_table",
		dmlOpMatcherName:  "delete",
	}

	matchGroup, err := NewMatchers(data)
	if err != nil {
		assert.FailNow(err.Error())
	}

	assert.Equal(3, len(matchGroup))

	cases := []struct {
		msg     core.Msg
		matched bool
	}{
		{core.Msg{
			Database: "fake",
		},
			false,
		},
		{
			core.Msg{
				Database: "test_db",
				Table:    "fake",
			},
			false,
		},
		{
			core.Msg{
				Database: "fake",
				Table:    "test_table",
			},
			false,
		},
		{
			core.Msg{
				Database: "test_db",
				Table:    "test_table",
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
				},
			},
			false,
		},
		{
			core.Msg{
				Database: "test_db",
				Table:    "test_table",
				DmlMsg: &core.DMLMsg{
					Operation: core.Delete,
				},
			},
			true,
		},
	}

	for _, tt := range cases {
		assert.Equal(tt.matched, matchGroup.Match(&tt.msg))
	}
}
