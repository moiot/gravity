package matchers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
)

func TestDDLRegexMatcher(t *testing.T) {
	cases := []struct {
		name     string
		config   string
		msg      core.Msg
		expected bool
	}{
		{
			"ignore dml",
			".*",
			core.Msg{
				Type: core.MsgDML,
			},
			false,
		},
		{
			"matched ddl",
			"(?i)^DROP\\sTABLE",
			core.Msg{
				Type: core.MsgDDL,
				DdlMsg: &core.DDLMsg{
					Statement: "drop table t",
				},
			},
			true,
		},
		{
			"unmatched ddl",
			"(?i)^DROP\\sTABLE",
			core.Msg{
				Type: core.MsgDDL,
				DdlMsg: &core.DDLMsg{
					Statement: "alter table t add column i int(11)",
				},
			},
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := NewMatchers(map[string]interface{}{
				DDLRegexMatcherName: c.config,
			})
			require.NoError(t, err)
			require.Equal(t, c.expected, m.Match(&c.msg), c.msg)
		})
	}
}
