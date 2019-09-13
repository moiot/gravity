package matchers

import (
	"testing"

	"github.com/BurntSushi/toml"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
)

func TestTableMatcher_Match(t *testing.T) {
	tests := []struct {
		name   string
		config string
		table  string
		want   bool
	}{
		{
			"single glob",
			`"t*"`,
			"tt",
			true,
		},
		{
			"multiple glob match",
			`["a*", "t*"]`,
			"tt",
			true,
		},
		{
			"multiple glob not match",
			`["a*", "t*"]`,
			"bb",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := make(map[string]interface{})
			_, err := toml.Decode(TableMatcherName+" = "+tt.config, &config)
			require.NoError(t, err)
			m, err := NewMatchers(config)
			require.NoError(t, err)
			require.Equal(t, tt.want, m.Match(&core.Msg{
				Table: tt.table,
			}))
		})
	}
}
