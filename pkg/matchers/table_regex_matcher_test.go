package matchers

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
)

func TestTableRegexMatcher_Match(t *testing.T) {
	tests := []struct {
		name   string
		config string
		table  string
		want   bool
	}{
		{
			"single",
			`"^t_\\d+$"`,
			"t_01",
			true,
		},
		{
			"multiple",
			`["^a.*$", "^t_\\d+$"]`,
			"t_01",
			true,
		},
		{
			"multiple not match",
			`["^a.*$", "^t_\\d+$"]`,
			"t_0a",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := make(map[string]interface{})
			_, err := toml.Decode(TableRegexMatcherName+" = "+tt.config, &config)
			require.NoError(t, err)
			m, err := NewMatchers(config)
			require.NoError(t, err)
			require.Equal(t, tt.want, m.Match(&core.Msg{
				Table: tt.table,
			}))
		})
	}
}
