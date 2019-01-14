package config

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestMapStructureDecodeEmptyStuff(t *testing.T) {
	r := require.New(t)

	t.Run("toml with empty key", func(tt *testing.T) {
		s := `
[some-random-key]
`

		m := make(map[string]interface{})
		_, err := toml.Decode(s, &m)
		r.NoError(err)
		r.Equal(1, len(m))
		_, ok := m["some-random-key"]
		r.True(ok)
		r.Equal(map[string]interface{}{}, m["some-random-key"])
	})

	t.Run("json with empty key", func(tt *testing.T) {
		s := `
{
"some-random-key": {}
}
`
		m := make(map[string]interface{})
		err := json.Unmarshal([]byte(s), &m)
		r.NoError(err)
		r.Equal(1, len(m))
		_, ok := m["some-random-key"]
		r.True(ok)
		r.Equal(map[string]interface{}{}, m["some-random-key"])
	})
}
