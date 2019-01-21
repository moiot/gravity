package filters

import (
	"testing"

	"github.com/moiot/gravity/pkg/config"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

func TestRejectFilter(t *testing.T) {
	assert := assert.New(t)

	data := []config.GenericConfig{
		{
			Type: "reject",
			Config: map[string]interface{}{
				"match-schema": "test_db",
				"match-table":  "test_table_1",
			},
		},
	}

	filters, err := NewFilters(data)
	if err != nil {
		assert.FailNow(err.Error())
	}

	assert.Equal(1, len(filters))

	f1 := filters[0]
	cases := []struct {
		name         string
		msg          core.Msg
		continueNext bool
	}{
		{"test_table_1 rejected", core.Msg{Database: "test_db", Table: "test_table_1"}, false},
		{"other_table not rejected", core.Msg{Database: "test_db", Table: "other_table"}, true},
	}
	for _, c := range cases {
		continueNext, err := f1.Filter(&c.msg)
		if err != nil {
			assert.FailNow(err.Error())
		}
		assert.Equalf(c.continueNext, continueNext, c.name)
	}
}
