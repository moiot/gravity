package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/matchers"

	"github.com/moiot/gravity/pkg/core"
)

func Test_acceptFilterType_Filter(t *testing.T) {
	a := assert.New(t)

	data := []config.GenericConfig{
		{
			Type: AcceptFilterName,
			Config: map[string]interface{}{
				matchers.SchemaMatcherName: "test_db",
				matchers.TableMatcherName:  "test_table_1",
			},
		},
	}

	filters, err := NewFilters(data)
	if err != nil {
		a.FailNow(err.Error())
	}

	a.Equal(1, len(filters))

	f1 := filters[0]
	cases := []struct {
		name         string
		msg          core.Msg
		continueNext bool
	}{
		{"test_table_1 accepted", core.Msg{Database: "test_db", Table: "test_table_1"}, true},
		{"other_table not accepted", core.Msg{Database: "test_db", Table: "other_table"}, false},
	}
	for _, c := range cases {
		continueNext, err := f1.Filter(&c.msg)
		if err != nil {
			a.FailNow(err.Error())
		}
		a.Equalf(c.continueNext, continueNext, c.name)
	}
}
