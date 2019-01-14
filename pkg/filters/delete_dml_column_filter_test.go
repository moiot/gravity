package filters

import (
	"testing"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/stretchr/testify/require"
)

func newDeleteDmlColumnFilter(s string) (core.IFilter, error) {
	cfg, err := newFilterConfigFromJson(s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	f := &deleteDmlColumnFilter{}
	if err := f.Configure(cfg); err != nil {
		return nil, errors.Trace(err)
	}

	return f, nil
}

func TestDeleteDmlColumnFilter_Configure(t *testing.T) {
	r := require.New(t)

	_, err := newFilterConfigFromJson(`
{
	"type": "delete-dml-column",
	"match-schema": "test",
	"match-table": "test_table",
	"columns": ["a", "b"]
}
`)
	r.NoError(err)
}

func TestDeleteDmlColumnFilter_Filter(t *testing.T) {

	r := require.New(t)

	f, err := newDeleteDmlColumnFilter(`
{
	"type": "delete-dml-column",
	"match-schema": "test",
	"match-table": "test_table",
	"columns": ["b", "a", "d"]
}
`)
	r.NoError(err)

	msg := core.Msg{
		Type:     core.MsgDML,
		Database: "test",
		Table:    "test_table",
		DmlMsg: &core.DMLMsg{
			Data: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
				"d": 4,
			},
			Old: map[string]interface{}{
				"a": 10,
				"b": 20,
				"c": 30,
				"d": 40,
			},
			Pks: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
			},
		},
	}

	c, err := f.Filter(&msg)
	r.True(c)
	r.NoError(err)

	// Data
	_, ok := msg.DmlMsg.Data["a"]
	r.False(ok)

	_, ok = msg.DmlMsg.Data["b"]
	r.False(ok)

	_, ok = msg.DmlMsg.Data["c"]
	r.True(ok)

	// Old
	_, ok = msg.DmlMsg.Old["a"]
	r.False(ok)

	_, ok = msg.DmlMsg.Old["b"]
	r.False(ok)

	_, ok = msg.DmlMsg.Old["c"]
	r.True(ok)

	// Pks
	r.Equal(1, len(msg.DmlMsg.Pks))

	_, ok = msg.DmlMsg.Pks["a"]
	r.False(ok)

	_, ok = msg.DmlMsg.Pks["c"]
	r.True(ok)

	_, ok = msg.DmlMsg.Pks["d"]
	r.False(ok)

}
