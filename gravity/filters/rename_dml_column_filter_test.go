package filters

import (
	"testing"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/stretchr/testify/require"
)

func newRenameDmlColumnFilterFromJson(s string) (core.IFilter, error) {
	cfg, err := newFilterConfigFromJson(s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	f := &renameDmlColumnFilter{}
	if err := f.Configure(cfg); err != nil {
		return nil, errors.Trace(err)
	}
	return f, nil
}

func TestRenameDmlColumnFilter_Configure(t *testing.T) {
	r := require.New(t)
	cfg, err := newFilterConfigFromJson(`
{
	"type": "rename-dml-column",
	"match-schema": "test",
	"match-table": "test_table",
	"from": ["a", "b"],
	"to": ["c", "d"]
}
`)
	r.NoError(err)

	f := &renameDmlColumnFilter{}

	r.NoError(f.Configure(cfg))
}

func TestRenameDmlColumnFilter_Filter(t *testing.T) {
	r := require.New(t)

	f, err := newRenameDmlColumnFilterFromJson(`
{
	"type": "rename-dml-column",
	"match-schema": "test",
	"match-table": "test_table",
	"from": ["a", "b"],
	"to": ["c", "d"]
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
				"e": 3,
				"f": 4,
			},
			Old: map[string]interface{}{
				"a": 100,
				"b": 101,
				"e": 102,
				"f": 103,
			},
			Pks: map[string]interface{}{
				"a": 1,
			},
		},
	}

	c, err := f.Filter(&msg)
	r.True(c)
	r.NoError(err)

	// Data
	_, exists := msg.DmlMsg.Data["a"]
	r.False(exists)

	_, exists = msg.DmlMsg.Data["b"]
	r.False(exists)

	_, exists = msg.DmlMsg.Data["c"]
	r.True(exists)

	_, exists = msg.DmlMsg.Data["d"]
	r.True(exists)

	// Old
	_, exists = msg.DmlMsg.Old["a"]
	r.False(exists)

	_, exists = msg.DmlMsg.Old["b"]
	r.False(exists)

	_, exists = msg.DmlMsg.Old["c"]
	r.True(exists)

	_, exists = msg.DmlMsg.Old["d"]
	r.True(exists)

	// Pks
	_, exists = msg.DmlMsg.Pks["a"]
	r.False(exists)

	_, exists = msg.DmlMsg.Pks["b"]
	r.False(exists)

	_, exists = msg.DmlMsg.Pks["c"]
	r.True(exists)

	_, exists = msg.DmlMsg.Pks["d"]
	r.False(exists)

	r.Equal(1, msg.DmlMsg.Data["c"])
	r.Equal(2, msg.DmlMsg.Data["d"])
	r.Equal(100, msg.DmlMsg.Old["c"])
	r.Equal(101, msg.DmlMsg.Old["d"])
	r.Equal(1, msg.DmlMsg.Pks["c"])
}
