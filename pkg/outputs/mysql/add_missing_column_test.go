package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/schema_store"
)

func TestAddMissingColumn(t *testing.T) {
	r := require.New(t)

	t.Run("when the missing column have default value NOT NULL", func(tt *testing.T) {
		msg := &core.Msg{
			DmlMsg: &core.DMLMsg{
				Data:      make(map[string]interface{}),
				Operation: core.Insert,
			},
		}

		t := &schema_store.Table{
			Columns: []schema_store.Column{
				{
					Name:       "a",
					IsNullable: false,
					DefaultVal: schema_store.ColumnValueString{
						ValueString: "123",
						IsNull:      false,
					},
				},
			},
		}
		b, err := AddMissingColumn(msg, t)
		r.True(b)
		r.NoError(err)
		r.Equal("123", msg.DmlMsg.Data["a"])
	})

	t.Run("when the missing column have default value of NULL", func(tt *testing.T) {
		msg := &core.Msg{
			DmlMsg: &core.DMLMsg{
				Data:      make(map[string]interface{}),
				Operation: core.Insert,
			},
		}

		t := &schema_store.Table{
			Columns: []schema_store.Column{
				{
					Name:       "a",
					IsNullable: true,
					DefaultVal: schema_store.ColumnValueString{
						ValueString: "",
						IsNull:      true,
					},
				},
			},
		}
		b, err := AddMissingColumn(msg, t)
		r.True(b)
		r.NoError(err)
		v, ok := msg.DmlMsg.Data["a"]
		r.True(ok)
		r.Nil(v)
	})
}
