package sql_execution_engine

import (
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/schema_store"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenerateReplaceSQLWithMultipleValues(t *testing.T) {
	r := require.New(t)

	columns := []schema_store.Column{
		{
			Idx:  1,
			Name: "v1",
		},
		{
			Idx:  0,
			Name: "v2",
		},
	}

	tableDef := schema_store.Table{
		Schema:  "test_db",
		Name:    "test_table",
		Columns: columns,
	}

	data1 := map[string]interface{}{
		"v1": "v1_value_1",
		"v2": "v2_value_1",
	}

	data2 := map[string]interface{}{
		"v2": "v2_value_2",
		"v1": "v1_value_2",
	}

	msgBatch := []*core.Msg{
		{
			DmlMsg: &core.DMLMsg{
				Operation: core.Update,
				Data:      data1,
			},
		},
		{
			DmlMsg: &core.DMLMsg{
				Operation: core.Update,
				Data:      data2,
			},
		},
	}

	sql, args, err := GenerateReplaceSQLWithMultipleValues(msgBatch, &tableDef)
	r.NoError(err)
	r.Equal("REPLACE INTO `test_db`.`test_table` (`v2`,`v1`) VALUES (?,?),(?,?)", sql)
	r.Equal("v2_value_1", args[0])
	r.Equal("v1_value_1", args[1])
	r.Equal("v2_value_2", args[2])
	r.Equal("v1_value_2", args[3])
}

func TestGenerateSingleDeleteSQL(t *testing.T) {
	r := require.New(t)
	columns := []schema_store.Column{
		{
			Idx:  1,
			Name: "v1",
		},
		{
			Idx:  0,
			Name: "v2",
		},
		{
			Idx:  3,
			Name: "v3",
		},
	}

	tableDef := schema_store.Table{
		Schema:  "test_db",
		Name:    "test_table",
		Columns: columns,
	}

	data := map[string]interface{}{
		"v1": "v1_value",
		"v2": "v2_value",
		"v3": "v3_value",
	}

	pks := map[string]interface{}{
		"v1": "v1_value",
		"v2": "v2_value",
	}

	msg := &core.Msg{DmlMsg: &core.DMLMsg{Data: data, Pks: pks}}
	sql, args, err := GenerateSingleDeleteSQL(msg, &tableDef)

	r.NoError(err)
	r.Equal("DELETE FROM `test_db`.`test_table` WHERE `v1` = ? AND `v2` = ?", sql)
	r.Equal("v1_value", args[0])
	r.Equal("v2_value", args[1])
}
