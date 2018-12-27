package sql_execution_engine_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/schema_store"
	. "github.com/moiot/gravity/sql_execution_engine"
)

func TestGenerateSingleReplaceSQL(t *testing.T) {
	assert := assert.New(t)

	t.Run("when schema is the same", func(tt *testing.T) {
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

		data := map[string]interface{}{
			"v1": "v1_value",
			"v2": "v2_value",
		}

		msg := &core.Msg{DmlMsg: &core.DMLMsg{Data: data}}
		statement, args, err := GenerateSingleReplaceSQL(msg, &tableDef)
		assert.NoError(err)
		expectedStatement1 := "REPLACE INTO `test_db`.`test_table` (`v1`,`v2`) VALUES (?,?)"
		expectedStatement2 := "REPLACE INTO `test_db`.`test_table` (`v2`,`v1`) VALUES (?,?)"
		if statement == expectedStatement1 {
			assert.Equal("v1_value", args[0])
			assert.Equal("v2_value", args[1])
		} else if statement == expectedStatement2 {
			assert.Equal("v1_value", args[1])
			assert.Equal("v2_value", args[0])
		} else {
			t.Fatalf("unexpected statement %s", statement)
		}
	})

	t.Run("when schema is not the same", func(tt *testing.T) {
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
				Idx:  2,
				Name: "v3",
			},
			{
				Idx:  3,
				Name: "v4",
			},
			{
				Idx:  4,
				Name: "v5",
			},
		}

		tableDef := schema_store.Table{
			Schema:  "test_db",
			Name:    "test_table",
			Columns: columns,
		}

		data := map[string]interface{}{
			"v2": "v2_value",
			"v3": "v3_value",
		}

		msg := &core.Msg{DmlMsg: &core.DMLMsg{Data: data}}
		_, _, err := GenerateSingleReplaceSQL(msg, &tableDef)
		assert.NotNil(err)
	})

}

var _ = Describe("GenerateReplaceSQL", func() {
	Describe("GenerateSingleDelete", func() {
		It("generate right sql", func() {
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
			Expect(err).To(BeNil())
			Expect(sql).To(Equal("DELETE FROM `test_db`.`test_table` WHERE `v1` = ? AND `v2` = ?"))
			Expect(args[0]).To(Equal("v1_value"))
			Expect(args[1]).To(Equal("v2_value"))
		})
	})

	Describe("GenerateReplaceSQLWithMultipleValues", func() {
		It("generate right sql", func() {
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
			Expect(err).To(BeNil())
			Expect(sql).To(Equal("REPLACE INTO `test_db`.`test_table` (`v2`,`v1`) VALUES (?,?),(?,?)"))
			Expect(args[0]).To(Equal("v2_value_1"))
			Expect(args[1]).To(Equal("v1_value_1"))
			Expect(args[2]).To(Equal("v2_value_2"))
			Expect(args[3]).To(Equal("v1_value_2"))

			msgBatch = []*core.Msg{
				{
					DmlMsg: &core.DMLMsg{
						Operation: core.Insert,
						Data:      data1,
					},
				},
			}
			sql, args, err = GenerateReplaceSQLWithMultipleValues(msgBatch, &tableDef)
			Expect(err).To(BeNil())
			Expect(sql).To(Equal("REPLACE INTO `test_db`.`test_table` (`v2`,`v1`) VALUES (?,?)"))
			Expect(args[0]).To(Equal("v2_value_1"))
			Expect(args[1]).To(Equal("v1_value_1"))
		})
	})
})
