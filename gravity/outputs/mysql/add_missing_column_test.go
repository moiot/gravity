package mysql

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/schema_store"
)

var _ = Describe("add_missing_column", func() {
	Context("when data does not have a field in target column", func() {
		It("adds the missing column with default column data", func() {
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
			Expect(b).To(BeTrue())
			Expect(err).To(BeNil())
			Expect(msg.DmlMsg.Data["a"]).To(Equal("123"))
		})
	})
})
