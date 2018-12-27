package mysql_test

// import (
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
//
// 	"github.com/moiot/gravity/pkg/core"
// 	"github.com/moiot/gravity/plugins"
// 	"github.com/moiot/gravity/schema_store"
// )
//
// var _ = Describe("cast default data", func() {
// 	Context("when data is the same as source default", func() {
// 		Context("when data is null and target does not allow null", func() {
// 			msg := &core.Msg{
// 				DmlMsg: &core.DML{
// 					Operation: core.Insert,
// 					Data: map[string]interface{}{
// 						"a": nil,
// 					},
// 				},
// 				TableDef: &schema_store.Table{
// 					Columns: []schema_store.Column{
// 						{
// 							Name: "a",
// 							DefaultVal: schema_store.ColumnValueString{
// 								IsNull: true,
// 							},
// 							IsNullable: true,
// 						},
// 					},
// 				},
// 			}
// 			It("cast data to target default", func() {
// 				t := &schema_store.Table{
// 					Columns: []schema_store.Column{
// 						{
// 							Name:       "a",
// 							IsNullable: false,
// 							DefaultVal: schema_store.ColumnValueString{
// 								ValueString: "123",
// 								IsNull:      false,
// 							},
// 						},
// 					},
// 				}
// 				p := plugins.CastDefaultData{}
// 				b, err := p.Map(msg, nil, t)
// 				Expect(b).To(BeTrue())
// 				Expect(err).To(BeNil())
// 				Expect(msg.DmlMsg.Data["a"]).To(Equal("123"))
// 			})
// 		})
//
// 		Context("when data is not null", func() {
// 			msg := &core.Msg{
// 				DmlMsg: &core.DML{
// 					Operation: core.Insert,
// 					Data: map[string]interface{}{
// 						"a": "aa",
// 					},
// 				},
//
// 				TableDef: &schema_store.Table{
// 					Columns: []schema_store.Column{
// 						{
// 							Name:       "a",
// 							IsNullable: false,
// 							DefaultVal: schema_store.ColumnValueString{
// 								ValueString: "aa",
// 								IsNull:      true,
// 							},
// 						},
// 					},
// 				},
// 			}
// 			It("does not cast data", func() {
// 				t := &schema_store.Table{
// 					Columns: []schema_store.Column{
// 						{
// 							Name:       "a",
// 							IsNullable: false,
// 							DefaultVal: schema_store.ColumnValueString{
// 								ValueString: "123",
// 								IsNull:      false,
// 							},
// 						},
// 					},
// 				}
// 				p := plugins.CastDefaultData{}
// 				b, err := p.Map(msg, nil, t)
// 				Expect(b).To(BeTrue())
// 				Expect(err).To(BeNil())
// 				Expect(msg.DmlMsg.Data["a"]).To(Equal("aa"))
// 			})
// 		})
// 	})
//
// 	Context("when data is not the same as source default", func() {
// 		It("does not cast", func() {
// 			msg := &core.Msg{
// 				DmlMsg: &core.DML{
// 					Operation: core.Insert,
// 					Data: map[string]interface{}{
// 						"a": nil,
// 					},
// 				},
// 				TableDef: &schema_store.Table{
// 					Columns: []schema_store.Column{
// 						{
// 							Name:       "a",
// 							IsNullable: false,
// 							DefaultVal: schema_store.ColumnValueString{
// 								ValueString: "123",
// 								IsNull:      false,
// 							},
// 						},
// 					},
// 				},
// 			}
//
// 			t := &schema_store.Table{
// 				Columns: []schema_store.Column{
// 					{
// 						Name:       "a",
// 						IsNullable: false,
// 						DefaultVal: schema_store.ColumnValueString{
// 							ValueString: "123",
// 							IsNull:      false,
// 						},
// 					},
// 				},
// 			}
// 			p := plugins.CastDefaultData{}
// 			b, err := p.Map(msg, nil, t)
// 			Expect(b).To(BeTrue())
// 			Expect(err).To(BeNil())
// 		})
// 	})
// })
