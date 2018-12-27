package schema_store_test

import (
	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/pkg/mysql_test"
	. "github.com/moiot/gravity/schema_store"
)

var _ = Describe("simple_schema_store_test", func() {
	var schemaStoreTestDB = "simpleSchemaStoreTest"
	It("returns schemas", func() {
		db := mysql_test.MustSetupSourceDB(schemaStoreTestDB)

		store, err := NewSimpleSchemaStoreFromDBConn(db)
		Expect(err).To(BeNil())

		s, err := store.GetSchema(schemaStoreTestDB)

		Expect(err).To(BeNil())
		table := s[mysql_test.TestTableName]
		Expect(table).NotTo(BeNil())
		Expect(table.ColumnNames()).To(Equal([]string{"id", "name", "email", "ts"}))

		emailColumn, ok := table.Column("email")
		Expect(ok).To(BeTrue())
		Expect(emailColumn.IsNullable).To(BeFalse())
		Expect(emailColumn.DefaultVal.IsNull).To(BeFalse())
		Expect(emailColumn.DefaultVal.ValueString).To(Equal("default_email"))

		Expect(store.IsInCache("radom_db")).To(Equal(false))
	})
})
