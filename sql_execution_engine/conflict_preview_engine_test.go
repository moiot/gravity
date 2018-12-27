package sql_execution_engine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/pkg/core"

	"database/sql"
	"fmt"

	"errors"

	log "github.com/sirupsen/logrus"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/protocol/msgpb"
	"github.com/moiot/gravity/schema_store"
)

var (
	previewTargetDB = "padderPreviewTestTargetDB"
	msg             *core.Msg
)

var _ = Describe("padder preview engine test", func() {
	uniqMap := map[string][]string{}
	uniqMap["PRIMARY"] = []string{"id"}
	table := &schema_store.Table{
		Schema: previewTargetDB,
		Name:   mysql_test.TestTableWithoutTs,
		Columns: []schema_store.Column{
			{Name: "id"},
			{Name: "name"},
		},
		UniqueKeyColumnMap: uniqMap,
	}
	msg = &core.Msg{}
	Context("preview test with real mysql", func() {
		db := mysql_test.MustSetupSourceDB(previewTargetDB)
		engine := conflictPreviewEngine{db: db, maxRetry: 2}
		Context("preview without conflict test", func() {
			BeforeEach(func() {
				execSql(db, fmt.Sprintf("truncate table %s.%s", previewTargetDB, mysql_test.TestTableWithoutTs))
				log.Debug("clear table rows")
			})
			It("preview insert msg: no conflict", func() {
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id":   1,
						"name": "2",
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err := engine.Execute([]*core.Msg{msg}, table)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("preview update msg: no conflict", func() {
				id := 1
				oldName := "oldName"
				newName := "newName"
				args := map[string]interface{}{
					"id":   id,
					"name": oldName,
				}
				err := mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				Expect(err).ShouldNot(HaveOccurred())
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Update,
					Data: map[string]interface{}{
						"id":   1,
						"name": newName,
					},
					Old: map[string]interface{}{
						"id":   1,
						"name": oldName,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("preview delete msg: no conflict", func() {
				engine := conflictPreviewEngine{db: db, enableDelete: true, maxRetry: 2}
				args := map[string]interface{}{
					"id":   1,
					"name": "name",
				}
				err := mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				Expect(err).ShouldNot(HaveOccurred())
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Delete,
					Data: map[string]interface{}{
						"id":   1,
						"name": "name",
					},
					Pks: map[string]interface{}{
						"id": msgpb.NewMySQLValidColumnDataInt64("1"),
					},
				}
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("skip delete msg when disabling delete", func() {
				engine := conflictPreviewEngine{db: db, enableDelete: false, maxRetry: 2}
				id := "1"
				name := "name"
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Delete,
					Data: map[string]interface{}{
						"id":   id,
						"name": name,
					},
					Pks: map[string]interface{}{
						"id": id,
					},
				}
				err := engine.Execute([]*core.Msg{msg}, table)
				Expect(err).To(BeEquivalentTo(ErrDeleteRowSkip))

				args := map[string]interface{}{
					"id":   id,
					"name": name,
				}
				err = mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				Expect(err).ShouldNot(HaveOccurred())
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).To(BeEquivalentTo(ErrDeleteRowSkip))

			})
		})
		Context("preview with conflict test", func() {
			BeforeEach(func() {
				execSql(db, fmt.Sprintf("truncate table %s.%s", previewTargetDB, mysql_test.TestTableWithoutTs))
				log.Debug("clear table rows")
			})
			It("preview insert msg: conflict exists", func() {
				id := 1
				name := "name"

				args := map[string]interface{}{
					"id":   id,
					"name": name,
				}
				err := mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id":   1,
						"name": name,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).To(BeEquivalentTo(ErrRowConflict))
			})

			It("preview update msg: conflict exists as new data already exists", func() {
				id := 1
				oldName := "oldName"
				newName := "newName"

				args := map[string]interface{}{
					"id":   id,
					"name": oldName,
				}
				err := mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				err = mysql_test.UpdateTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, id, oldName)
				Expect(err).ShouldNot(HaveOccurred())
				err = mysql_test.DeleteTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, id)
				Expect(err).ShouldNot(HaveOccurred())
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Update,
					Data: map[string]interface{}{
						"id":   1,
						"name": newName,
					},
					Old: map[string]interface{}{
						"id":   1,
						"name": oldName,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).To(BeEquivalentTo(ErrRowConflict))
			})
			It("preview delete msg: conflict exists when enabling delete", func() {
				id := 1
				name := "name"
				engine := conflictPreviewEngine{db: db, enableDelete: true, maxRetry: 2}

				args := map[string]interface{}{
					"id":   id,
					"name": name,
				}
				err := mysql_test.InsertIntoTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, args)
				Expect(err).ShouldNot(HaveOccurred())
				err = mysql_test.DeleteTestTable(db, previewTargetDB, mysql_test.TestTableWithoutTs, id)
				Expect(err).ShouldNot(HaveOccurred())
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Delete,
					Data: map[string]interface{}{
						"id":   1,
						"name": name,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = engine.Execute([]*core.Msg{msg}, table)
				Expect(err).To(BeEquivalentTo(ErrRowConflict))
			})
		})
	})

	Context("preview test with mock mysql", func() {
		var engine conflictPreviewEngine
		var mockDB *sql.DB
		var mock sqlmock.Sqlmock
		var err error
		AfterEach(func() {
			if mockDB != nil {
				mockDB.Close()
			}
		})
		BeforeEach(func() {
			mockDB, mock, err = sqlmock.New()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mockDB).NotTo(BeNil())
			engine = conflictPreviewEngine{db: mockDB, maxRetry: 2}
		})
		It("preview insert msg: conflict unknown", func() {
			id := 1
			name := "name"
			msg.DmlMsg = &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":   id,
					"name": name,
				},
				Pks: map[string]interface{}{
					"id": id,
				},
			}
			statement := fmt.Sprintf("SELECT count(\\*) FROM %s.%s", previewTargetDB, mysql_test.TestTableWithoutTs)
			testConflictUnknown(statement, engine, table, mock)
		})

		It("preview update msg: conflict unknown", func() {
			id := 1
			name := "name"
			oldName := "oldName"
			msg.DmlMsg = &core.DMLMsg{
				Operation: core.Update,
				Data: map[string]interface{}{
					"id":   id,
					"name": name,
				},
				Old: map[string]interface{}{
					"id":   id,
					"name": oldName,
				},
				Pks: map[string]interface{}{
					"id": id,
				},
			}
			statement := fmt.Sprintf("SELECT count(\\*) FROM %s.%s", previewTargetDB, mysql_test.TestTableWithoutTs)
			testConflictUnknown(statement, engine, table, mock)
		})

		It("preview delete msg: conflict unknown", func() {
			mockDB, mock, err := sqlmock.New()
			Expect(err).ShouldNot(HaveOccurred())
			engine := conflictPreviewEngine{db: mockDB, enableDelete: true, maxRetry: 2}
			Expect(mockDB).NotTo(BeNil())
			id := 1
			name := "name"
			msg.DmlMsg = &core.DMLMsg{
				Operation: core.Delete,
				Data: map[string]interface{}{
					"id":   id,
					"name": name,
				},
				Pks: map[string]interface{}{
					"id": id,
				},
			}
			statement := fmt.Sprintf("SELECT count(\\*) FROM %s.%s", previewTargetDB, mysql_test.TestTableWithoutTs)
			testConflictUnknown(statement, engine, table, mock)
		})

	})
})

func testConflictUnknown(statement string, engine conflictPreviewEngine, table *schema_store.Table, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(statement).WillReturnError(errors.New("mock error"))
	err := engine.Execute([]*core.Msg{msg}, table)
	Expect(err).Should(HaveOccurred())
}

func execSql(db *sql.DB, stmt string) {
	_, err := db.Exec(stmt)
	Expect(err).ShouldNot(HaveOccurred())
}
