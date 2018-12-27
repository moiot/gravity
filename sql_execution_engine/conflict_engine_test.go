package sql_execution_engine_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/pkg/core"

	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"time"

	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/schema_store"
	"github.com/moiot/gravity/sql_execution_engine"
)

var _ = Describe("MysqlConflictAwareExecutor", func() {
	tbl := &schema_store.Table{}
	var mockDB *sql.DB
	var realDB *sql.DB
	var mock sqlmock.Sqlmock
	var err error
	var msg *core.Msg
	var msgBatch []*core.Msg

	BeforeEach(func() {
		tbl.Schema = "test"
		tbl.Name = "t"
		tbl.Columns = []schema_store.Column{
			{Name: "id"},
			{Name: "v"},
		}

		msg = &core.Msg{}

		msgBatch = []*core.Msg{msg}
	})

	AfterEach(func() {
		if mockDB != nil {
			mockDB.Close()
			log.Debug("mock db closed")
		}

		if realDB != nil {
			realDB.Close()
			log.Debug("real db closed")
		}

	})

	Context("with mock mysql", func() {
		BeforeEach(func() {
			mockDB, mock, err = sqlmock.New()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mockDB).NotTo(BeNil())
		})

		Context("with override", func() {

			It("should work for insert", func() {
				fmt.Errorf("mockDB: %v\n", mockDB)

				executor := sql_execution_engine.NewConflictEngine(mockDB, true, 3, 100*time.Millisecond, false)

				mock.ExpectExec("INSERT INTO `test`.`t`").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))

				// msg.TableDef = &schema_store.Table{
				// 	Columns: []schema_store.Column{
				// 		{Name: "id"}, {Name: "v"},
				// 	},
				// }
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Insert,
					Data:      map[string]interface{}{"id": 1, "v": 1},
					Pks:       map[string]interface{}{"id": 1},
				}
				err = executor.Execute(msgBatch, tbl)
				Expect(err).ShouldNot(HaveOccurred())

				mock.ExpectExec("INSERT INTO `test`.`t`").WithArgs(1, 1).WillReturnError(&mysql.MySQLError{Number: 1062})
				mock.ExpectQuery("select id,v from `test`.`t` where id =").WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"1", "1"}))
				mock.ExpectExec("REPLACE INTO `test`.`t`").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
				err = executor.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())

				Expect(mock.ExpectationsWereMet()).ShouldNot(HaveOccurred())
			})

			It("should work for update", func() {
				executor := sql_execution_engine.NewConflictEngine(mockDB, true, 2, 100*time.Millisecond, false)

				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Update,
					Data:      map[string]interface{}{"id": 1, "v": 2},
					Old:       map[string]interface{}{"id": 1, "v": 1},
					Pks:       map[string]interface{}{"id": 1},
				}
				mock.ExpectExec("UPDATE `test`.`t` SET").WillReturnResult(sqlmock.NewResult(1, 1))
				err = executor.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())

				msg.DmlMsg.Data = map[string]interface{}{"id": 1, "v": "3"}
				msg.DmlMsg.Old = map[string]interface{}{"id": 1, "v": 1}
				mock.ExpectExec("UPDATE `test`.`t` SET").WillReturnResult(sqlmock.NewResult(1, 0))
				mock.ExpectQuery("select id,v from `test`.`t` where id =").WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"1", "2"}))
				mock.ExpectExec("REPLACE INTO `test`.`t`").WillReturnResult(sqlmock.NewResult(1, 1))
				err = executor.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())

				Expect(mock.ExpectationsWereMet()).ShouldNot(HaveOccurred())
			})
		})
	})

	Context("with real mysql", func() { //to ensure all sql can be executed, no assertion here

		BeforeEach(func() {
			realDB = mysql_test.MustCreateTargetDBConn()
			execSql(realDB, "CREATE database if not EXISTS test")
			execSql(realDB, `
CREATE TABLE if not EXISTS test.t (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  v bigint(11) unsigned NOT NULL,
  foo varchar(10),
  bar datetime,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`)

			tbl.Columns = []schema_store.Column{
				{Name: "id"},
				{Name: "v"},
				{Name: "foo"},
				{Name: "bar"},
			}
		})

		AfterEach(func() {
			execSql(realDB, "drop table if exists `test`.`t`")
			log.Debug("table dropped")
		})

		Context("with override", func() {

			It("should work for insert", func() {
				e := sql_execution_engine.NewConflictEngine(realDB, true, 3, 100*time.Millisecond, false)

				// msg.TableDef = &schema_store.Table{
				// 	Columns: []schema_store.Column{
				// 		{Name: "id"}, {Name: "v"}, {Name: "foo"}, {Name: "bar"},
				// 	},
				// }
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id":  1,
						"v":   1,
						"foo": nil,
						"bar": nil,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = e.Execute(msgBatch, tbl)
				Expect(err).ShouldNot(HaveOccurred())

				err = e.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("should work for update", func() {
				e := sql_execution_engine.NewConflictEngine(realDB, true, 3, 100*time.Millisecond, false)

				execSql(realDB, "insert into `test`.`t`(id,v) values (1,1)")

				// msg.TableDef = &schema_store.Table{
				// 	Columns: []schema_store.Column{
				// 		{Name: "id"}, {Name: "v"}, {Name: "foo"}, {Name: "bar"},
				// 	},
				// }
				msg.DmlMsg = &core.DMLMsg{
					Operation: core.Update,
					Data: map[string]interface{}{
						"id":  1,
						"v":   2,
						"foo": 123,
						"bar": nil,
					},
					Old: map[string]interface{}{
						"id":  1,
						"v":   1,
						"foo": nil,
						"bar": nil,
					},
					Pks: map[string]interface{}{
						"id": 1,
					},
				}
				err = e.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())

				msg.DmlMsg.Data = map[string]interface{}{
					"id":  1,
					"v":   3,
					"foo": 123,
					"bar": nil,
				}
				msg.DmlMsg.Old = map[string]interface{}{
					"id":  1,
					"v":   1,
					"foo": nil,
					"bar": nil,
				}
				err = e.Execute(msgBatch, tbl) //override
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
})

func execSql(db *sql.DB, stmt string) {
	_, err := db.Exec(stmt)
	Expect(err).ShouldNot(HaveOccurred())
}
