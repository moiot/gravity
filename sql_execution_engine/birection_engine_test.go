package sql_execution_engine_test

import (
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"
	. "github.com/moiot/gravity/sql_execution_engine"
)

var _ = Describe("mysql birection engine", func() {
	var mockDB *sql.DB
	var sqlMock sqlmock.Sqlmock
	var err error

	table := schema_store.Table{
		Schema: "test",
		Name:   "t",
		Columns: []schema_store.Column{
			{Name: "id"},
		},
	}

	BeforeEach(func() {
		mockDB, sqlMock, err = sqlmock.New()
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		if mockDB != nil {
			mockDB.Close()
		}
	})

	Context("annotation mode", func() {

		It("can handle insert", func() {
			engine := NewBidirectionEngine(mockDB, utils.Annotation)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("\\/\\*drc:bidirectional\\*\\/REPLACE INTO `test`.`t` \\(`id`\\) VALUES (?)").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).To(BeNil())

		})

		It("can handle update", func() {
			engine := NewBidirectionEngine(mockDB, utils.Annotation)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Update,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("\\/\\*drc:bidirectional\\*\\/REPLACE INTO `test`.`t` \\(`id`\\) VALUES (?)").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).To(BeNil())
		})

		It("cannot handle delete when pk is missing", func() {
			engine := NewBidirectionEngine(mockDB, utils.Annotation)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Delete,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("\\/\\*drc:bidirectional\\*\\/DELETE FROM `test`.`t` WHERE `id` = ?").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)
			Expect(err).NotTo(BeNil())
		})

		It("can handle delete", func() {
			engine := NewBidirectionEngine(mockDB, utils.Annotation)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Delete,
					Data:      map[string]interface{}{"id": 1},
					Pks:       map[string]interface{}{"id": 1},
				},
			}
			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("\\/\\*drc:bidirectional\\*\\/DELETE FROM `test`.`t` WHERE `id` = ?").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()
			err := engine.Execute([]*core.Msg{msg}, &table)
			Expect(err).To(BeNil())
		})
	})

	Context("statement mode", func() {
		It("can handle insert", func() {
			sqlMock.ExpectExec("CREATE DATABASE IF NOT EXISTS drc").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("CREATE TABLE IF NOT EXISTS drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

			engine := NewBidirectionEngine(mockDB, utils.Stmt)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("insert into drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("REPLACE INTO `test`.`t` \\(`id`\\) VALUES (?)").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).To(BeNil())
		})

		It("can handle update", func() {
			sqlMock.ExpectExec("CREATE DATABASE IF NOT EXISTS drc").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("CREATE TABLE IF NOT EXISTS drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

			engine := NewBidirectionEngine(mockDB, utils.Stmt)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Update,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("insert into drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("REPLACE INTO `test`.`t` \\(`id`\\) VALUES (?)").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).To(BeNil())
		})

		It("cannot handle delete when pk is empty", func() {
			sqlMock.ExpectExec("CREATE DATABASE IF NOT EXISTS drc").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("CREATE TABLE IF NOT EXISTS drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

			engine := NewBidirectionEngine(mockDB, utils.Stmt)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Delete,
					Data:      map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("insert into drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("DELETE FROM `test`.`t` WHERE `id` = ?").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).NotTo(BeNil())
		})

		It("can handle delete", func() {
			sqlMock.ExpectExec("CREATE DATABASE IF NOT EXISTS drc").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("CREATE TABLE IF NOT EXISTS drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

			engine := NewBidirectionEngine(mockDB, utils.Stmt)

			msg := &core.Msg{
				DmlMsg: &core.DMLMsg{
					Operation: core.Delete,
					Data:      map[string]interface{}{"id": 1},
					Pks:       map[string]interface{}{"id": 1},
				},
			}

			sqlMock.ExpectBegin()
			sqlMock.ExpectExec("insert into drc._drc_bidirection").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectExec("DELETE FROM `test`.`t` WHERE `id` = ?").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			sqlMock.ExpectCommit()

			err := engine.Execute([]*core.Msg{msg}, &table)

			Expect(err).To(BeNil())
		})
	})
})
