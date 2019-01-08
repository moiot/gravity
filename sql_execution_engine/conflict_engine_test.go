package sql_execution_engine

import (
	"github.com/moiot/gravity/gravity/registry"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/moiot/gravity/pkg/core"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/schema_store"
)

func TestConflictEngineWithMockDB(t *testing.T) {

	tbl := &schema_store.Table{
		Schema: "test",
		Name: "t",
		Columns: []schema_store.Column{
			{Name: "id"},
			{Name: "v"},
		},
	}

	r := require.New(t)

	mockDB, mock, err := sqlmock.New()
	r.NoError(err)
	r.NotNil(mockDB)

	defer mockDB.Close()

	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, ConflictDetectEngine)
	r.NoError(err)

	err = p.Configure("test", map[string]interface{}{
		"override-conflict": true,
		"max-conflict-retry": 3,
		"retry-sleep-duration": "100ms",
	})
	r.NoError(err)

	i, ok := p.(EngineInitializer)
	r.True(ok)

	err  = i.Init(mockDB)
	r.NoError(err)

	executor, ok := p.(EngineExecutor)
	r.True(ok)

	t.Run("insert", func(tt *testing.T) {
		msg := &core.Msg{}
		msgBatch := []*core.Msg{msg}
		msg.DmlMsg = &core.DMLMsg{
			Operation: core.Insert,
			Data:      map[string]interface{}{"id": 1, "v": 1},
			Pks:       map[string]interface{}{"id": 1},
		}

		mock.ExpectExec("INSERT INTO `test`.`t`").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))

		err = executor.Execute(msgBatch, tbl)
		r.NoError(err)

		mock.ExpectExec("INSERT INTO `test`.`t`").WithArgs(1, 1).WillReturnError(&mysql.MySQLError{Number: 1062})
		mock.ExpectQuery("select id,v from `test`.`t` where id =").WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"1", "1"}))
		mock.ExpectExec("REPLACE INTO `test`.`t`").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))

		// override
		err = executor.Execute(msgBatch, tbl)
		r.NoError(err)

		// expect sql not the same
		err = mock.ExpectationsWereMet()
		r.NoError(err)

	})

	t.Run("update", func(tt *testing.T) {
		msg := &core.Msg{}
		msg.DmlMsg = &core.DMLMsg{
			Operation: core.Update,
			Data:      map[string]interface{}{"id": 1, "v": 2},
			Old:       map[string]interface{}{"id": 1, "v": 1},
			Pks:       map[string]interface{}{"id": 1},
		}
		msgBatch := []*core.Msg{msg}

		mock.ExpectExec("UPDATE `test`.`t` SET").WillReturnResult(sqlmock.NewResult(1, 1))
		err := executor.Execute(msgBatch, tbl)
		r.NoError(err)

		msg.DmlMsg.Data = map[string]interface{}{"id": 1, "v": "3"}
		msg.DmlMsg.Old = map[string]interface{}{"id": 1, "v": 1}
		mock.ExpectExec("UPDATE `test`.`t` SET").WillReturnResult(sqlmock.NewResult(1, 0))
		mock.ExpectQuery("select id,v from `test`.`t` where id =").WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"1", "2"}))
		mock.ExpectExec("REPLACE INTO `test`.`t`").WillReturnResult(sqlmock.NewResult(1, 1))

		err = executor.Execute(msgBatch, tbl)
		r.NoError(err)

		err  = mock.ExpectationsWereMet()
		r.NoError(err)
	})

}

func TestConflictEngineWithRealDB(t *testing.T) {
	r := require.New(t)

	db := mysql_test.MustCreateTargetDBConn()
	r.NoError(execSql(db, "CREATE database if not EXISTS test"))
	execSql(db, `
CREATE TABLE if not EXISTS test.t (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  v bigint(11) unsigned NOT NULL,
  foo varchar(10),
  bar datetime,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`)

	defer func() {
		execSql(db, "drop table if exists `test`.`t`")
		db.Close()
	}()

	tbl := &schema_store.Table{
		Schema: "test",
		Name: "t",
		Columns: []schema_store.Column{
			{Name: "id"},
			{Name: "v"},
			{Name: "foo"},
			{Name: "bar"},
		},
	}

	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, ConflictDetectEngine)
	r.NoError(err)

	err = p.Configure("test", map[string]interface{}{
		"override-conflict": true,
		"max-conflict-retry": 3,
		"retry-sleep-duration": "100ms",
	})
	r.NoError(err)

	i, ok := p.(EngineInitializer)
	r.True(ok)

	r.NoError(i.Init(db))

	executor, ok := p.(EngineExecutor)
	r.True(ok)


	t.Run("insert", func(tt *testing.T) {
		msg := &core.Msg{}
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
		msgBatch := []*core.Msg{msg}

		r.NoError(executor.Execute(msgBatch, tbl))

		// override
		r.NoError(executor.Execute(msgBatch, tbl))
	})

	t.Run("update", func(tt *testing.T) {
		execSql(db, "insert into `test`.`t`(id,v) values (1,1)")

		msg := &core.Msg{}
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
		msgBatch := []*core.Msg{msg}

		err = executor.Execute(msgBatch, tbl)
		r.NoError(err)

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

		err = executor.Execute(msgBatch, tbl)
		r.NoError(err)
	})
}