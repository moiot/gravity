package sql_execution_engine

import (
	"testing"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestMySQLReplaceEngineConfigure(t *testing.T) {
	r := require.New(t)

	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, MySQLReplaceEngine)
	r.NoError(err)

	e := p.Configure("test", make(map[string]interface{}))
	r.NoError(e)

	_, ok := p.(EngineInitializer)
	r.True(ok)

	_, ok = p.(EngineExecutor)
	r.True(ok)
}

func TestMySQLReplaceEngineExecute(t *testing.T) {
	r := require.New(t)
	tbl := &schema_store.Table{
		Schema: "test",
		Name:   "t",
		Columns: []schema_store.Column{
			{Name: "id", Idx: 0},
			{Name: "v", Idx: 1},
		},
	}

	msg := &core.Msg{}
	msg.DmlMsg = &core.DMLMsg{
		Operation: core.Insert,
		Data:      map[string]interface{}{"id": 1, "v": 1},
		Pks:       map[string]interface{}{"id": 1},
	}
	msgBatch := []*core.Msg{msg}

	t.Run("normal execution", func(tt *testing.T) {
		mockDB, mock, err := sqlmock.New()
		r.NoError(err)
		r.NotNil(mockDB)

		executor := NewEngineExecutor("test", MySQLReplaceEngine, mockDB, nil)

		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `test`.`t` \\(`id`,`v`\\)").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		r.NoError(executor.Execute(msgBatch, tbl))
	})

	t.Run("with annotation", func(tt *testing.T) {
		mockDB, mock, err := sqlmock.New()
		r.NoError(err)
		r.NotNil(mockDB)

		executor := NewEngineExecutor("test", MySQLReplaceEngine, mockDB, map[string]interface{}{"sql-annotation": "/*gravity_annotation*/"})

		mock.ExpectBegin()
		mock.ExpectExec("\\/\\*gravity_annotation\\*\\/REPLACE INTO `test`.`t` \\(`id`,`v`\\)").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		r.NoError(executor.Execute(msgBatch, tbl))
	})

	t.Run("with internal txn tag", func(tt *testing.T) {
		mockDB, mock, err := sqlmock.New()
		r.NoError(err)
		r.NotNil(mockDB)
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WithArgs().WillReturnResult(sqlmock.NewResult(1, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WithArgs().WillReturnResult(sqlmock.NewResult(1, 0))

		executor := NewEngineExecutor("test", MySQLReplaceEngine, mockDB, map[string]interface{}{"tag-internal-txn": true})
		mock.ExpectBegin()
		mock.ExpectExec(utils.TxnTagSQLFormat).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("REPLACE INTO `test`.`t` \\(`id`,`v`\\)").WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		r.NoError(executor.Execute(msgBatch, tbl))
	})
}
