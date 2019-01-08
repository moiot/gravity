package sql_execution_engine

import (
	"fmt"
	"testing"

	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mysql_test"
)

func TestManualSQLEngine(t *testing.T) {
	r := require.New(t)

	newName := "fake data"

	SQLTemplate := "UPDATE `{{.TargetTable.Schema}}`.`{{.TargetTable.Name}}` SET name = ?"

	msg := core.Msg{
		Type: core.MsgDML,
		DmlMsg: &core.DMLMsg{
			Data: map[string]interface{}{
				"name": newName,
			},
		},
	}

	tableDef := schema_store.Table{
		Schema: t.Name(),
		Name:   mysql_test.TestTableName,
	}

	t.Run("normal update", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		r.NoError(err)

		mock.ExpectExec(fmt.Sprintf("UPDATE `%s`.`%s` SET name = ?", t.Name(), mysql_test.TestTableName)).WithArgs(newName).WillReturnResult(sqlmock.NewResult(1, 1))

		executor := NewEngineExecutor("test", ManualEngine, db, map[string]interface{}{
			"sql-template": SQLTemplate,
			"sql-arg-expr": []string{"name"},
		})
		r.NoError(executor.Execute([]*core.Msg{&msg}, &tableDef))
	})

	t.Run("with annotation", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		r.NoError(err)

		mock.ExpectExec(fmt.Sprintf("\\/\\*hello\\*\\/UPDATE `%s`.`%s` SET name = ?", t.Name(), mysql_test.TestTableName)).WithArgs(newName).WillReturnResult(sqlmock.NewResult(1, 1))

		executor := NewEngineExecutor("test", ManualEngine, db, map[string]interface{}{
			"sql-annotation": "hello",
			"sql-template":   SQLTemplate,
			"sql-arg-expr":   []string{"name"},
		})
		r.NoError(executor.Execute([]*core.Msg{&msg}, &tableDef))
	})

	t.Run("with txn tag", func(tt *testing.T) {
		db, mock, err := sqlmock.New()
		r.NoError(err)

		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WithArgs().WillReturnResult(sqlmock.NewResult(1, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WithArgs().WillReturnResult(sqlmock.NewResult(1, 0))
		mock.ExpectBegin()
		mock.ExpectExec(utils.TxnTagSQLFormat).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(fmt.Sprintf("UPDATE `%s`.`%s` SET name = ?", t.Name(), mysql_test.TestTableName)).WithArgs(newName).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		executor := NewEngineExecutor("test", ManualEngine, db, map[string]interface{}{
			"tag-internal-txn": true,
			"sql-template":     SQLTemplate,
			"sql-arg-expr":     []string{"name"},
		})
		r.NoError(executor.Execute([]*core.Msg{&msg}, &tableDef))

	})
}
