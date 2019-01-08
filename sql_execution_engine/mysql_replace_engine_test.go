package sql_execution_engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/moiot/gravity/pkg/mysql_test"

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
	testSchemaName := strings.ToLower(t.Name())
	tbl := &schema_store.Table{
		Schema: testSchemaName,
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

		mock.ExpectExec(fmt.Sprintf("REPLACE INTO `%s`.`t` \\(`id`,`v`\\)", testSchemaName)).WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
		r.NoError(executor.Execute(msgBatch, tbl))
	})

	t.Run("with annotation", func(tt *testing.T) {
		mockDB, mock, err := sqlmock.New()
		r.NoError(err)
		r.NotNil(mockDB)

		executor := NewEngineExecutor("test", MySQLReplaceEngine, mockDB, map[string]interface{}{
			"sql-annotation": "gravity_annotation",
		})

		mock.ExpectExec(fmt.Sprintf("\\/\\*gravity_annotation\\*\\/REPLACE INTO `%s`.`t` \\(`id`,`v`\\)", testSchemaName)).WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
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
		mock.ExpectExec(fmt.Sprintf("REPLACE INTO `%s`.`t` \\(`id`,`v`\\)", testSchemaName)).WithArgs(1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		r.NoError(executor.Execute(msgBatch, tbl))
	})

	t.Run("with real mysql and txn tag", func(tt *testing.T) {
		db := mysql_test.MustSetupTargetDB(testSchemaName)
		ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
id int(11) unsigned NOT NULL,
v varchar(256) DEFAULT NULL,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
`, testSchemaName, "t")

		_, err := db.Exec(ddl)
		r.NoError(err)

		executor := NewEngineExecutor("test", MySQLReplaceEngine, db, map[string]interface{}{"tag-internal-txn": true})
		r.NoError(executor.Execute(msgBatch, tbl))
	})
}
