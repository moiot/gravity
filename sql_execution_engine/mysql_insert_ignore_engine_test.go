package sql_execution_engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/schema_store"
	"github.com/stretchr/testify/require"
)

func TestMysqlInsertIgnoreEngine_Execute(t *testing.T) {
	r := require.New(t)

	testSchemaName := strings.ToLower(t.Name())

	// init test table
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

	// tableDef and msgBatch
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

	executor := NewEngineExecutor("test", MySQLInsertIgnore, db, nil)

	// no data initially
	n, err := mysql_test.CountTestTable(db, testSchemaName, "t")
	r.NoError(err)
	r.Equal(0, n)

	r.NoError(executor.Execute(msgBatch, tbl))

	// one row
	n, err = mysql_test.CountTestTable(db, testSchemaName, "t")
	r.NoError(err)
	r.Equal(1, n)

	// execute with the same row again, expect no exception, and still one row
	r.NoError(executor.Execute(msgBatch, tbl))
	n, err = mysql_test.CountTestTable(db, testSchemaName, "t")
	r.NoError(err)
	r.Equal(1, n)

	// execute another row, n == 2
	msg.DmlMsg.Data["id"] = 2
	r.NoError(executor.Execute(msgBatch, tbl))
	n, err = mysql_test.CountTestTable(db, testSchemaName, "t")
	r.NoError(err)
	r.Equal(2, n)

}
