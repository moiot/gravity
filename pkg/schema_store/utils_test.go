package schema_store

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/mysql_test"
)

func TestSchemaStoreUtils(t *testing.T) {
	assert := assert.New(t)

	var sourceDB *sql.DB
	var testDBName = "schemaStoreUtilsTest"

	sourceDB = mysql_test.MustSetupSourceDB(testDBName)
	sourceDB.SetMaxOpenConns(20)
	sourceDB.SetMaxIdleConns(0)

	t.Run("GetTableDefFromDB for single table", func(tt *testing.T) {
		table, err := GetTableDefFromDB(sourceDB, testDBName, mysql_test.TestTableName)
		assert.Nil(err)
		assert.Equal(4, len(table.Columns))

		assert.Equal("id", table.Columns[0].Name)
		assert.Equal(0, table.Columns[0].Idx)
		assert.False(table.Columns[0].IsNullable)
		assert.False(table.Columns[0].IsUnsigned)
		assert.True(table.Columns[0].IsPrimaryKey)
		assert.Equal("id", table.PrimaryKeyColumns[0].Name)
		assert.Equal("id", table.UniqueKeyColumnMap["PRIMARY"][0])
		assert.Equal("name", table.Columns[1].Name)
		assert.False(table.Columns[1].IsPrimaryKey)
	})

	t.Run("GetCreateSchemaStatement", func(tt *testing.T) {
		err, sql := GetCreateSchemaStatement(sourceDB, testDBName)
		assert.Nil(err)
		assert.NotEqual("", sql)
	})

	t.Run("GetSchemaFromDB for single table", func(tt *testing.T) {
		schema, err := GetSchemaFromDB(sourceDB, testDBName)
		assert.Nil(err)
		assert.NotNil(schema[mysql_test.TestTableName])
	})

	t.Run("GetSchemaFromDB for 1024 tables", func(tt *testing.T) {

		for i := 0; i < 1024; i++ {
			tableName := fmt.Sprintf("t_%d", i)
			statement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s(
id INT NOT NULL,
PRIMARY KEY (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8`, testDBName, tableName)
			_, err := sourceDB.Exec(statement)
			assert.Nil(err)
		}

		schema, err := GetSchemaFromDB(sourceDB, testDBName)
		assert.Nil(err)
		assert.NotNil(schema["t_0"])
		assert.NotNil(schema["t_1023"])
	})
}
