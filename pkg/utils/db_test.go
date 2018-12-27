package utils_test

import (
	"database/sql"
	"fmt"
	"reflect"

	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/mysql_test"
	. "github.com/moiot/gravity/pkg/utils"
)

func TestDBUtils(t *testing.T) {
	testDBNameBase := "test_db_utils"

	assert := assert.New(t)

	t.Run("query db with sqlInt64", func(tt *testing.T) {
		dbName := fmt.Sprintf("%s_%d", testDBNameBase, 1)
		db := mysql_test.MustSetupSourceDB(dbName)
		for i := 1; i < 10; i++ {
			args := map[string]interface{}{
				"id":   i,
				"name": fmt.Sprintf("name_%d", i),
			}

			err := mysql_test.InsertIntoTestTable(db, dbName, mysql_test.TestScanColumnTableIdPrimary, args)
			assert.Nil(err)
		}

		rowsData, err := QueryGeneralRowsDataWithSQL(
			db,
			fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE id >= ?", dbName, mysql_test.TestScanColumnTableIdPrimary),
			sql.NullInt64{Valid: true, Int64: 1})

		assert.Nil(err)
		assert.Equal(len(rowsData), 9)

		// id is uint32
		row := rowsData[0]
		id := reflect.ValueOf(row[0]).Elem().Interface()
		_, ok := id.(uint32)
		assert.True(ok)

		rowsData, err = QueryGeneralRowsDataWithSQL(
			db,
			fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE id <= ?", dbName, mysql_test.TestScanColumnTableIdPrimary),
			sql.NullInt64{Valid: true, Int64: 2})
		assert.Nil(err)
		assert.Equal(len(rowsData), 2)

		min := sql.NullInt64{Valid: true, Int64: 2}
		minI := reflect.ValueOf(&min).Interface()
		rowsData, err = QueryGeneralRowsDataWithSQL(
			db,
			fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE id >= ?", dbName, mysql_test.TestScanColumnTableIdPrimary),
			minI)
		assert.Nil(err)
		assert.Equal(len(rowsData), 8)

		// max name, and min name
		rowsData, err = QueryGeneralRowsDataWithSQL(db, fmt.Sprintf("SELECT MAX(name) FROM `%s`.`%s`", dbName, mysql_test.TestScanColumnTableIdPrimary))
		d1 := reflect.ValueOf(rowsData[0][0]).Elem().Interface()
		d1String, ok := d1.(sql.NullString)
		assert.True(ok)

		rowsData, err = QueryGeneralRowsDataWithSQL(db, fmt.Sprintf("SELECT MIN(name) FROM `%s`.`%s`", dbName, mysql_test.TestScanColumnTableIdPrimary))
		d2 := reflect.ValueOf(rowsData[0][0]).Elem().Interface()
		d2String, ok := d2.(sql.NullString)
		assert.True(ok)
		assert.NotEqual(d1String, d2String)
	})

	t.Run("GetPrimaryKeys", func(tt *testing.T) {
		dbName := fmt.Sprintf("%s_%d", testDBNameBase, 2)
		db := mysql_test.MustSetupSourceDB(dbName)

		pks, err := GetPrimaryKeys(db, dbName, mysql_test.TestScanColumnTableMultiPrimary)
		assert.Nil(err)
		assert.Equal(pks, []string{"id", "name"})
	})

	t.Run("GetUniqueIndexesWithoutPks", func(tt *testing.T) {
		dbName := fmt.Sprintf("%s_%d", testDBNameBase, 3)
		db := mysql_test.MustSetupSourceDB(dbName)

		keys, err := GetUniqueIndexesWithoutPks(db, dbName, mysql_test.TestScanColumnTableUniqueIndexEmailString)
		assert.Nil(err)
		assert.Equal(keys, []string{"email"})
	})

	t.Run("EstimateRowsCount", func(tt *testing.T) {
		dbName := fmt.Sprintf("%s_%d", testDBNameBase, 4)
		db := mysql_test.MustSetupSourceDB(dbName)
		args := map[string]interface{}{
			"id":   1,
			"name": "test_name",
		}
		err := mysql_test.InsertIntoTestTable(db, dbName, mysql_test.TestScanColumnTableIdPrimary, args)
		assert.Nil(err)

		_, err = db.Exec(fmt.Sprintf("ANALYZE TABLE `%s`.`%s`", dbName, mysql_test.TestScanColumnTableIdPrimary))
		assert.Nil(err)

		rowsCount, err := EstimateRowsCount(db, dbName, mysql_test.TestScanColumnTableIdPrimary)
		assert.Nil(err)
		assert.Equal(rowsCount, 1)
	})
}
