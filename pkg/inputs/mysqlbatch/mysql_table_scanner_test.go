package mysqlbatch

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/emitter"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/schema_store"
)

func TestFindMaxMinValueCompositePks(t *testing.T) {
	r := require.New(t)

	testDBName := utils.TestCaseMd5Name(t)
	db := mysql_test.MustSetupSourceDB(testDBName)
	defer db.Close()

	for i := 1; i < 100; i++ {
		args := map[string]interface{}{
			"id":    i,
			"name":  fmt.Sprintf("name_%d", i),
			"email": fmt.Sprintf("email_%d", i),
		}
		err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableCompositePrimaryOutOfOrder, args)
		r.NoError(err)
	}

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestScanColumnTableCompositePrimaryOutOfOrder, []string{"email", "name"})

	maxEmail, ok := max[0].(sql.NullString)

	fmt.Printf("maxEmail type: %v\n", reflect.TypeOf(max[0]))

	r.True(ok)

	r.Equal("email_99", maxEmail.String)

	maxName, ok := max[1].(sql.NullString)
	r.True(ok)
	r.Equal("name_99", maxName.String)

	minEmail, ok := min[0].(sql.NullString)
	r.True(ok)
	r.Equal("email_1", minEmail.String)

	minName, ok := min[1].(sql.NullString)
	r.True(ok)
	r.Equal("name_1", minName.String)
}

func TestFindMaxMinValueInt(t *testing.T) {
	r := require.New(t)
	testDBName := utils.TestCaseMd5Name(t)

	db := mysql_test.MustSetupSourceDB(testDBName)
	defer db.Close()

	for i := 1; i < 100; i++ {
		args := map[string]interface{}{
			"id": i,
		}
		err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, args)
		r.NoError(err)
	}

	r.NoError(mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, map[string]interface{}{"id": 5000}))
	r.NoError(mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, map[string]interface{}{"id": 1410812506}))

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, []string{"id"})

	maxVal, ok := max[0].(int64)
	r.True(ok)
	r.EqualValues(1410812506, maxVal)

	minVal, ok := min[0].(int64)
	r.True(ok)

	r.EqualValues(1, minVal)
}

func TestFindMaxMinValueString(t *testing.T) {
	r := require.New(t)
	testDBName := utils.TestCaseMd5Name(t)

	db := mysql_test.MustSetupSourceDB(testDBName)
	defer db.Close()

	for i := 1; i <= 2; i++ {
		name := fmt.Sprintf("test_%d", i)
		args := map[string]interface{}{
			"id":   i,
			"name": name,
		}
		err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, args)
		if err != nil {
			r.FailNow(err.Error())
		}
	}

	count, err := mysql_test.CountTestTable(db, testDBName, mysql_test.TestTableName)
	r.NoError(err)
	r.EqualValues(2, count)

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, []string{"name"})

	maxV, ok1 := max[0].(sql.NullString)
	r.True(ok1)

	minV, ok2 := min[0].(sql.NullString)
	r.True(ok2)
	r.Equal("test_2", maxV.String)
	r.Equal("test_1", minV.String)
}

func TestFindMaxMinValueTime(t *testing.T) {
	r := require.New(t)
	testDBName := utils.TestCaseMd5Name(t)

	db := mysql_test.MustSetupSourceDB(testDBName)
	startTime := time.Now()
	for i := 0; i < 100; i++ {
		args := map[string]interface{}{
			"id": i,
			"ts": startTime.Add(time.Duration(i) * time.Minute),
		}
		err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, args)
		r.Nil(err)
	}

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, []string{"ts"})
	maxT := max[0].(mysql.NullTime)
	minT := min[0].(mysql.NullTime)
	// assert.True(t, reflect.DeepEqual(mysql.NullTime{Time: startTime.Add(99 * time.Second), Valid: true}, maxT))
	// assert.True(t, reflect.DeepEqual(mysql.NullTime{Time: startTime, Valid: true}, minT))
	assert.EqualValues(t, startTime.Add(99*time.Minute).Minute(), maxT.Time.Minute())
	assert.EqualValues(t, startTime.Minute(), minT.Time.Minute())
}

type fakeMsgSubmitter struct {
	msgs []*core.Msg
}

func (submitter *fakeMsgSubmitter) SubmitMsg(msg *core.Msg) error {
	if msg.Type == core.MsgDML {
		submitter.msgs = append(submitter.msgs, msg)
	}
	if msg.AfterCommitCallback != nil {
		if err := msg.AfterCommitCallback(msg); err != nil {
			return errors.Trace(err)
		}
	}
	close(msg.Done)
	return nil
}

func TestTableScanner_Start(t *testing.T) {
	r := require.New(t)

	t.Run("it terminates", func(tt *testing.T) {
		testDBName := utils.TestCaseMd5Name(tt)

		dbCfg := mysql_test.SourceDBConfig()
		positionRepo := position_repos.NewMySQLRepo(tt.Name(), "", dbCfg)

		testCases := []struct {
			name        string
			seedFunc    func(db *sql.DB)
			cfg         PluginConfig
			scanColumns []string
		}{
			{
				"no record in table",
				nil,
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableIdPrimary},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{"id"},
			},
			{
				"sends one msg when source table have only one record",
				func(db *sql.DB) {
					args := map[string]interface{}{
						"id":   1,
						"name": "name",
					}
					mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableIdPrimary, args)
				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableIdPrimary},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{"id"},
			},
			{
				"terminates when scan column is int",
				func(db *sql.DB) {
					for i := 1; i < 10; i++ {
						args := map[string]interface{}{
							"id": i,
						}
						r.NoError(mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableIdPrimary, args))
					}
				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableIdPrimary},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{"id"},
			},
			{
				"terminates when scan column is string",
				func(db *sql.DB) {
					t := time.Now()
					for i := 1; i < 10; i++ {
						t.Add(time.Second)
						args := map[string]interface{}{
							"id":    i,
							"email": fmt.Sprintf("email_%d", i),
							"ts":    t,
						}
						err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableUniqueIndexEmailString, args)
						r.NoError(err)

					}
				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableUniqueIndexEmailString},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{"email"},
			},
			{
				"terminates when scan column is time",
				func(db *sql.DB) {
					t := time.Now()
					for i := 1; i < 10; i++ {
						t = t.Add(1000 * time.Second)
						args := map[string]interface{}{
							"id": i,
							"ts": t,
						}
						r.NoError(mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableUniqueIndexTime, args))
					}
				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableUniqueIndexTime},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{"ts"},
			},
			{
				"terminates when do a full scan",
				func(db *sql.DB) {
					for i := 1; i < 10; i++ {
						args := map[string]interface{}{
							"id": i,
						}
						err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableNoKey, args)
						r.NoError(err)
					}

				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableNoKey},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{ScanColumnForDump},
			},
			{
				"terminates when table has composite primary key",
				func(db *sql.DB) {
					for i := 1; i < 10; i++ {
						args := map[string]interface{}{
							"id":    i,
							"name":  fmt.Sprintf("name_%d", i),
							"email": fmt.Sprintf("email_%d", i),
						}
						err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableCompositePrimaryOutOfOrder, args)
						r.NoError(err)
					}

				},
				PluginConfig{
					Source: dbCfg,
					TableConfigs: []TableConfig{
						{
							Schema: testDBName,
							Table:  []string{mysql_test.TestScanColumnTableCompositePrimaryOutOfOrder},
						},
					},
					NrScanner:           1,
					TableScanBatch:      1,
					BatchPerSecondLimit: 10000,
				},
				[]string{ScanColumnForDump},
			},
			// {
			// 	"terminates when table has composite unique key",
			// 	func(db *sql.DB) {
			// 		for i := 1; i < 10; i++ {
			// 			args := map[string]interface{}{
			// 				"id": i,
			// 				"ts": time.Now(),
			// 			}
			// 			err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestScanColumnTableCompositeUniqueKey, args)
			// 			r.NoError(err)
			// 		}
			//
			// 	},
			// 	PluginConfig{
			// 		Source: dbCfg,
			// 		TableConfigs: []TableConfig{
			// 			{
			// 				Schema: testDBName,
			// 				Table:  []string{mysql_test.TestScanColumnTableCompositeUniqueKey},
			// 			},
			// 		},
			// 		NrScanner:           1,
			// 		TableScanBatch:      1,
			// 		BatchPerSecondLimit: 10000,
			// 	},
			// 	[]string{ScanColumnForDump},
			//
			// },
		}

		for _, c := range testCases {
			err := c.cfg.ValidateAndSetDefault()
			r.NoError(err)

			db := mysql_test.MustSetupSourceDB(testDBName)

			if c.seedFunc != nil {
				c.seedFunc(db)
			}

			schemaStore, err := schema_store.NewSimpleSchemaStoreFromDBConn(db)
			r.NoError(err)
			cfg := c.cfg

			tableDefs, tableConfigs := GetTables(db, schemaStore, nil, cfg.TableConfigs, nil)
			r.Equal(1, len(tableDefs))
			r.Equal(1, len(tableConfigs))

			throttle := time.NewTicker(100 * time.Millisecond)

			positionCache, err := position_cache.NewPositionCache(
				testDBName,
				positionRepo,
				EncodeBatchPositionValue,
				DecodeBatchPositionValue,
				10*time.Second)
			r.NoError(err)

			r.NoError(SetupInitialPosition(positionCache, db))

			for i := range tableDefs {
				// empty table should be ignored
				if c.seedFunc == nil {
					tableDefs, tableConfigs = DeleteEmptyTables(db, tableDefs, tableConfigs)
					r.Equal(0, len(tableDefs))
				} else {
					cnt := int64(100)
					_, err := InitTablePosition(db, positionCache, tableDefs[i], c.scanColumns, &cnt)
					r.NoError(err)
				}
			}

			if len(tableDefs) == 0 {
				continue
			}

			submitter := &fakeMsgSubmitter{}
			em, err := emitter.NewEmitter(nil, submitter)
			r.NoError(err)

			q := make(chan *TableWork, 1)
			q <- &TableWork{TableDef: tableDefs[0], TableConfig: &tableConfigs[0], ScanColumns: c.scanColumns}
			close(q)

			// randomly and delete the max value
			deleteMaxValueRandomly(
				db,
				utils.TableIdentity(tableDefs[0].Schema, tableDefs[0].Name),
				positionCache,
			)

			tableScanner := NewTableScanner(
				tt.Name(),
				q,
				db,
				positionCache,
				em,
				throttle,
				schemaStore,
				&cfg,
				context.Background(),
			)
			r.NoError(tableScanner.Start())
			tableScanner.Wait()

			// do it again, the submitter should not receive any message now.
			submitter = &fakeMsgSubmitter{}
			em, err = emitter.NewEmitter(nil, submitter)
			r.NoError(err)

			positionCache, err = position_cache.NewPositionCache(
				testDBName,
				positionRepo,
				EncodeBatchPositionValue,
				DecodeBatchPositionValue,
				10*time.Second)
			r.NoError(err)

			q = make(chan *TableWork, 1)
			q <- &TableWork{TableDef: tableDefs[0], TableConfig: &tableConfigs[0], ScanColumns: c.scanColumns}
			close(q)

			tableScanner = NewTableScanner(
				tt.Name(),
				q,
				db,
				positionCache,
				em,
				throttle,
				schemaStore,
				&cfg,
				context.Background(),
			)
			r.NoError(tableScanner.Start())
			tableScanner.Wait()
			r.Equalf(0, len(submitter.msgs), "test case: %v", c.name)
		}
	})
}

func TestGenerateNextScanQueryAndArgs(t *testing.T) {
	r := require.New(t)

	cases := []struct {
		scanColumns []string
		minArgs     []interface{}
		pivotIndex  int
		retSQL      string
	}{
		{
			[]string{"v1"},
			[]interface{}{1},
			0,
			"SELECT * FROM a.b WHERE v1 > ? ORDER BY v1 LIMIT ?",
		},
		{
			[]string{"v1", "v2"},
			[]interface{}{1, 2},
			1,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 > ? ORDER BY v1, v2 LIMIT ?",
		},
		{
			[]string{"v1", "v2"},
			[]interface{}{1, 2},
			0,
			"SELECT * FROM a.b WHERE v1 > ? ORDER BY v1, v2 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			2,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 = ? AND v3 > ? ORDER BY v1, v2, v3 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			1,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 > ? ORDER BY v1, v2, v3 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			0,
			"SELECT * FROM a.b WHERE v1 > ? ORDER BY v1, v2, v3 LIMIT ?",
		},
	}

	for _, c := range cases {
		sql, args := GenerateNextScanQueryAndArgs(
			"a.b",
			c.scanColumns,
			c.minArgs,
			c.pivotIndex,
			100)
		r.Equal(c.retSQL, sql)
		for i := 0; i <= c.pivotIndex; i++ {
			r.EqualValues(c.minArgs[i], args[i])
		}
	}
}

func TestGenerateScanQueryAndArgs(t *testing.T) {
	r := require.New(t)

	cases := []struct {
		scanColumns []string
		minArgs     []interface{}
		pivotIndex  int
		retSQL      string
	}{
		{
			[]string{"v1"},
			[]interface{}{1},
			0,
			"SELECT * FROM a.b WHERE v1 >= ? ORDER BY v1 LIMIT ?",
		},
		{
			[]string{"v1", "v2"},
			[]interface{}{1, 2},
			1,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 >= ? ORDER BY v1, v2 LIMIT ?",
		},
		{
			[]string{"v1", "v2"},
			[]interface{}{1, 2},
			0,
			"SELECT * FROM a.b WHERE v1 >= ? ORDER BY v1, v2 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			2,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 = ? AND v3 >= ? ORDER BY v1, v2, v3 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			1,
			"SELECT * FROM a.b WHERE v1 = ? AND v2 >= ? ORDER BY v1, v2, v3 LIMIT ?",
		},
		{
			[]string{"v1", "v2", "v3"},
			[]interface{}{"1", 1, 10},
			0,
			"SELECT * FROM a.b WHERE v1 >= ? ORDER BY v1, v2, v3 LIMIT ?",
		},
	}

	for _, c := range cases {
		sql, args := GenerateScanQueryAndArgs(
			"a.b",
			c.scanColumns,
			c.minArgs,
			10,
			c.pivotIndex)
		r.Equal(c.retSQL, sql)
		// excluding batch
		r.Equal(c.pivotIndex, len(args)-1-1)
		for i := 0; i <= c.pivotIndex; i++ {
			r.EqualValues(c.minArgs[i], args[i])
		}
	}
}

func TestScanValuesFromRowValues(t *testing.T) {
	r := require.New(t)

	rowValues := []interface{}{1, 2, 3, 4, 5}
	scanIndexes := []int{1, 4}

	scanValues := ScanValuesFromRowValues(rowValues, scanIndexes)
	r.EqualValues([]interface{}{2, 5}, scanValues)

}

func TestGreaterThanMax(t *testing.T) {
	r := require.New(t)

	dbName := utils.TestCaseMd5Name(t)
	db := mysql_test.MustSetupSourceDB(dbName)

	tcs := []struct {
		scanValues []interface{}
		maxValues  []interface{}
		expected   bool
	}{
		{
			[]interface{}{1, 2, 3},
			[]interface{}{1, 2, 4},
			false,
		},
		{
			[]interface{}{1, 2, 3},
			[]interface{}{1, 3, 1},
			false,
		},
		{
			[]interface{}{1, 2, 3},
			[]interface{}{2, 1, 1},
			false,
		},
		{
			[]interface{}{1, 2, 3},
			[]interface{}{1, 2, 3},
			false,
		},
		{
			[]interface{}{1, 2, 4},
			[]interface{}{1, 2, 3},
			true,
		},
		{
			[]interface{}{1, 3, 1},
			[]interface{}{1, 2, 3},
			true,
		},
		{
			[]interface{}{2, 1, 1},
			[]interface{}{1, 2, 3},
			true,
		},
	}

	for _, c := range tcs {
		b, err := GreaterThanMax(db, c.scanValues, c.maxValues)
		r.NoError(err)
		r.Equalf(c.expected, b, fmt.Sprintf("scanValue: %+v, maxValue: %+v", c.scanValues, c.maxValues))
	}
}

func TestNextScanElementForChunk(t *testing.T) {
	r := require.New(t)

	dbName := utils.TestCaseMd5Name(t)
	db := mysql_test.MustSetupSourceDB(dbName)

	mysql_test.SeedCompositePrimaryKeyInt(db, dbName)

	fullTableName := utils.TableIdentity(
		dbName,
		mysql_test.TestScanColumnTableCompositePrimaryInt)

	columnTypes, err := GetTableColumnTypes(db, dbName, mysql_test.TestScanColumnTableCompositePrimaryInt)
	r.NoError(err)

	tcs := []struct {
		currentScanValues []interface{}
		pivotIndex        int
		nextRowValues     []interface{}
		exists            bool
	}{
		{
			[]interface{}{1, 6, 1},
			2,
			[]interface{}{1, 6, 2, 2},
			true,
		},
		{
			[]interface{}{1, 6, 3},
			2,
			[]interface{}{1, 6, 4, 4},
			true,
		},
		{
			[]interface{}{1, 6, 4},
			2,
			nil,
			false,
		},
		{
			[]interface{}{1, 6, 4},
			1,
			[]interface{}{1, 7, 1, 5},
			true,
		},
		{
			[]interface{}{1, 7, 4},
			2,
			nil,
			false,
		},
		{
			[]interface{}{1, 7, 4},
			1,
			[]interface{}{1, 8, 1, 9},
			true,
		},
		{
			[]interface{}{1, 8, 4},
			1,
			nil,
			false,
		},
		{
			[]interface{}{1, 8, 4},
			0,
			[]interface{}{2, 1, 1, 13},
			true,
		},
		{
			[]interface{}{2, 1, 1},
			2,
			[]interface{}{2, 1, 2, 14},
			true,
		},
		{
			[]interface{}{2, 3, 4},
			2,
			nil,
			false,
		},
	}

	for _, c := range tcs {
		nextRows, exists, err := NextScanElementForChunk(
			db,
			fullTableName,
			columnTypes,
			[]string{"a", "b", "c"},
			c.currentScanValues,
			c.pivotIndex)
		r.NoError(err)
		r.Equalf(c.exists, exists, "scanValues: %+v, nextRowValues:%+v", c.currentScanValues, c.nextRowValues)
		if exists {
			for i := range nextRows {
				r.EqualValues(c.nextRowValues[i], nextRows[i])
			}
		}
	}
}

func TestNextBatchStartPoint(t *testing.T) {
	r := require.New(t)
	dbName := utils.TestCaseMd5Name(t)
	db := mysql_test.MustSetupSourceDB(dbName)
	mysql_test.SeedCompositePrimaryKeyInt(db, dbName)

	fullTableName := utils.TableIdentity(
		dbName,
		mysql_test.TestScanColumnTableCompositePrimaryInt)

	columnTypes, err := GetTableColumnTypes(db, dbName, mysql_test.TestScanColumnTableCompositePrimaryInt)
	r.NoError(err)

	tcs := []struct {
		currentMinValues []interface{}
		maxValues        []interface{}
		retNextMinValues []interface{}
		nextPivotIndex   int
		retContinue      bool
	}{
		// test with maxValues
		{
			[]interface{}{1, 6, 1},
			[]interface{}{2, 3, 4},
			[]interface{}{1, 6, 2},
			2,
			true,
		},
		{
			[]interface{}{1, 6, 1},
			[]interface{}{1, 6, 0},
			nil,
			0,
			false,
		},
		{
			[]interface{}{1, 6, 1},
			[]interface{}{1, 6, 1},
			nil,
			0,
			false,
		},

		{
			[]interface{}{1, 6, 1},
			[]interface{}{1, 6, 2},
			[]interface{}{1, 6, 2},
			2,
			true,
		},
		{
			[]interface{}{1, 6, 4},
			[]interface{}{1, 7, 1},
			[]interface{}{1, 7, 1},
			1,
			true,
		},
		{
			[]interface{}{1, 7, 4},
			[]interface{}{2, 7, 1},
			[]interface{}{1, 8, 1},
			1,
			true,
		},
		{
			[]interface{}{1, 8, 4},
			[]interface{}{2, 3, 1},
			[]interface{}{2, 1, 1},
			0,
			true,
		},
		{
			[]interface{}{2, 2, 1},
			[]interface{}{2, 3, 1},
			[]interface{}{2, 2, 2},
			2,
			true,
		},
	}

	for _, c := range tcs {
		nextMinValues, continueNext, nextPivotIndex, err := NextBatchStartPoint(
			db,
			fullTableName,
			[]string{"a", "b", "c"},
			columnTypes,
			[]int{0, 1, 2},
			c.currentMinValues,
			c.maxValues,
		)
		r.NoError(err)
		r.Equalf(c.retContinue, continueNext, "currentMinValues: %+v, maxValues: %+v", c.currentMinValues, c.maxValues)
		if c.retContinue {
			r.Equal(len(c.currentMinValues), len(nextMinValues))
			for i := range nextMinValues {
				r.EqualValuesf(c.retNextMinValues[i], nextMinValues[i], "currentMinValues: %+v, maxValues: %+v", c.currentMinValues, c.maxValues)
			}
			r.Equal(c.nextPivotIndex, nextPivotIndex)
		}
	}

}

func deleteMaxValueRandomly(db *sql.DB, fullTableName string, positionCache position_cache.PositionCacheInterface) {
	p, exists, err := positionCache.Get()
	if err != nil {
		panic(err.Error())
	}

	if !exists {
		panic("empty position")
	}

	batchPositionValue := p.Value.(BatchPositionValueV1)
	stats := batchPositionValue.TableStates[fullTableName]
	// Skip multiple scan key and full dump
	if len(stats.Max) != 1 || stats.Max[0].Column == "*" {
		return
	}
	if rand.Float32() < 0.5 {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", fullTableName, stats.Max[0].Column), stats.Max[0].Value)
		if err != nil {
			panic(err.Error())
		}
	}

}
