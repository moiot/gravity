package mysqlbatch

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/emitter"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/schema_store"
)

func TestFindMaxMinValueInt(t *testing.T) {
	assert := assert.New(t)
	testDBName := "mysql_table_scanner_test_1"

	db := mysql_test.MustSetupSourceDB(testDBName)
	defer db.Close()

	for i := 1; i < 100; i++ {
		args := map[string]interface{}{
			"id": i,
		}
		err := mysql_test.InsertIntoTestTable(db, testDBName, mysql_test.TestTableName, args)
		if err != nil {
			assert.FailNow(err.Error())
		}
	}

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, "id")

	maxVal, ok := max.(sql.NullInt64)
	assert.True(ok)

	assert.EqualValues(99, maxVal.Int64)

	minVal, ok := min.(sql.NullInt64)
	assert.True(ok)
	assert.EqualValues(1, minVal.Int64)
}

func TestFindMaxMinValueString(t *testing.T) {
	r := require.New(t)
	testDBName := "mysql_table_scanner_test_2"

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

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, "name")

	maxV, ok1 := max.(sql.NullString)
	r.True(ok1)

	minV, ok2 := min.(sql.NullString)
	r.True(ok2)
	r.Equal("test_2", maxV.String)
	r.Equal("test_1", minV.String)
}

func TestFindMaxMinValueTime(t *testing.T) {
	r := require.New(t)
	testDBName := "mysql_table_scanner_test_3"

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

	max, min := FindMaxMinValueFromDB(db, testDBName, mysql_test.TestTableName, "ts")
	maxT := max.(mysql.NullTime)
	minT := min.(mysql.NullTime)
	// assert.True(t, reflect.DeepEqual(mysql.NullTime{Time: startTime.Add(99 * time.Second), Valid: true}, maxT))
	// assert.True(t, reflect.DeepEqual(mysql.NullTime{Time: startTime, Valid: true}, minT))
	assert.EqualValues(t, startTime.Add(99*time.Minute).Minute(), maxT.Time.Minute())
	assert.EqualValues(t, startTime.Minute(), minT.Time.Minute())
}

func TestDetectScanColumn(t *testing.T) {
	r := require.New(t)
	t.Run("returns the primary", func(tt *testing.T) {
		testDBName := "mysql_table_scanner_test_4"

		db := mysql_test.MustSetupSourceDB(testDBName)
		col, _, err := DetectScanColumn(db, testDBName, mysql_test.TestScanColumnTableIdPrimary, 1000)
		r.Nil(err)
		r.Equal("id", col)
	})

	t.Run("returns * if only have multiple primary key", func(tt *testing.T) {
		testDBName := "mysql_table_scanner_test_5"

		db := mysql_test.MustSetupSourceDB(testDBName)
		c, _, err := DetectScanColumn(db, testDBName, mysql_test.TestScanColumnTableMultiPrimary, 1000)
		r.Nil(err)
		r.Equal("*", c)
	})

	t.Run("returns error if only have multiple primary key", func(tt *testing.T) {
		testDBName := "mysql_table_scanner_test_6"

		db := mysql_test.MustSetupSourceDB(testDBName)
		_, _, err := DetectScanColumn(db, testDBName, mysql_test.TestScanColumnTableMultiPrimary, 0)
		r.NotNil(err)
	})

	t.Run("returns the uniq index", func(tt *testing.T) {
		testDBName := "mysql_table_scanner_test_7"

		db := mysql_test.MustSetupSourceDB(testDBName)
		col, _, err := DetectScanColumn(db, testDBName, mysql_test.TestScanColumnTableUniqueIndexEmailString, 1000)
		r.Nil(err)
		r.Equal("email", col)
	})
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
		positionRepo, err := position_store.NewMySQLRepo(dbCfg, "")
		r.NoError(err)

		testCases := []struct {
			name       string
			seedFunc   func(db *sql.DB)
			cfg        PluginConfig
			scanColumn string
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
				"id",
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
				"id",
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
				"id",
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
				"email",
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
				"ts",
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
				"*",
			},
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

			tableDefs, tableConfigs := GetTables(db, schemaStore, cfg.TableConfigs, nil)
			r.Equal(1, len(tableDefs))
			r.Equal(1, len(tableConfigs))

			throttle := time.NewTicker(100 * time.Millisecond)

			positionCache, err := position_store.NewPositionCache(testDBName, positionRepo, 10*time.Second)
			r.NoError(err)

			r.NoError(SetupInitialPosition(positionCache, db))

			submitter := &fakeMsgSubmitter{}
			em, err := emitter.NewEmitter(nil, submitter)
			r.NoError(err)

			q := make(chan *TableWork, 1)
			q <- &TableWork{TableDef: tableDefs[0], TableConfig: &tableConfigs[0], ScanColumn: c.scanColumn}
			close(q)

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

			positionCache, err = position_store.NewPositionCache(testDBName, positionRepo, 10*time.Second)
			r.NoError(err)

			q = make(chan *TableWork, 1)
			q <- &TableWork{TableDef: tableDefs[0], TableConfig: &tableConfigs[0], ScanColumn: c.scanColumn}
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
