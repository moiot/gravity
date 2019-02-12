package mysqlbatch

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/parser"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/mysql"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
)

type TableScanner struct {
	pipelineName  string
	tableWorkC    chan *TableWork
	cfg           *PluginConfig
	positionStore position_store.MySQLTablePositionStore
	db            *sql.DB
	emitter       core.Emitter
	throttle      *time.Ticker
	ctx           context.Context
	schemaStore   schema_store.SchemaStore
	wg            sync.WaitGroup
	parser        *parser.Parser
}

func (tableScanner *TableScanner) Start() error {

	tableScanner.wg.Add(1)
	go func() {
		defer tableScanner.wg.Done()

		for {
			select {
			case work, ok := <-tableScanner.tableWorkC:
				if !ok {
					log.Infof("[TableScanner] queue closed, exit")
					return
				}

				err := tableScanner.initTableDDL(work.TableDef)
				if err != nil {
					log.Fatalf("[TableScanner] initTableDDL for %s.%s, err: %s", work.TableDef.Schema, work.TableDef.Name, err)
				}

				if utils.IsTableEmpty(tableScanner.db, work.TableDef.Schema, work.TableDef.Name) {
					msg := NewCloseInputStreamMsg(work.TableDef)
					if err := tableScanner.emitter.Emit(msg); err != nil {
						log.Fatalf("[LoopInBatch] failed to emit close stream msg: %v", errors.ErrorStack(err))
					}
					log.Infof("[TableScanner] table %s.%s is empty.", work.TableDef.Schema, work.TableDef.Name)
					continue
				}

				err = tableScanner.InitTablePosition(work.TableDef, work.TableConfig)
				if err != nil {
					log.Fatalf("[TableScanner] InitTablePosition failed: %v", errors.ErrorStack(err))
				}
				max, min, ok := tableScanner.positionStore.GetMaxMin(utils.TableIdentity(work.TableDef.Schema, work.TableDef.Name))
				log.Infof("positionStore.GetMaxMin: max value type: %v, max %v; min value type: %v, min %v", reflect.TypeOf(max.Value), max, reflect.TypeOf(min.Value), min)
				scanColumn := max.Column
				if !ok {
					log.Fatalf("[table_scanner] failed to find max min")
				}
				// If the scan column is *, then we do a full dump of the table
				if scanColumn == "*" {
					tableScanner.FindAll(tableScanner.db, work.TableDef, work.TableConfig)
				} else {
					tableScanner.LoopInBatch(
						tableScanner.db, work.TableDef,
						work.TableConfig, scanColumn,
						max,
						min,
						tableScanner.cfg.TableScanBatch)

					if tableScanner.ctx.Err() == nil {
						log.Infof("[table_worker] LoopInBatch done with table %s", work.TableDef.Name)
					} else if tableScanner.ctx.Err() == context.Canceled {
						log.Infof("[TableScanner] LoopInBatch canceled")
						return
					} else {
						log.Fatalf("[TableScanner] LoopInBatch unknow case,err: %v", tableScanner.ctx.Err())
					}
				}

			case <-tableScanner.ctx.Done():
				log.Infof("[TableScanner] canceled by context")
				return
			}
		}
	}()

	return nil
}

func (tableScanner *TableScanner) InitTablePosition(tableDef *schema_store.Table, tableConfig *TableConfig) error {
	_, _, ok := tableScanner.positionStore.GetMaxMin(utils.TableIdentity(tableDef.Schema, tableDef.Name))
	if !ok {
		log.Infof("[InitTablePosition] init table position")

		// detect scan column first
		var scanColumn string
		var scanType string

		column, err := DetectScanColumn(tableScanner.db, tableDef.Schema, tableDef.Name, tableScanner.cfg.MaxFullDumpCount)
		if err != nil {
			return errors.Trace(err)
		}
		scanColumn = column

		if scanColumn == "*" {
			maxPos := position_store.MySQLTablePosition{Column: scanColumn}
			minPos := position_store.MySQLTablePosition{Column: scanColumn}
			tableScanner.positionStore.PutMaxMin(utils.TableIdentity(tableDef.Schema, tableDef.Name), maxPos, minPos)
		} else {
			max, min := FindMaxMinValueFromDB(tableScanner.db, tableDef.Schema, tableDef.Name, scanColumn)
			maxPos := position_store.MySQLTablePosition{Value: max, Type: scanType, Column: scanColumn}
			minPos := position_store.MySQLTablePosition{Value: min, Type: scanType, Column: scanColumn}
			tableScanner.positionStore.PutMaxMin(utils.TableIdentity(tableDef.Schema, tableDef.Name), maxPos, minPos)
			log.Infof("[InitTablePosition] PutMaxMin: max value type: %v, max: %v; min value type: %v, min: %v", reflect.TypeOf(maxPos.Value), maxPos, reflect.TypeOf(minPos.Value), minPos)
		}
		log.Infof("[InitTablePosition] schema: %v, table: %v, scanColumn: %v", tableDef.Schema, tableDef.Name, scanColumn)
	}
	return nil
}

func (tableScanner *TableScanner) Wait() {
	tableScanner.wg.Wait()
}

// DetectScanColumn find a column that we used to scan the table
// SHOW INDEX FROM ..
// Pick primary key, if there is only one primary key
// If pk not found try using unique index
// fail
func DetectScanColumn(sourceDB *sql.DB, dbName string, tableName string, maxFullDumpRowsCount int) (string, error) {
	pks, err := utils.GetPrimaryKeys(sourceDB, dbName, tableName)
	if err != nil {
		return "", errors.Trace(err)
	}

	if len(pks) == 1 {
		return pks[0], nil
	}

	uniqueIndexes, err := utils.GetUniqueIndexesWithoutPks(sourceDB, dbName, tableName)
	if err != nil {
		return "", errors.Trace(err)
	}

	if len(uniqueIndexes) > 0 {
		return uniqueIndexes[0], nil
	}

	rowsCount, err := utils.EstimateRowsCount(sourceDB, dbName, tableName)
	if err != nil {
		return "", errors.Trace(err)
	}

	if rowsCount < maxFullDumpRowsCount {
		return "*", nil
	}

	return "", errors.Errorf("no scan column can be found automatically for %s.%s", dbName, tableName)
}

func FindMaxMinValueFromDB(db *sql.DB, dbName string, tableName string, scanColumn string) (interface{}, interface{}) {
	var max interface{}
	var min interface{}

	maxStatement := fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`.`%s`", scanColumn, dbName, tableName)
	log.Infof("[FindMaxMinValueFromDB] statement: %s", maxStatement)

	maxRowPtrs, err := utils.QueryGeneralRowsDataWithSQL(db, maxStatement)
	if err != nil {
		log.Fatalf("[FindMaxMinValueFromDB] failed to QueryGeneralRowsDataWithSQL, err: %v", errors.ErrorStack(err))
	}

	max = reflect.ValueOf(maxRowPtrs[0][0]).Elem().Interface()

	minStatement := fmt.Sprintf("SELECT MIN(`%s`) FROM `%s`.`%s`", scanColumn, dbName, tableName)
	log.Infof("[FindMaxMinValueFromDB] statement: %s", minStatement)
	minRowPtrs, err := utils.QueryGeneralRowsDataWithSQL(db, minStatement)
	if err != nil {
		log.Fatalf("[FindMaxMinValueFromDB] failed to QueryGeneralRowsDataWithSQL, err: %v", errors.ErrorStack(err))
	}

	min = reflect.ValueOf(minRowPtrs[0][0]).Elem().Interface()

	return max, min
}

// LoopInBatch will iterate the table by sql like this:
// SELECT * FROM a WHERE some_key > some_value LIMIT 10000
// It will get the min, max value of the column and iterate batch by batch
func (tableScanner *TableScanner) LoopInBatch(db *sql.DB, tableDef *schema_store.Table, tableConfig *TableConfig, scanColumn string, max position_store.MySQLTablePosition, min position_store.MySQLTablePosition, batch int) {
	pipelineName := tableScanner.pipelineName

	if batch <= 0 {
		log.Fatalf("[LoopInBatch] batch size is 0")
	}

	maxMapString, err := max.MapString()
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get maxString, max: %v, err: %v", max, errors.ErrorStack(err))
	}
	batchIdx := 0
	firstLoop := true
	maxReached := false
	var statement string

	currentMinPos, ok := tableScanner.positionStore.GetCurrent(utils.TableIdentity(tableDef.Schema, tableDef.Name))
	if !ok {
		tableScanner.positionStore.PutCurrent(utils.TableIdentity(tableDef.Schema, tableDef.Name), min)
		currentMinPos = min
	}
	log.Infof("[LoopInBatch] prepare current: %v", currentMinPos)

	currentMinValue := currentMinPos.Value
	resultCount := 0

	columnTypes, err := GetTableColumnTypes(db, tableDef.Schema, tableDef.Name)
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get columnType, err: %v", errors.ErrorStack(err))
	}

	scanIdx, err := GetScanIdx(columnTypes, scanColumn)
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get scanIdx, err: %v", errors.ErrorStack(err))
	}

	rowsBatchDataPtrs := newBatchDataPtrs(columnTypes, batch)

	for {

		if firstLoop {
			statement = fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s >= ? ORDER BY %s LIMIT ?", tableDef.Schema, tableDef.Name, scanColumn, scanColumn)
			firstLoop = false
		} else {
			statement = fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s > ? ORDER BY %s LIMIT ?", tableDef.Schema, tableDef.Name, scanColumn, scanColumn)
		}

		<-tableScanner.throttle.C

		queryStartTime := time.Now()
		rows, err := db.Query(statement, currentMinValue, batch)
		if err != nil {
			log.Fatalf("[LoopInBatch] table %s.%s, err: %v", tableDef.Schema, tableDef.Name, err)
		}

		rowIdx := 0
		for rows.Next() {

			metrics.ScannerJobFetchedCount.WithLabelValues(pipelineName).Add(1)

			resultCount++

			rowsBatchDataPtrs[rowIdx], err = utils.ScanGeneralRowsWithDataPtrs(rows, columnTypes, rowsBatchDataPtrs[rowIdx])
			if err != nil {
				log.Fatalf("[LoopInBatch] table %s.%s, scan error: %v", tableDef.Schema, tableDef.Name, errors.ErrorStack(err))
			}

			currentMinValue = reflect.ValueOf(rowsBatchDataPtrs[rowIdx][scanIdx]).Elem().Interface()
			rowIdx++

			if mysql.MySQLDataEquals(max.Value, currentMinValue) {
				maxReached = true
				break
			}
		}

		err = rows.Err()
		if err != nil {
			log.Fatalf("[LoopInBatch] table %s.%s, rows err: %v", tableDef.Schema, tableDef.Name, err)
		}

		rows.Close()

		// no result found for this query
		if rowIdx == 0 {
			log.Infof("[TableScanner] query result is 0, return")
			return
		}

		metrics.ScannerBatchQueryDuration.WithLabelValues(pipelineName).Observe(time.Now().Sub(queryStartTime).Seconds())

		batchIdx++

		var lastMsg *core.Msg
		// process this batch's data
		for i := 0; i < rowIdx; i++ {
			rowPtrs := rowsBatchDataPtrs[i]
			posV := mysql.NormalizeSQLType(reflect.ValueOf(rowPtrs[scanIdx]).Elem().Interface())
			position := position_store.MySQLTablePosition{Value: posV, Column: scanColumn}

			msg := NewMsg(rowPtrs, columnTypes, tableDef, tableScanner.AfterMsgCommit, position)

			if err := tableScanner.emitter.Emit(msg); err != nil {
				log.Fatalf("[LoopInBatch] failed to emit job: %v", errors.ErrorStack(err))
			}

			lastMsg = msg
		}

		log.Infof("[LoopInBatch] sourceDB: %s, table: %s, currentMinPos: %v, maxMapString.column: %v, maxMapString.value: %v, maxMapString.type: %v, resultCount: %v",
			tableDef.Schema, tableDef.Name, currentMinValue, maxMapString["column"], maxMapString["value"], maxMapString["type"], resultCount)

		// we break the loop here in case the currentMinPos comes larger than the max we have in the beginning.
		if maxReached {

			log.Infof("[LoopInBatch] %s.%s max reached", tableDef.Schema, tableDef.Name)

			if lastMsg != nil {
				<-lastMsg.Done

				// close the stream
				msg := NewCloseInputStreamMsg(tableDef)
				if err := tableScanner.emitter.Emit(msg); err != nil {
					log.Fatalf("[LoopInBatch] failed to emit close stream msg: %v", errors.ErrorStack(err))
				}
				log.Infof("[LoopInBatch] sent close input stream msg for %s", *msg.InputStreamKey)
			}
			return
		}

		select {
		case <-tableScanner.ctx.Done():
			log.Infof("[table_worker] canceled by context")
			return
		default:
			continue
		}
	}
}

func (tableScanner *TableScanner) FindAll(db *sql.DB, tableDef *schema_store.Table, tableConfig *TableConfig) {
	columnTypes, err := GetTableColumnTypes(db, tableDef.Schema, tableDef.Name)
	if err != nil {
		log.Fatalf("[FindAll] failed to get columnType: %v", errors.ErrorStack(err))
	}

	statement := fmt.Sprintf("SELECT * FROM `%s`.`%s`", tableDef.Schema, tableDef.Name)

	allData, err := utils.QueryGeneralRowsDataWithSQL(db, statement)
	if err != nil {
		log.Fatalf("[FindAll] failed to find all, err: %v", errors.ErrorStack(err))
	}

	for i := range allData {
		rowPtrs := allData[i]
		msg := NewMsg(rowPtrs, columnTypes, tableDef, nil, position_store.MySQLTablePosition{})
		if err := tableScanner.emitter.Emit(msg); err != nil {
			log.Fatalf("[tableScanner] failed to emit: %v", errors.ErrorStack(err))
		}
	}
}

func (tableScanner *TableScanner) AfterMsgCommit(msg *core.Msg) error {
	p, ok := msg.InputContext.(position_store.MySQLTablePosition)
	if !ok {
		return errors.Errorf("type invalid")
	}

	tableScanner.positionStore.PutCurrent(*msg.InputStreamKey, p)

	return nil
}

func (tableScanner *TableScanner) initTableDDL(table *schema_store.Table) error {
	row := tableScanner.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", table.Schema, table.Name))
	var t, create string
	err := row.Scan(&t, &create)
	if err != nil {
		return errors.Trace(err)
	}

	msg := NewCreateTableMsg(tableScanner.parser, table, create)

	if err := tableScanner.emitter.Emit(msg); err != nil {
		return errors.Trace(err)
	}

	<-msg.Done

	return nil
}

func GetTableColumnTypes(db *sql.DB, schema string, table string) ([]*sql.ColumnType, error) {
	statement := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", schema, table)
	rows, err := db.Query(statement)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func GetScanIdx(columnTypes []*sql.ColumnType, scanColumn string) (int, error) {
	for i := range columnTypes {
		if columnTypes[i].Name() == scanColumn {
			return i, nil
		}
	}
	return 0, errors.Errorf("cannot find scan index")
}

func newBatchDataPtrs(columnTypes []*sql.ColumnType, batch int) [][]interface{} {
	ret := make([][]interface{}, batch)
	for batchIdx := 0; batchIdx < batch; batchIdx++ {
		vPtrs := make([]interface{}, len(columnTypes))
		for columnIdx, _ := range columnTypes {
			scanType := utils.GetScanType(columnTypes[columnIdx])
			vptr := reflect.New(scanType)
			vPtrs[columnIdx] = vptr.Interface()
		}
		ret[batchIdx] = vPtrs
	}
	return ret
}

func NewTableScanner(
	pipelineName string,
	tableWorkC chan *TableWork,
	db *sql.DB,
	positionStore position_store.MySQLTablePositionStore,
	emitter core.Emitter,
	throttle *time.Ticker,
	schemaStore schema_store.SchemaStore,
	cfg *PluginConfig,
	ctx context.Context) *TableScanner {

	tableScanner := TableScanner{
		pipelineName:  pipelineName,
		tableWorkC:    tableWorkC,
		db:            db,
		positionStore: positionStore,
		emitter:       emitter,
		throttle:      throttle,
		schemaStore:   schemaStore,
		cfg:           cfg,
		ctx:           ctx,
		parser:        parser.New(),
	}
	return &tableScanner
}

func String2Val(s string, scanType string) interface{} {
	var currentMin interface{}
	var err error
	if scanType == "string" {
		currentMin = s
	} else if scanType == "int" {
		currentMin, err = strconv.Atoi(s)
		if err != nil {
			log.Fatalf("[LoopInBatch] failed to convert string to int: %v", err)
		}
	} else {
		log.Infof("[LoopInBatch] scanColumn not supported")
	}
	return currentMin
}
