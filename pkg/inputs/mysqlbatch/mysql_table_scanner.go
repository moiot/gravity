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
	positionCache position_store.PositionCacheInterface
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
			case work, exists := <-tableScanner.tableWorkC:
				if !exists {
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

				err = tableScanner.InitTablePosition(work.TableDef, work.TableConfig, work.ScanColumn, work.EstimatedRowCount)
				if err != nil {
					log.Fatalf("[TableScanner] InitTablePosition failed: %v", errors.ErrorStack(err))
				}
				max, min, exists, err := GetMaxMin(tableScanner.positionCache, utils.TableIdentity(work.TableDef.Schema, work.TableDef.Name))
				if err != nil {
					log.Fatalf("[TableScanner] InitTablePosition failed: %v", errors.ErrorStack(err))
				}
				if !exists {
					log.Fatalf("[table_scanner] failed to find max min, table: %v", utils.TableIdentity(work.TableDef.Schema, work.TableDef.Name))
				}

				log.Infof("positionCache.GetMaxMin: max value type: %v, max %v; min value type: %v, min %v", reflect.TypeOf(max.Value), max, reflect.TypeOf(min.Value), min)
				scanColumn := max.Column

				// If the scan column is *, then we do a full dump of the table
				if scanColumn == "*" {
					tableScanner.FindAll(tableScanner.db, work.TableDef, work.TableConfig)
				} else {
					tableScanner.LoopInBatch(
						tableScanner.db, work.TableDef,
						work.TableConfig, scanColumn,
						*max,
						*min,
						tableScanner.cfg.TableScanBatch)

					if tableScanner.ctx.Err() == nil {
						log.Infof("[table_worker] LoopInBatch done with table %s", utils.TableIdentity(work.TableDef.Schema, work.TableDef.Name))
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

func (tableScanner *TableScanner) InitTablePosition(tableDef *schema_store.Table, tableConfig *TableConfig, scanColumn string, estimatedRowCount int64) error {
	fullTableName := utils.TableIdentity(tableDef.Schema, tableDef.Name)
	_, _, exists, err := GetMaxMin(tableScanner.positionCache, fullTableName)
	if err != nil {
		log.Fatalf("[tableScanner] failed to GetMaxMin: %v", errors.ErrorStack(err))
	}

	if !exists {
		log.Infof("[InitTablePosition] init table position")

		var scanType string
		if scanColumn == "*" {
			maxPos := TablePosition{Column: scanColumn, Type: PlainInt, Value: 1}
			minPos := TablePosition{Column: scanColumn, Type: PlainInt, Value: 0}
			if err := PutMaxMin(tableScanner.positionCache, fullTableName, &maxPos, &minPos); err != nil {
				return errors.Trace(err)
			}
		} else {
			max, min := FindMaxMinValueFromDB(tableScanner.db, tableDef.Schema, tableDef.Name, scanColumn)
			maxPos := TablePosition{Value: max, Type: scanType, Column: scanColumn}
			minPos := TablePosition{Value: min, Type: scanType, Column: scanColumn}
			if err := PutMaxMin(tableScanner.positionCache, fullTableName, &maxPos, &minPos); err != nil {
				log.Fatalf("[InitTablePosition] failed to PutMaxMin, err: %v", errors.ErrorStack(err))
			}
			log.Infof("[InitTablePosition] PutMaxMin: max value type: %v, max: %v; min value type: %v, min: %v", reflect.TypeOf(maxPos.Value), maxPos, reflect.TypeOf(minPos.Value), minPos)
		}

		if err := PutEstimatedCount(tableScanner.positionCache, fullTableName, estimatedRowCount); err != nil {
			log.Fatalf("[InitTablePosition] failed to put estimated count, err: %v", errors.ErrorStack(err))
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
func DetectScanColumn(sourceDB *sql.DB, dbName string, tableName string, maxFullDumpRowsCount int64) (string, int64, error) {
	rowsCount, err := utils.EstimateRowsCount(sourceDB, dbName, tableName)
	if err != nil {
		return "", 0, errors.Trace(err)
	}

	pks, err := utils.GetPrimaryKeys(sourceDB, dbName, tableName)
	if err != nil {
		return "", 0, errors.Trace(err)
	}

	if len(pks) == 1 {
		return pks[0], rowsCount, nil
	}

	uniqueIndexes, err := utils.GetUniqueIndexesWithoutPks(sourceDB, dbName, tableName)
	if err != nil {
		return "", 0, errors.Trace(err)
	}

	if len(uniqueIndexes) > 0 {
		return uniqueIndexes[0], rowsCount, nil
	}

	if rowsCount < maxFullDumpRowsCount {
		return "*", rowsCount, nil
	}

	return "", rowsCount, errors.Errorf("no scan column can be found automatically for %s.%s", dbName, tableName)
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
func (tableScanner *TableScanner) LoopInBatch(db *sql.DB, tableDef *schema_store.Table, tableConfig *TableConfig, scanColumn string, max TablePosition, min TablePosition, batch int) {
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

	currentPosition, exists, err := GetCurrentPos(tableScanner.positionCache, utils.TableIdentity(tableDef.Schema, tableDef.Name))
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get current pos: %v", errors.ErrorStack(err))
	}

	if !exists {
		currentPosition = &min
	} else {
		if mysql.MySQLDataEquals(currentPosition.Value, max.Value) {
			log.Infof("[LoopInBatch] already scanned: %v", utils.TableIdentity(tableDef.Schema, tableDef.Name))
			return
		}
	}

	log.Infof("[LoopInBatch] prepare current: %v", currentPosition)

	currentMinValue := currentPosition.Value
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

		// process this batch's data
		for i := 0; i < rowIdx; i++ {
			rowPtrs := rowsBatchDataPtrs[i]
			posV := mysql.NormalizeSQLType(reflect.ValueOf(rowPtrs[scanIdx]).Elem().Interface())
			position := TablePosition{Value: posV, Column: scanColumn}

			msg := NewMsg(rowPtrs, columnTypes, tableDef, tableScanner.AfterMsgCommit, position)

			if err := tableScanner.emitter.Emit(msg); err != nil {
				log.Fatalf("[LoopInBatch] failed to emit job: %v", errors.ErrorStack(err))
			}
		}

		log.Infof("[LoopInBatch] sourceDB: %s, table: %s, currentPosition: %v, maxMapString.column: %v, maxMapString.value: %v, maxMapString.type: %v, resultCount: %v",
			tableDef.Schema, tableDef.Name, currentMinValue, maxMapString["column"], maxMapString["value"], maxMapString["type"], resultCount)

		// we break the loop here in case the currentPosition comes larger than the max we have in the beginning.
		if maxReached {

			log.Infof("[LoopInBatch] finish table: %v", utils.TableIdentity(tableDef.Schema, tableDef.Name))

			if err := waitAndCloseStream(tableDef, tableScanner.emitter); err != nil {
				log.Fatalf("[LoopInBatch] failed to emit close stream closeMsg: %v", errors.ErrorStack(err))
			}

			if err := tableScanner.positionCache.Flush(); err != nil {
				log.Fatalf("[LoopInBatch] failed to flush position cache: %v", errors.ErrorStack(err))
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
	streamKey := utils.TableIdentity(tableDef.Schema, tableDef.Name)

	// do not re-scan table that has already dumped
	current, currentExists, err := GetCurrentPos(tableScanner.positionCache, streamKey)
	if err != nil {
		log.Fatalf("[FindAll] failed to get current pos: %v", errors.ErrorStack(err))
	}

	max, _, maxExists, err := GetMaxMin(tableScanner.positionCache, streamKey)
	if err != nil {
		log.Fatalf("[FindAll] failed to get max min: %v", errors.ErrorStack(err))
	}
	if currentExists && maxExists {
		if reflect.DeepEqual(max.Value, current.Value) {
			log.Infof("[FindAll] table: %v already dumped", streamKey)
			return
		}
	}

	log.Infof("[tableScanner] dump table: %s", streamKey)

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
		msg := NewMsg(rowPtrs, columnTypes, tableDef, tableScanner.AfterMsgCommit, TablePosition{Column: "*", Type: PlainInt, Value: i + 1})
		if err := tableScanner.emitter.Emit(msg); err != nil {
			log.Fatalf("[FindAll] failed to emit: %v", errors.ErrorStack(err))
		}
	}

	// set the current and max position to be the same
	p := TablePosition{Column: "*", Type: PlainInt, Value: len(allData)}

	if err := PutCurrentPos(tableScanner.positionCache, utils.TableIdentity(tableDef.Schema, tableDef.Name), &p, false); err != nil {
		log.Fatalf("[FindAll] failed to put current pos: %v", errors.ErrorStack(err))
	}

	if err := PutMaxMin(
		tableScanner.positionCache,
		utils.TableIdentity(tableDef.Schema, tableDef.Name),
		&p,
		&TablePosition{Column: "*", Type: PlainInt, Value: 0}); err != nil {
		log.Fatalf("[FindAll] failed to put max min: %v", errors.ErrorStack(err))
	}

	log.Infof("[FindAll] finished dump table: %v", streamKey)

	// close the stream
	if err := waitAndCloseStream(tableDef, tableScanner.emitter); err != nil {
		log.Fatalf("[FindAll] failed to emit close stream closeMsg: %v", errors.ErrorStack(err))
	}

	if err := tableScanner.positionCache.Flush(); err != nil {
		log.Fatalf("[FindAll] failed to flush position cache, err: %v", errors.ErrorStack(err))
	}
	log.Infof("[FindAll] sent close input stream closeMsg")
}

func (tableScanner *TableScanner) AfterMsgCommit(msg *core.Msg) error {
	p, ok := msg.InputContext.(TablePosition)
	if !ok {
		return errors.Errorf("type invalid")
	}

	if err := PutCurrentPos(tableScanner.positionCache, *msg.InputStreamKey, &p, true); err != nil {
		return errors.Trace(err)
	}

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
	positionCache position_store.PositionCacheInterface,
	emitter core.Emitter,
	throttle *time.Ticker,
	schemaStore schema_store.SchemaStore,
	cfg *PluginConfig,
	ctx context.Context) *TableScanner {

	tableScanner := TableScanner{
		pipelineName:  pipelineName,
		tableWorkC:    tableWorkC,
		db:            db,
		positionCache: positionCache,
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

func waitAndCloseStream(tableDef *schema_store.Table, em core.Emitter) error {
	barrierMsg := NewBarrierMsg(tableDef)
	if err := em.Emit(barrierMsg); err != nil {
		return errors.Trace(err)
	}
	<-barrierMsg.Done

	closeMsg := NewCloseInputStreamMsg(tableDef)
	if err := em.Emit(closeMsg); err != nil {
		return errors.Trace(err)
	}
	<-closeMsg.Done
	return nil
}
