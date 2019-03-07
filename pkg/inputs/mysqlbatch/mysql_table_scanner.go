package mysqlbatch

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/moiot/gravity/pkg/metrics"

	"github.com/juju/errors"
	"github.com/pingcap/parser"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
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
				fullTableName := utils.TableIdentity(work.TableDef.Schema, work.TableDef.Name)

				err := tableScanner.initTableDDL(work.TableDef)
				if err != nil {
					log.Fatalf("[TableScanner] initTableDDL for %s.%s, err: %s", work.TableDef.Schema, work.TableDef.Name, errors.ErrorStack(err))
				}

				max, min, exists, err := GetMaxMin(tableScanner.positionCache, fullTableName)
				if err != nil {
					log.Fatalf("[TableScanner] GetMaxMin failed: %v", errors.ErrorStack(err))
				}

				if !exists {
					log.Fatalf("[table_scanner] GetMaxMin not exists table: %v", fullTableName)
				}

				scanColumns := work.ScanColumns

				// If no scan column defined, we do a full dump of the table
				if IsScanColumnsForDump(scanColumns) {
					tableScanner.FindAll(tableScanner.db, work.TableDef, work.TableConfig)
				} else {
					tableScanner.LoopInBatch(
						tableScanner.db, work.TableDef,
						work.TableConfig, scanColumns,
						max,
						min,
						tableScanner.cfg.TableScanBatch)

					if tableScanner.ctx.Err() == nil {
						log.Infof("[table_worker] LoopInBatch done with table %s", fullTableName)
					} else if tableScanner.ctx.Err() == context.Canceled {
						log.Infof("[TableScanner] LoopInBatch canceled")
						return
					} else {
						log.Fatalf("[TableScanner] LoopInBatch unknown case,err: %v", tableScanner.ctx.Err())
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

func (tableScanner *TableScanner) Wait() {
	tableScanner.wg.Wait()
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
func (tableScanner *TableScanner) LoopInBatch(
	db *sql.DB,
	tableDef *schema_store.Table,
	tableConfig *TableConfig,
	scanColumns []string,
	max []TablePosition,
	min []TablePosition,
	batch int) {
	pipelineName := tableScanner.pipelineName

	if batch <= 0 {
		log.Fatalf("[LoopInBatch] batch size is 0")
	}

	batchIdx := 0
	firstLoop := true

	fullTableName := utils.TableIdentity(tableDef.Schema, tableDef.Name)

	currentPositions, done, exists, err := GetCurrentPos(tableScanner.positionCache, fullTableName)
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get current pos: %v", errors.ErrorStack(err))
	}

	if !exists {
		currentPositions = min
	} else {
		if done {
			log.Infof("[LoopInBatch] already scanned: %v", fullTableName)
			return
		}
	}

	log.Infof("[LoopInBatch] prepare current: %+v", currentPositions)

	currentMinValues := make([]interface{}, len(currentPositions))
	maxValues := make([]interface{}, len(currentPositions))
	for i, p := range currentPositions {
		currentMinValues[i] = p.Value
		maxValues[i] = max[i].Value
	}
	resultCount := 0

	columnTypes, err := GetTableColumnTypes(db, tableDef.Schema, tableDef.Name)
	if err != nil {
		log.Fatalf("[LoopInBatch] failed to get columnType, err: %v", errors.ErrorStack(err))
	}

	scanIndexes := make([]int, len(scanColumns))
	for i, c := range scanColumns {
		scanIndexes[i], err = GetScanIdx(columnTypes, c)
		if err != nil {
			log.Fatalf("[LoopInBatch] failed to get scanIdx, err: %v", errors.ErrorStack(err))
		}
	}

	rowsBatchDataPtrs := newBatchDataPtrs(columnTypes, batch)

	for {

		statement, args := GenerateScanQueryAndArgs(firstLoop, fullTableName, scanColumns, currentMinValues, maxValues, batch)
		firstLoop = false

		<-tableScanner.throttle.C

		queryStartTime := time.Now()
		rows, err := db.Query(statement, args...)
		if err != nil {
			log.Fatalf("[LoopInBatch] table %s.%s, err: %v", tableDef.Schema, tableDef.Name, err)
		}

		rowIdx := 0
		for rows.Next() {

			JobFetchedCount.WithLabelValues(pipelineName).Add(1)

			resultCount++

			rowsBatchDataPtrs[rowIdx], err = utils.ScanGeneralRowsWithDataPtrs(rows, columnTypes, rowsBatchDataPtrs[rowIdx])
			if err != nil {
				log.Fatalf("[LoopInBatch] table %s.%s, scan error: %v", tableDef.Schema, tableDef.Name, errors.ErrorStack(err))
			}

			for i := range currentMinValues {
				currentMinValues[i] = reflect.ValueOf(rowsBatchDataPtrs[rowIdx][scanIndexes[i]]).Elem().Interface()
			}
			rowIdx++
		}

		err = rows.Err()
		if err != nil {
			log.Fatalf("[LoopInBatch] table %s.%s, rows err: %v", tableDef.Schema, tableDef.Name, err)
		}

		rows.Close()

		// no result found for this query
		if rowIdx == 0 {
			log.Infof("[TableScanner] query result is 0, return")
			if err := tableScanner.finishBatchScan(tableDef); err != nil {
				log.Fatalf("[LoopInBatch] failed finish batch scan: %v", errors.ErrorStack(err))
			}
			return
		}

		BatchQueryDuration.WithLabelValues(pipelineName).Observe(time.Now().Sub(queryStartTime).Seconds())

		batchIdx++

		// process this batch's data
		for i := 0; i < rowIdx; i++ {
			rowPtrs := rowsBatchDataPtrs[i]

			positions := make([]TablePosition, len(scanColumns))
			for columnScanIdx := range scanColumns {
				positions[columnScanIdx] = TablePosition{
					Value:  mysql.NormalizeSQLType(reflect.ValueOf(rowPtrs[scanIndexes[columnScanIdx]]).Elem().Interface()),
					Column: scanColumns[columnScanIdx]}
			}

			msg := NewMsg(rowPtrs, columnTypes, tableDef, tableScanner.AfterMsgCommit, positions, queryStartTime)

			if err := tableScanner.emitter.Emit(msg); err != nil {
				log.Fatalf("[LoopInBatch] failed to emit job: %v", errors.ErrorStack(err))
			}
		}

		log.Infof("[LoopInBatch] sourceDB: %s, table: %s, currentMinValues: %+v, maxValues: %+v, resultCount: %v",
			tableDef.Schema, tableDef.Name, currentMinValues, maxValues, resultCount)

		select {
		case <-tableScanner.ctx.Done():
			log.Infof("[table_worker] canceled by context")
			return
		default:
			continue
		}
	}
}

func (tableScanner *TableScanner) finishBatchScan(tableDef *schema_store.Table) error {
	if err := waitAndCloseStream(tableDef, tableScanner.emitter); err != nil {
		return errors.Trace(err)
	}

	if err := PutDone(tableScanner.positionCache, utils.TableIdentity(tableDef.Schema, tableDef.Name)); err != nil {
		return errors.Trace(err)
	}

	if err := tableScanner.positionCache.Flush(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (tableScanner *TableScanner) FindAll(db *sql.DB, tableDef *schema_store.Table, tableConfig *TableConfig) {
	streamKey := utils.TableIdentity(tableDef.Schema, tableDef.Name)

	// do not re-scan table that has already dumped
	_, done, _, err := GetCurrentPos(tableScanner.positionCache, streamKey)
	if err != nil {
		log.Fatalf("[FindAll] failed to get current pos: %v", errors.ErrorStack(err))
	}
	if done {
		log.Infof("[FindAll] table: %v already dumped", streamKey)
		return
	}

	log.Infof("[tableScanner] dump table: %s", streamKey)

	columnTypes, err := GetTableColumnTypes(db, tableDef.Schema, tableDef.Name)
	if err != nil {
		log.Fatalf("[FindAll] failed to get columnType: %v", errors.ErrorStack(err))
	}

	statement := fmt.Sprintf("SELECT * FROM `%s`.`%s`", tableDef.Schema, tableDef.Name)

	queryStartTime := time.Now()
	allData, err := utils.QueryGeneralRowsDataWithSQL(db, statement)
	if err != nil {
		log.Fatalf("[FindAll] failed to find all, err: %v", errors.ErrorStack(err))
	}

	for i := range allData {
		rowPtrs := allData[i]
		msg := NewMsg(rowPtrs, columnTypes, tableDef, tableScanner.AfterMsgCommit, []TablePosition{{Column: ScanColumnForDump, Type: PlainInt, Value: i + 1}}, queryStartTime)
		if err := tableScanner.emitter.Emit(msg); err != nil {
			log.Fatalf("[FindAll] failed to emit: %v", errors.ErrorStack(err))
		}
	}

	// set the current and max position to be the same
	p := TablePosition{Column: ScanColumnForDump, Type: PlainInt, Value: len(allData)}

	if err := PutCurrentPos(tableScanner.positionCache, streamKey, []TablePosition{p}, false); err != nil {
		log.Fatalf("[FindAll] failed to put current pos: %v", errors.ErrorStack(err))
	}

	if err := PutMaxMin(
		tableScanner.positionCache,
		utils.TableIdentity(tableDef.Schema, tableDef.Name),
		[]TablePosition{p},
		[]TablePosition{{Column: ScanColumnForDump, Type: PlainInt, Value: 0}}); err != nil {
		log.Fatalf("[FindAll] failed to put max min: %v", errors.ErrorStack(err))
	}

	if err := PutDone(tableScanner.positionCache, streamKey); err != nil {
		log.Fatalf("[FindAll] failed to put done: %v", errors.ErrorStack(err))
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
	positions, ok := msg.InputContext.([]TablePosition)
	if !ok {
		return errors.Errorf("type invalid")
	}

	if err := PutCurrentPos(tableScanner.positionCache, *msg.InputStreamKey, positions, true); err != nil {
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

var (
	BatchQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gravity",
			Subsystem: "output",
			Name:      "batch_query_duration",
			Help:      "bucketed histogram of batch fetch duration time",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{metrics.PipelineTag})

	JobFetchedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gravity",
		Subsystem: "output",
		Name:      "job_fetched_count",
		Help:      "Number of data rows fetched by scanner",
	}, []string{metrics.PipelineTag})
)

func init() {
	prometheus.MustRegister(BatchQueryDuration, JobFetchedCount)
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

func GenerateScanQueryAndArgs(
	firstLoop bool,
	fullTableName string,
	scanColumns []string,
	currentMinValues []interface{},
	maxValues []interface{},
	batch int) (string, []interface{}) {
	prefix := fmt.Sprintf("SELECT * FROM %s WHERE ", fullTableName)

	var where []string
	var args []interface{}

	for i, column := range scanColumns {
		if firstLoop {
			where = append(where, fmt.Sprintf("%s >= ? AND %s <= ?", column, column))
		} else {
			where = append(where, fmt.Sprintf("%s > ? AND %s <= ?", column, column))
		}
		args = append(args, currentMinValues[i], maxValues[i])
	}

	whereString := strings.Join(where, " AND ")
	orderByString := strings.Join(scanColumns, ", ")

	query := fmt.Sprintf("%s%s ORDER BY %s LIMIT ?", prefix, whereString, orderByString)
	args = append(args, batch)
	return query, args
}
