package mysqlbatch

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/inputs/helper"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
)

type TableConfig struct {
	Schema string `mapstructure:"schema" toml:"schema" json:"schema"`
	// Table is an array of string, each string is a glob expression
	// that describes the table name
	Table []string `mapstructure:"table" toml:"table" json:"table"`

	// ScanColumn is an array of string, that enforces these table's scan columns
	ScanColumn []string `mapstructure:"scan-column" toml:"scan-column" json:"scan-column"`
}

type PluginConfig struct {
	Source      *utils.DBConfig `mapstructure:"source" toml:"source" json:"source"` // keep same with mysql binlog config to make most cases simple
	SourceSlave *utils.DBConfig `mapstructure:"source-slave" toml:"source-slave" json:"source-slave"`

	SourceProbeCfg *helper.SourceProbeCfg `mapstructure:"source-probe-config"json:"source-probe-config"`

	TableConfigs []TableConfig `mapstructure:"table-configs" json:"table-configs"`

	IgnoreTables []TableConfig `mapstructure:"ignore-tables" json:"ignore-tables"`

	NrScanner        int   `mapstructure:"nr-scanner" toml:"nr-scanner" json:"nr-scanner"`
	TableScanBatch   int   `mapstructure:"table-scan-batch" toml:"table-scan-batch" json:"table-scan-batch"`
	MaxFullDumpCount int64 `mapstructure:"max-full-dump-count"  toml:"max-full-dump-count"  json:"max-full-dump-count"`

	BatchPerSecondLimit int `mapstructure:"batch-per-second-limit" toml:"batch-per-second-limit" json:"batch-per-second-limit"`
}

func (cfg *PluginConfig) ValidateAndSetDefault() error {
	if cfg.Source == nil {
		return errors.Errorf("[mysqlscanner] source must be configured")
	}

	if cfg.NrScanner <= 0 {
		cfg.NrScanner = 10
	}

	if cfg.TableScanBatch <= 0 {
		cfg.TableScanBatch = 10000
	}

	if cfg.BatchPerSecondLimit <= 0 {
		cfg.BatchPerSecondLimit = 1
	}

	if cfg.MaxFullDumpCount <= 0 {
		cfg.MaxFullDumpCount = 100000
	}

	return nil
}

type mysqlBatchInputPlugin struct {
	pipelineName string
	cfg          *PluginConfig

	sourceDB *sql.DB
	scanDB   *sql.DB

	probeDBConfig      *utils.DBConfig
	probeSQLAnnotation string

	sourceSchemaStore schema_store.SchemaStore

	ctx    context.Context
	cancel context.CancelFunc

	throttle *time.Ticker

	positionCache position_store.PositionCacheInterface

	tableScanners []*TableScanner

	// startBinlogPos utils.MySQLBinlogPosition
	doneC chan position_store.Position

	closeOnce sync.Once
}

type TableWork struct {
	TableDef          *schema_store.Table
	TableConfig       *TableConfig
	ScanColumns       []string
	EstimatedRowCount int64
}

const Name = "mysql-batch"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &mysqlBatchInputPlugin{}, false)
}

func (plugin *mysqlBatchInputPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.WeakDecode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if err := cfg.ValidateAndSetDefault(); err != nil {
		return errors.Trace(err)
	}

	plugin.probeDBConfig, plugin.probeSQLAnnotation = helper.GetProbCfg(cfg.SourceProbeCfg, cfg.Source)

	plugin.cfg = &cfg
	return nil
}

func (plugin *mysqlBatchInputPlugin) NewPositionCache() (position_store.PositionCacheInterface, error) {

	positionRepo, err := position_store.NewMySQLRepo(
		plugin.probeDBConfig,
		plugin.probeSQLAnnotation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_store.NewPositionCache(
		plugin.pipelineName,
		positionRepo,
		EncodeBatchPositionValue,
		DecodeBatchPositionValue,
		position_store.DefaultFlushPeriod)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sourceDB, err := utils.CreateDBConnection(plugin.cfg.Source)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer sourceDB.Close()

	if err := SetupInitialPosition(positionCache, sourceDB); err != nil {
		return nil, errors.Trace(err)
	}

	return positionCache, nil
}

func (plugin *mysqlBatchInputPlugin) Start(emitter core.Emitter, router core.Router, positionCache position_store.PositionCacheInterface) error {
	cfg := plugin.cfg
	plugin.positionCache = positionCache

	sourceDB, err := utils.CreateDBConnection(cfg.Source)
	if err != nil {
		return errors.Annotatef(err, "[NewMysqlFullInput] failed to create source connection ")
	}
	plugin.sourceDB = sourceDB

	scanDB := sourceDB
	if cfg.SourceSlave != nil {
		scanDB, err = utils.CreateDBConnection(cfg.SourceSlave)
		if err != nil {
			return errors.Trace(err)
		}
	}
	plugin.scanDB = scanDB

	sourceSchemaStore, err := schema_store.NewSimpleSchemaStoreFromDBConn(sourceDB)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.sourceSchemaStore = sourceSchemaStore

	rate := time.Second / time.Duration(cfg.BatchPerSecondLimit)
	throttle := time.NewTicker(rate)
	plugin.throttle = throttle

	ctx, cancelFunc := context.WithCancel(context.Background())
	plugin.ctx = ctx
	plugin.cancel = cancelFunc

	tableDefs, tableConfigs := GetTables(scanDB, sourceSchemaStore, cfg.IgnoreTables, cfg.TableConfigs, router)
	if err != nil {
		return errors.Trace(err)
	}

	// Delete empty table
	tableDefs, tableConfigs = DeleteEmptyTables(scanDB, tableDefs, tableConfigs)

	// Detect any potential error before any work is done, so that we can check the error early.
	scanColumnsArray := make([][]string, len(tableDefs))
	estimatedRowCount := make([]int64, len(tableDefs))
	var allErrors []error
	for i, t := range tableDefs {
		rowCount, err := utils.EstimateRowsCount(plugin.scanDB, t.Schema, t.Name)
		if err != nil {
			return errors.Trace(err)
		}

		if len(tableConfigs[i].ScanColumn) == 0 {
			columns, err := DetectScanColumns(plugin.scanDB, t.Schema, t.Name, rowCount, plugin.cfg.MaxFullDumpCount)
			if err != nil {
				log.Errorf("failed to detect scan columns, schema: %v, table: %v", t.Schema, t.Name)
				allErrors = append(allErrors, err)
			}
			scanColumnsArray[i] = columns
		} else {
			scanColumnsArray[i] = tableConfigs[i].ScanColumn
		}

		estimatedRowCount[i] = rowCount
	}
	if len(allErrors) > 0 {
		return errors.Errorf("failed detect %d tables scan column, consider define ignore-tables to ignore", len(allErrors))
	}

	tableDefs, tableConfigs, scanColumnsArray, estimatedRowCount, err = InitializePositionAndDeleteScannedTable(
		scanDB,
		positionCache,
		scanColumnsArray,
		estimatedRowCount,
		tableDefs,
		tableConfigs)
	if err != nil {
		return errors.Trace(err)
	}

	tableQueue := make(chan *TableWork, len(tableDefs))
	for i := 0; i < len(tableDefs); i++ {
		work := TableWork{
			TableDef:          tableDefs[i],
			TableConfig:       &tableConfigs[i],
			ScanColumns:       scanColumnsArray[i],
			EstimatedRowCount: estimatedRowCount[i]}
		tableQueue <- &work
	}
	close(tableQueue)

	for i := 0; i < cfg.NrScanner; i++ {
		tableScanner := NewTableScanner(
			plugin.pipelineName,
			tableQueue,
			scanDB,
			positionCache,
			emitter,
			plugin.throttle,
			sourceSchemaStore,
			cfg,
			plugin.ctx,
		)
		plugin.tableScanners = append(plugin.tableScanners, tableScanner)
	}

	plugin.doneC = make(chan position_store.Position)

	for _, tableScanner := range plugin.tableScanners {
		if err := tableScanner.Start(); err != nil {
			return errors.Trace(err)
		}
	}

	go plugin.waitFinish(positionCache)

	return nil
}

func (plugin *mysqlBatchInputPlugin) Stage() config.InputMode {
	return config.Batch
}

func (plugin *mysqlBatchInputPlugin) SendDeadSignal() error {
	plugin.Close()
	return nil
}

func (plugin *mysqlBatchInputPlugin) Done() chan position_store.Position {
	return plugin.doneC
}

func (plugin *mysqlBatchInputPlugin) Wait() {
	for i := range plugin.tableScanners {
		plugin.tableScanners[i].Wait()
	}
}

func (plugin *mysqlBatchInputPlugin) Close() {

	plugin.closeOnce.Do(func() {
		log.Infof("[scanner server] closing...")

		if plugin.cancel != nil {
			plugin.cancel()
		}

		for i := range plugin.tableScanners {
			plugin.tableScanners[i].Wait()
		}

		if plugin.sourceDB != nil {
			plugin.sourceDB.Close()
		}

		if plugin.scanDB != nil {
			plugin.scanDB.Close()
		}

		if plugin.sourceSchemaStore != nil {
			plugin.sourceSchemaStore.Close()
		}

		log.Infof("[mysqlBatchInputPlugin] closed")
	})

}

func (plugin *mysqlBatchInputPlugin) waitFinish(positionCache position_store.PositionCacheInterface) {
	for idx := range plugin.tableScanners {
		plugin.tableScanners[idx].Wait()
	}

	if plugin.ctx.Err() == nil {
		// It's better to flush the streamPosition here, so that the table streamPosition
		// state is persisted as soon as the table scan finishes.
		if err := positionCache.Flush(); err != nil {
			log.Fatalf("[mysqlBatchInputPlugin] failed to flush streamPosition cache")
		}

		log.Infof("[plugin.waitFinish] table scanners done")
		startBinlog, err := GetStartBinlog(positionCache)
		if err != nil {
			log.Fatalf("[mysqlBatchInputPlugin] failed to get start streamPosition: %v", errors.ErrorStack(err))
		}

		currentPosition := startBinlog
		binlogPositionsValue := helper.BinlogPositionsValue{
			StartPosition:   &startBinlog,
			CurrentPosition: &currentPosition,
		}

		// Notice that we should not change streamPosition stage in this plugin.
		// Changing the stage is done by two stage plugin.
		streamPosition := position_store.Position{
			PositionMeta: position_store.PositionMeta{
				Name:       plugin.pipelineName,
				Stage:      config.Stream,
				UpdateTime: time.Now(),
			},

			Value: binlogPositionsValue,
		}

		plugin.doneC <- streamPosition

		close(plugin.doneC)
	} else if plugin.ctx.Err() == context.Canceled {
		log.Infof("[plugin.waitFinish] table scanner cancelled")
		close(plugin.doneC)
	} else {
		log.Fatalf("[plugin.waitFinish] unknown case: ctx.Err(): %v", plugin.ctx.Err())
	}
}

func InitTablePosition(
	db *sql.DB,
	positionCache position_store.PositionCacheInterface,
	tableDef *schema_store.Table,
	scanColumns []string,
	estimatedRowCount int64) (bool, error) {

	fullTableName := utils.TableIdentity(tableDef.Schema, tableDef.Name)
	_, _, exists, err := GetMaxMin(positionCache, fullTableName)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !exists {
		maxPositions := make([]TablePosition, len(scanColumns))
		minPositions := make([]TablePosition, len(scanColumns))
		if IsScanColumnsForDump(scanColumns) {
			maxPositions[0] = TablePosition{Column: ScanColumnForDump, Type: PlainInt, Value: 1}
			minPositions[0] = TablePosition{Column: ScanColumnForDump, Type: PlainInt, Value: 0}
			if err := PutMaxMin(positionCache, fullTableName, maxPositions, minPositions); err != nil {
				return false, errors.Trace(err)
			}
			log.Infof("[InitTablePosition] scan dump PutMaxMin table: %v, maxPos: %+v, minPos: %+v", fullTableName, maxPositions, minPositions)
		} else {
			retMax, retMin := FindMaxMinValueFromDB(db, tableDef.Schema, tableDef.Name, scanColumns)
			for i, column := range scanColumns {
				maxPositions[i] = TablePosition{Value: retMax[i], Column: column}
				minPositions[i] = TablePosition{Value: retMin[i], Column: column}
			}

			if err := PutMaxMin(positionCache, fullTableName, maxPositions, minPositions); err != nil {
				return false, errors.Trace(err)
			}
			log.Infof("[InitTablePosition] scan key PutMaxMin table: %v, maxPos: %+v, minPos: %+v", fullTableName, maxPositions, minPositions)
		}

		if err := PutEstimatedCount(positionCache, fullTableName, estimatedRowCount); err != nil {
			return false, errors.Trace(err)
		}

		log.Infof("[InitTablePosition] table: %v, scanColumns: %+v", fullTableName, scanColumns)
		return false, nil
	} else {
		_, done, exists, err := GetCurrentPos(positionCache, fullTableName)
		if err != nil {
			return false, errors.Trace(err)
		}

		if !exists {
			return false, nil
		}
		return done, nil
	}
}

//
// DetectScanColumns find columns that we used to scan the table
// First, we try primary keys, then we try unique key; we try dump the table at last.
// Note that composite unique key is not supported.
//
func DetectScanColumns(sourceDB *sql.DB, dbName string, tableName string, estimatedRowsCount int64, maxFullDumpRowsCountLimit int64) ([]string, error) {
	pks, err := utils.GetPrimaryKeys(sourceDB, dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(pks) > 0 {
		return pks, nil
	}

	uniqueIndexes, err := utils.GetUniqueIndexesWithoutPks(sourceDB, dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// We don't support composite unique index right now.
	// When composite unique key exists, the column can still be NULL.
	if len(uniqueIndexes) == 1 {
		return uniqueIndexes, nil
	}

	// Now there is no unique key detected, we end up trying a full dump.
	// So we do a validation here to ensure we are not doing a dump on a really big table.
	if estimatedRowsCount < maxFullDumpRowsCountLimit {
		return []string{ScanColumnForDump}, nil
	}

	return nil, errors.Errorf("no scan column can be found automatically for %s.%s", dbName, tableName)
}

func IsScanColumnsForDump(scanColumns []string) bool {
	return len(scanColumns) > 0 && scanColumns[0] == ScanColumnForDump
}
