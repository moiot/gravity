package mysqlbatch

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/mysql"

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
}

type PluginConfig struct {
	Source      *utils.DBConfig `mapstructure:"source" toml:"source" json:"source"` // keep same with mysql binlog config to make most cases simple
	SourceSlave *utils.DBConfig `mapstructure:"source-slave" toml:"source-slave" json:"source-slave"`

	SourceProbeCfg *helper.SourceProbeCfg `mapstructure:"source-probe-config"json:"source-probe-config"`

	TableConfigs []TableConfig `mapstructure:"table-configs"json:"table-configs"`

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
	ScanColumn        string
	EstimatedRowCount int64
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mysqlbatch", &mysqlBatchInputPlugin{}, false)
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

	tableDefs, tableConfigs := GetTables(scanDB, sourceSchemaStore, cfg.TableConfigs, router)
	if err != nil {
		return errors.Trace(err)
	}

	// Delete empty table
	tableDefs, tableConfigs = DeleteEmptyTables(scanDB, tableDefs, tableConfigs)

	// Detect any potential error before any work is done, so that we can check the error early.
	scanColumns := make([]string, len(tableDefs))
	estimatedRowCount := make([]int64, len(tableDefs))
	var allErrors []error
	for i, t := range tableDefs {
		column, rowCount, err := DetectScanColumn(plugin.scanDB, t.Schema, t.Name, plugin.cfg.MaxFullDumpCount)
		if err != nil {
			log.Errorf("failed to detect scan column, schema: %v, table: %v", t.Schema, t.Name)
			allErrors = append(allErrors, err)
		}
		scanColumns[i] = column
		estimatedRowCount[i] = rowCount
	}
	if len(allErrors) > 0 {
		return errors.Errorf("failed detect %d tables scan column", len(allErrors))
	}

	tableDefs, tableConfigs, scanColumns, estimatedRowCount, err = InitializePositionAndDeleteScannedTable(scanDB, positionCache, scanColumns, estimatedRowCount, tableDefs, tableConfigs)
	if err != nil {
		return errors.Trace(err)
	}

	tableQueue := make(chan *TableWork, len(tableDefs))
	for i := 0; i < len(tableDefs); i++ {
		work := TableWork{TableDef: tableDefs[i], TableConfig: &tableConfigs[i], ScanColumn: scanColumns[i], EstimatedRowCount: estimatedRowCount[i]}
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

func InitTablePosition(db *sql.DB, positionCache position_store.PositionCacheInterface, tableDef *schema_store.Table, scanColumn string, estimatedRowCount int64) (bool, error) {
	fullTableName := utils.TableIdentity(tableDef.Schema, tableDef.Name)
	max, _, exists, err := GetMaxMin(positionCache, fullTableName)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !exists {
		log.Infof("[InitTablePosition] init table position")

		var scanType string
		if scanColumn == "*" {
			maxPos := TablePosition{Column: scanColumn, Type: PlainInt, Value: 1}
			minPos := TablePosition{Column: scanColumn, Type: PlainInt, Value: 0}
			if err := PutMaxMin(positionCache, fullTableName, maxPos, minPos); err != nil {
				return false, errors.Trace(err)
			}
		} else {
			max, min := FindMaxMinValueFromDB(db, tableDef.Schema, tableDef.Name, scanColumn)
			maxPos := TablePosition{Value: max, Type: scanType, Column: scanColumn}
			minPos := TablePosition{Value: min, Type: scanType, Column: scanColumn}
			if err := PutMaxMin(positionCache, fullTableName, maxPos, minPos); err != nil {
				return false, errors.Trace(err)
			}
			log.Infof("[InitTablePosition] table: %v, PutMaxMin: maxPos: %+v, minPos: %+v", fullTableName, maxPos, minPos)
		}

		if err := PutEstimatedCount(positionCache, fullTableName, estimatedRowCount); err != nil {
			return false, errors.Trace(err)
		}

		log.Infof("[InitTablePosition] schema: %v, table: %v, scanColumn: %v", tableDef.Schema, tableDef.Name, scanColumn)
		return false, nil
	} else {
		current, exists, err := GetCurrentPos(positionCache, fullTableName)
		if err != nil {
			return false, errors.Trace(err)
		}

		if !exists {
			return false, nil
		}

		if mysql.MySQLDataEquals(current.Value, max.Value) {
			return true, nil
		} else {
			return false, nil
		}
	}
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
