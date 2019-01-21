package mysqlbatch

import (
	"context"
	"database/sql"
	"regexp"
	"sync"
	"time"

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
	// Table is an array of string, each string is a regular expression
	// that describes the table name
	Table     []string `mapstructure:"table" toml:"table" json:"table"`
	tableExps []*regexp.Regexp
}

type PluginConfig struct {
	Source      *utils.DBConfig `mapstructure:"source" toml:"source" json:"source"` // keep same with mysql binlog config to make most cases simple
	SourceSlave *utils.DBConfig `mapstructure:"source-slave" toml:"source-slave" json:"source-slave"`

	TableConfigs []TableConfig `mapstructure:"table-configs"json:"table-configs"`

	NrScanner        int `mapstructure:"nr-scanner" toml:"nr-scanner" json:"nr-scanner"`
	TableScanBatch   int `mapstructure:"table-scan-batch" toml:"table-scan-batch" json:"table-scan-batch"`
	MaxFullDumpCount int `mapstructure:"max-full-dump-count"  toml:"max-full-dump-count"  json:"max-full-dump-count"`

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

	// make sure table is a valid regular expression
	for i, tableConfig := range cfg.TableConfigs {
		cfg.TableConfigs[i].tableExps = make([]*regexp.Regexp, len(tableConfig.Table))
		for j, t := range tableConfig.Table {
			exp, err := regexp.Compile(t)
			if err != nil {
				return errors.Annotatef(err, "%s is not a valid regular expression", t)
			}
			cfg.TableConfigs[i].tableExps[j] = exp
		}
	}

	return nil
}

type mysqlFullInput struct {
	pipelineName string
	cfg          *PluginConfig

	sourceDB *sql.DB
	scanDB   *sql.DB

	sourceSchemaStore schema_store.SchemaStore

	ctx    context.Context
	cancel context.CancelFunc

	throttle *time.Ticker

	positionStore position_store.PositionStore

	tableScanners []*TableScanner

	startBinlogPos utils.MySQLBinlogPosition
	doneC          chan position_store.Position

	closeOnce sync.Once
}

type TableWork struct {
	TableDef    *schema_store.Table
	TableConfig *TableConfig
	ScanColumn  string
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mysqlbatch", &mysqlFullInput{}, false)
}

func (plugin *mysqlFullInput) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.WeakDecode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if err := cfg.ValidateAndSetDefault(); err != nil {
		return errors.Trace(err)
	}

	plugin.cfg = &cfg
	return nil
}

func (plugin *mysqlFullInput) Start(emitter core.Emitter) error {
	cfg := plugin.cfg

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

	tableDefs, tableConfigs := GetTables(sourceDB, sourceSchemaStore, cfg.TableConfigs)
	if err != nil {
		return errors.Trace(err)
	}

	ps := plugin.positionStore.(position_store.MySQLTablePositionStore)
	pos, ok := ps.GetStartBinlogPos()
	if !ok {
		log.Infof("[Server] init master binlog position")

		dbUtil := utils.NewMySQLDB(plugin.sourceDB)
		binlogFilePos, gtid, err := dbUtil.GetMasterStatus()
		if err != nil {
			return errors.Annotatef(err, "failed to show master status")
		}

		p := position_store.SerializeMySQLBinlogPosition(binlogFilePos, gtid)
		ps.PutStartBinlogPos(p)
		plugin.startBinlogPos = p

	} else {
		plugin.startBinlogPos = pos
	}

	// Detect any potential error before any work is done, so that we can check the error early.
	scanColumns := make([]string, len(tableDefs))
	for i, t := range tableDefs {
		column, err := DetectScanColumn(plugin.scanDB, t.Schema, t.Name, plugin.cfg.MaxFullDumpCount)
		if err != nil {
			return errors.Annotatef(err, "schema: %v, table: %v failed to detect scan column", t.Schema, t.Name)
		}
		scanColumns[i] = column
	}

	tableQueue := make(chan *TableWork, len(tableDefs))
	for i := 0; i < len(tableDefs); i++ {
		work := TableWork{TableDef: tableDefs[i], TableConfig: &tableConfigs[i], ScanColumn: scanColumns[i]}
		tableQueue <- &work
	}
	close(tableQueue)

	for i := 0; i < cfg.NrScanner; i++ {
		tableScanner := NewTableScanner(
			plugin.pipelineName,
			tableQueue,
			scanDB,
			ps,
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

	go plugin.waitFinish()

	return nil
}

func (plugin *mysqlFullInput) Identity() uint32 {
	return 0
}

func (plugin *mysqlFullInput) Stage() config.InputMode {
	return config.Batch
}

func (plugin *mysqlFullInput) NewPositionStore() (position_store.PositionStore, error) {

	positionStore, err := position_store.NewMySQLTableDBPositionStore(
		plugin.pipelineName,
		plugin.cfg.Source,
		"",
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	plugin.positionStore = positionStore
	return positionStore, nil
}

func (plugin *mysqlFullInput) PositionStore() position_store.PositionStore {
	return plugin.positionStore
}

func (plugin *mysqlFullInput) SendDeadSignal() error {
	plugin.Close()
	return nil
}

func (plugin *mysqlFullInput) Done() chan position_store.Position {
	return plugin.doneC
}

func (plugin *mysqlFullInput) Wait() {
	for i := range plugin.tableScanners {
		plugin.tableScanners[i].Wait()
	}
}

func (plugin *mysqlFullInput) Close() {

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

		log.Infof("[mysqlFullInput] closed")
	})

}

func (plugin *mysqlFullInput) waitFinish() {
	for idx := range plugin.tableScanners {
		plugin.tableScanners[idx].Wait()
	}

	if plugin.ctx.Err() == nil {
		log.Infof("[plugin.waitFinish] table scanners done")
		position := position_store.PipelineGravityMySQLPosition{
			CurrentPosition: &utils.MySQLBinlogPosition{},
			StartPosition:   &utils.MySQLBinlogPosition{},
		}
		*position.StartPosition = plugin.startBinlogPos
		*position.CurrentPosition = plugin.startBinlogPos
		plugin.doneC <- position_store.Position{
			Name:       plugin.pipelineName,
			Stage:      config.Stream,
			Raw:        position,
			UpdateTime: time.Now(),
		}
		close(plugin.doneC)
	} else if plugin.ctx.Err() == context.Canceled {
		log.Infof("[plugin.waitFinish] table scanner cancelled")
		close(plugin.doneC)
	} else {
		log.Fatalf("[plugin.waitFinish] unknown case: ctx.Err(): %v", plugin.ctx.Err())
	}
}
