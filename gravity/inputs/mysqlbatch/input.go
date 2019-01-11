package mysqlbatch

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/core"

	position_store2 "github.com/moiot/gravity/gravity/inputs/position_store"
	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/gravity/registry"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/position_store"

	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"
)

type TableConfig struct {
	Schema string   `mapstructure:"schema" toml:"schema" json:"schema"`
	Table  []string `mapstructure:"table" toml:"table" json:"table"`
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

	positionStore position_store2.PositionStore

	tableScanners []*TableScanner

	startBinlogPos utils.MySQLBinlogPosition
	doneC          chan position_store2.Position

	closeOnce sync.Once
}

type TableWork struct {
	TableDef    *schema_store.Table
	TableConfig *TableConfig
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

	tableQueue := make(chan *TableWork, len(tableDefs))
	for i := 0; i < len(tableDefs); i++ {
		work := TableWork{TableDef: tableDefs[i], TableConfig: &tableConfigs[i]}
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

	plugin.doneC = make(chan position_store2.Position)

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

func (plugin *mysqlFullInput) Stage() stages.InputStage {
	return stages.InputStageFull
}

func (plugin *mysqlFullInput) NewPositionStore() (position_store2.PositionStore, error) {

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

func (plugin *mysqlFullInput) PositionStore() position_store2.PositionStore {
	return plugin.positionStore
}

func (plugin *mysqlFullInput) SendDeadSignal() error {
	plugin.Close()
	return nil
}

func (plugin *mysqlFullInput) Done() chan position_store2.Position {
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
		plugin.doneC <- position_store2.Position{
			Name:       plugin.pipelineName,
			Stage:      stages.InputStageIncremental,
			Raw:        position,
			UpdateTime: time.Now(),
		}
	} else if plugin.ctx.Err() == context.Canceled {
		log.Infof("[plugin.waitFinish] table scanner cancelled")
		close(plugin.doneC)
	} else {
		log.Fatalf("[plugin.waitFinish] unknown case: ctx.Err(): %v", plugin.ctx.Err())
	}
}
