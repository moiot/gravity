package mysqlstream

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/inputs/helper"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
)

var (
	// re-sync retry timeout
	RetryTimeout = 3 * time.Second

	BinlogProbeInterval = 3 * time.Second
)

const inputStreamKey = "mysqlstream"

type MySQLBinlogInputPluginConfig struct {
	Source                  *config.DBConfig            `mapstructure:"source" toml:"source" json:"source"`
	IgnoreBiDirectionalData bool                        `mapstructure:"ignore-bidirectional-data" toml:"ignore-bidirectional-data" json:"ignore-bidirectional-data"`
	StartPosition           *config.MySQLBinlogPosition `mapstructure:"start-position" toml:"start-position" json:"start-position"`

	SourceProbeCfg *helper.SourceProbeCfg `mapstructure:"source-probe-config" json:"source-probe-config"`

	PositionRepo *config.GenericPluginConfig `mapstructure:"position-repo" toml:"position-repo" json:"position-repo"`

	// If we detect any internal txn tag that matches FailOnTxnTag, just fail.
	FailOnTxnTags []string `mapstructure:"fail-on-txn-tags" toml:"fail-on-txn-tags"`

	HeartbeatPeriodStr string        `toml:"heartbeat-period" json:"heartbeat-period" mapstructure:"heartbeat-period"`
	HeartbeatPeriod    time.Duration `toml:"-" json:"-" mapstructure:"-"`

	//
	// internal configurations that is not exposed to users
	//
	DisableBinlogChecker bool   `mapstructure:"-" json:"-"`
	DebugBinlog          bool   `mapstructure:"-" json:"-"`
	BinlogSyncerTimeout  string `mapstructure:"-" json:"-"`
}

type mysqlStreamInputPlugin struct {
	pipelineName string
	cfg          *MySQLBinlogInputPluginConfig

	sourceDB *sql.DB

	probeDBConfig      *config.DBConfig
	probeSQLAnnotation string

	ctx    context.Context
	cancel context.CancelFunc

	positionRepo  position_repos.PositionRepo
	positionCache position_cache.PositionCacheInterface
	binlogChecker binlog_checker.BinlogChecker
	binlogTailer  *BinlogTailer

	sourceSchemaStore schema_store.SchemaStore

	closeOnce sync.Once
}

const Name = "mysql-stream"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &mysqlStreamInputPlugin{}, false)
}

func (plugin *mysqlStreamInputPlugin) Configure(pipelineName string, configInput map[string]interface{}) error {
	plugin.pipelineName = pipelineName
	cfg := MySQLBinlogInputPluginConfig{}
	err := mapstructure.Decode(configInput, &cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// validate configurations
	if cfg.Source == nil {
		return errors.Errorf("[mysqlbinlog] empty master db configured")
	}

	// By default, fail on txn tag start with the same pipelineName prefix.
	if len(cfg.FailOnTxnTags) == 0 {
		cfg.FailOnTxnTags = []string{fmt.Sprintf("%s*", pipelineName)}
	}

	if cfg.HeartbeatPeriodStr != "" {
		cfg.HeartbeatPeriod, err = time.ParseDuration(cfg.HeartbeatPeriodStr)
		if err != nil {
			return errors.Annotatef(err, "invalid HeartbeatPeriodStr %s", cfg.HeartbeatPeriodStr)
		}
	}

	// probe connection settings
	plugin.probeDBConfig, plugin.probeSQLAnnotation = helper.GetProbCfg(cfg.SourceProbeCfg, cfg.Source)

	if cfg.PositionRepo == nil {
		cfg.PositionRepo = position_repos.NewMySQLRepoConfig(plugin.probeSQLAnnotation, plugin.probeDBConfig)
	}
	positionRepo, err := registry.GetPlugin(registry.PositionRepo, cfg.PositionRepo.Type)
	if err != nil {
		return errors.Trace(err)
	}
	if err := positionRepo.Configure(pipelineName, cfg.PositionRepo.Config); err != nil {
		return errors.Trace(err)
	}

	plugin.positionRepo = positionRepo.(position_repos.PositionRepo)
	plugin.cfg = &cfg
	return nil
}

func (plugin *mysqlStreamInputPlugin) NewPositionCache() (position_cache.PositionCacheInterface, error) {
	if err := plugin.positionRepo.Init(); err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_cache.NewPositionCache(
		plugin.pipelineName,
		plugin.positionRepo,
		helper.BinlogPositionValueEncoder,
		helper.BinlogPositionValueDecoder,
		position_cache.DefaultFlushPeriod,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbc := *plugin.cfg.Source
	dbc.MaxIdle = 1
	dbc.MaxOpen = 1
	sourceDB, err := utils.CreateDBConnection(&dbc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer sourceDB.Close()

	if err := SetupInitialPosition(sourceDB, positionCache, plugin.cfg.StartPosition); err != nil {
		return nil, errors.Trace(err)
	}
	return positionCache, nil
}

func (plugin *mysqlStreamInputPlugin) Start(emitter core.Emitter, router core.Router, positionCache position_cache.PositionCacheInterface) error {
	plugin.positionCache = positionCache

	sourceDB, err := utils.CreateDBConnection(plugin.cfg.Source)
	if err != nil {
		log.Fatalf("[gravity] failed to create source connection %v", errors.ErrorStack(err))
	}
	plugin.sourceDB = sourceDB

	sourceSchemaStore, err := schema_store.NewSimpleSchemaStoreFromDBConn(sourceDB)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.sourceSchemaStore = sourceSchemaStore

	// binlog checker
	plugin.binlogChecker, err = binlog_checker.NewBinlogChecker(
		plugin.pipelineName,
		plugin.probeDBConfig,
		plugin.probeSQLAnnotation,
		BinlogProbeInterval,
		plugin.cfg.DisableBinlogChecker)
	if err != nil {
		return errors.Trace(err)
	}
	if err := plugin.binlogChecker.Start(); err != nil {
		return errors.Trace(err)
	}

	// binlog tailer
	gravityServerID := utils.GenerateRandomServerID()
	plugin.ctx, plugin.cancel = context.WithCancel(context.Background())
	plugin.binlogTailer, err = NewBinlogTailer(
		plugin.pipelineName,
		plugin.cfg,
		gravityServerID,
		positionCache,
		sourceSchemaStore,
		sourceDB,
		emitter,
		router,
		plugin.binlogChecker,
		nil)
	if err != nil {
		return errors.Trace(err)
	}

	if err := plugin.binlogTailer.Start(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (plugin *mysqlStreamInputPlugin) Identity() uint32 {
	return plugin.binlogTailer.gravityServerID
}

func (plugin *mysqlStreamInputPlugin) Stage() config.InputMode {
	return config.Stream
}

func (plugin *mysqlStreamInputPlugin) SendDeadSignal() error {
	return mysql_test.SendDeadSignal(plugin.binlogTailer.sourceDB, plugin.pipelineName)
}

func (plugin *mysqlStreamInputPlugin) Wait() {
	plugin.binlogTailer.Wait()
}

func (plugin *mysqlStreamInputPlugin) Done() chan position_repos.Position {
	c := make(chan position_repos.Position)
	go func() {
		plugin.binlogTailer.Wait()
		position, exist, err := plugin.positionCache.Get()
		if err == nil && exist {
			c <- position
		} else {
			log.Fatalf("[mysqlInputPlugin] failed get position exist: %v, err: %v", exist, errors.ErrorStack(err))
		}
		close(c)
	}()
	return c
}

func (plugin *mysqlStreamInputPlugin) Close() {

	log.Infof("[mysqlStreamInputPlugin] closing...")

	if plugin.binlogChecker != nil {
		plugin.binlogChecker.Stop()
	}

	if plugin.binlogTailer != nil {
		plugin.binlogTailer.Close()
	}

	if plugin.sourceSchemaStore != nil {
		plugin.sourceSchemaStore.Close()
	}

	if plugin.sourceDB != nil {
		if err := plugin.sourceDB.Close(); err != nil {
			log.Errorf("[mysqlStreamInputPlugin.Close] error close db. %s", errors.Trace(err))
		}
	}

	log.Infof("[mysqlStreamInputPlugin] closed")
}
