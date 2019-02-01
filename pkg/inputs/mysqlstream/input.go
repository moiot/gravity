package mysqlstream

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_store"
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

type SourceProbeCfg struct {
	SourceMySQL *utils.DBConfig `mapstructure:"mysql"json:"mysql"`
	Annotation  string          `mapstructure:"annotation"json:"annotation"`
}

type MySQLBinlogInputPluginConfig struct {
	Source                  *utils.DBConfig            `mapstructure:"source" toml:"source" json:"source"`
	IgnoreBiDirectionalData bool                       `mapstructure:"ignore-bidirectional-data" toml:"ignore-bidirectional-data" json:"ignore-bidirectional-data"`
	StartPosition           *utils.MySQLBinlogPosition `mapstructure:"start-position" toml:"start-position" json:"start-position"`

	SourceProbeCfg *SourceProbeCfg `mapstructure:"source-probe-config"json:"source-probe-config"`

	//
	// internal configurations that is not exposed to users
	//
	DisableBinlogChecker bool   `mapstructure:"-"json:"-"`
	DebugBinlog          bool   `mapstructure:"-"json:"-"`
	BinlogSyncerTimeout  string `mapstructure:"-"json:"-"`
}

type mysqlStreamInput struct {
	pipelineName string
	cfg          *MySQLBinlogInputPluginConfig

	sourceDB *sql.DB

	probeDBConfig      *utils.DBConfig
	probeSQLAnnotation string

	ctx    context.Context
	cancel context.CancelFunc

	binlogChecker binlog_checker.BinlogChecker
	binlogTailer  *BinlogTailer

	sourceSchemaStore schema_store.SchemaStore
	positionStore     position_store.PositionStore

	closeOnce sync.Once
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mysqlstream", &mysqlStreamInput{}, false)
}

func (plugin *mysqlStreamInput) Configure(pipelineName string, configInput map[string]interface{}) error {
	plugin.pipelineName = pipelineName
	pluginConfig := MySQLBinlogInputPluginConfig{}
	err := mapstructure.Decode(configInput, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	// validate configurations
	if pluginConfig.Source == nil {
		return errors.Errorf("[mysqlbinlog] empty master db configured")
	}

	// probe connection settings
	var probeDBConfig *utils.DBConfig
	var probeSQLAnnotation string
	if pluginConfig.SourceProbeCfg != nil {
		if pluginConfig.SourceProbeCfg.SourceMySQL != nil {
			probeDBConfig = pluginConfig.SourceProbeCfg.SourceMySQL
		} else {
			probeDBConfig = pluginConfig.Source
		}
		probeSQLAnnotation = pluginConfig.SourceProbeCfg.Annotation
	} else {
		probeDBConfig = pluginConfig.Source
	}

	if probeSQLAnnotation != "" {
		probeSQLAnnotation = fmt.Sprintf("/*%s*/", probeSQLAnnotation)
	}

	plugin.probeDBConfig = probeDBConfig
	plugin.probeSQLAnnotation = probeSQLAnnotation

	plugin.cfg = &pluginConfig

	return nil
}

func (plugin *mysqlStreamInput) Stage() config.InputMode {
	return config.Stream
}

func (plugin *mysqlStreamInput) NewPositionStore() (position_store.PositionStore, error) {
	positionStore, err := position_store.NewMySQLBinlogDBPositionStore(
		plugin.pipelineName,
		plugin.probeDBConfig,
		plugin.probeSQLAnnotation,
		plugin.cfg.StartPosition,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	plugin.positionStore = positionStore
	return positionStore, nil
}

func (plugin *mysqlStreamInput) PositionStore() position_store.PositionStore {
	return plugin.positionStore
}

func (plugin *mysqlStreamInput) SendDeadSignal() error {
	return mysql_test.SendDeadSignal(plugin.binlogTailer.sourceDB, plugin.pipelineName)
}

func (plugin *mysqlStreamInput) Wait() {
	plugin.binlogTailer.Wait()
}

func (plugin *mysqlStreamInput) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.binlogTailer.Wait()
		c <- plugin.positionStore.Position()
		close(c)
	}()
	return c
}

func (plugin *mysqlStreamInput) Start(emitter core.Emitter, router core.Router) error {
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
		plugin.positionStore.(position_store.MySQLPositionStore),
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

func (plugin *mysqlStreamInput) Close() {

	plugin.closeOnce.Do(func() {
		log.Infof("[mysqlStreamInput] closing...")

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
				log.Errorf("[mysqlStreamInput.Close] error close db. %s", errors.Trace(err))
			}
		}

		log.Infof("[mysqlStreamInput] closed")
	})
}
