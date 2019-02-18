package tidb_kafka

import (
	"context"
	"time"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/position_store"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/registry"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/mysql_test"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/utils"
)

var (
	BinlogCheckInterval = time.Second
)

type tidbKafkaStreamInputPlugin struct {
	pipelineName string

	emitter core.Emitter

	gravityServerID uint32

	cfg *config.SourceTiDBConfig

	ctx           context.Context
	cancel        context.CancelFunc
	positionCache position_store.PositionCacheInterface
	binlogTailer  *BinlogTailer
	binlogChecker binlog_checker.BinlogChecker
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "tidbkafka", &tidbKafkaStreamInputPlugin{}, false)
}

func (plugin *tidbKafkaStreamInputPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := config.SourceTiDBConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.SourceDB == nil {
		return errors.Errorf("source-db must be configured")
	}

	if cfg.SourceKafka == nil {
		return errors.Errorf("source-kafka must be configured")
	}

	if cfg.OffsetStoreConfig == nil {
		return errors.Errorf("offset-positionCache must be configured")
	}

	plugin.cfg = &cfg

	return nil
}

func (plugin *tidbKafkaStreamInputPlugin) NewPositionCache() (position_store.PositionCacheInterface, error) {
	positionRepo, err := position_store.NewMySQLRepo(
		plugin.cfg.OffsetStoreConfig.SourceMySQL,
		plugin.cfg.OffsetStoreConfig.Annotation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_store.NewPositionCache(
		plugin.pipelineName,
		positionRepo,
		KafkaPositionValueEncoder,
		KafkaPositionValueDecoder,
		position_store.DefaultFlushPeriod)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return positionCache, nil
}

func (plugin *tidbKafkaStreamInputPlugin) Start(emitter core.Emitter, positionCache position_store.PositionCacheInterface) error {
	plugin.emitter = emitter
	plugin.gravityServerID = utils.GenerateRandomServerID()
	plugin.positionCache = positionCache
	plugin.ctx, plugin.cancel = context.WithCancel(context.Background())

	cfg := plugin.cfg

	binlogChecker, err := binlog_checker.NewBinlogChecker(
		plugin.pipelineName,
		cfg.SourceDB,
		"",
		5*time.Second,
		false,
	)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.binlogChecker = binlogChecker

	binlogTailer, err := NewBinlogTailer(
		plugin.pipelineName,
		plugin.gravityServerID,
		positionCache,
		cfg,
		emitter,
		binlogChecker,
	)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.binlogTailer = binlogTailer

	if err := plugin.binlogTailer.Start(); err != nil {
		return errors.Trace(err)
	}

	if err := plugin.binlogChecker.Start(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (plugin *tidbKafkaStreamInputPlugin) Stage() config.InputMode {
	return config.Stream
}

func (plugin *tidbKafkaStreamInputPlugin) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.binlogTailer.Wait()
		position, exist, err := plugin.positionCache.Get()
		if err != nil && exist {
			c <- position
		} else {
			log.Fatalf("[tidbKafkaStreamInputPlugin] failed to get position, exist: %v, err: %v", exist, errors.ErrorStack(err))
		}
		close(c)
	}()
	return c
}

func (plugin *tidbKafkaStreamInputPlugin) SendDeadSignal() error {
	db, err := utils.CreateDBConnection(plugin.cfg.SourceDB)
	if err != nil {
		return errors.Trace(err)
	}
	return mysql_test.SendDeadSignal(db, plugin.pipelineName)
}

func (plugin *tidbKafkaStreamInputPlugin) Wait() {
	plugin.binlogTailer.Wait()
}

func (plugin *tidbKafkaStreamInputPlugin) Identity() uint32 {
	return plugin.gravityServerID
}

func (plugin *tidbKafkaStreamInputPlugin) Close() {
	log.Infof("[mysql_binlog_server] stop...")

	plugin.binlogTailer.Close()
	log.Infof("[mysql_binlog_server] stopped binlogTailer")

	plugin.binlogChecker.Stop()
	log.Infof("[mysql_binlog_server] stopped binlogChecker")
}
