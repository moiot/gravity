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

	"github.com/moiot/gravity/pkg/offsets"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/utils"
)

var (
	BinlogCheckInterval = time.Second
)

type tidbKafkaInput struct {
	pipelineName string

	emitter core.Emitter

	gravityServerID uint32

	cfg *config.SourceTiDBConfig

	ctx    context.Context
	cancel context.CancelFunc

	binlogTailer  *BinlogTailer
	positionCache *position_store.PositionCache
	binlogChecker binlog_checker.BinlogChecker
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "tidbkafka", &tidbKafkaInput{}, false)
}

func (plugin *tidbKafkaInput) Configure(pipelineName string, data map[string]interface{}) error {
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

func (plugin *tidbKafkaInput) NewPositionCache() (*position_store.PositionCache, error) {
	positionRepo, err := position_store.NewMySQLRepo(
		plugin.pipelineName,
		plugin.cfg.OffsetStoreConfig.SourceMySQL,
		plugin.cfg.OffsetStoreConfig.Annotation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_store.NewPositionCache(plugin.pipelineName, positionRepo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	plugin.positionCache = positionCache
	return positionCache, nil
}

func (plugin *tidbKafkaInput) Start(emitter core.Emitter) error {
	plugin.emitter = emitter
	plugin.gravityServerID = utils.GenerateRandomServerID()

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
		plugin.gravityServerID,
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

func (plugin *tidbKafkaInput) Stage() config.InputMode {
	return config.Stream
}

func (plugin *tidbKafkaInput) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.binlogTailer.Wait()
		position := plugin.positionCache.Get()
		c <- position
		close(c)
	}()
	return c
}

func (plugin *tidbKafkaInput) SendDeadSignal() error {
	db, err := utils.CreateDBConnection(plugin.cfg.SourceDB)
	if err != nil {
		return errors.Trace(err)
	}
	return mysql_test.SendDeadSignal(db, plugin.Identity())
}

func (plugin *tidbKafkaInput) Wait() {
	plugin.binlogTailer.Wait()
}

func (plugin *tidbKafkaInput) Identity() uint32 {
	return plugin.gravityServerID
}

func (plugin *tidbKafkaInput) Close() {
	log.Infof("[mysql_binlog_server] stop...")

	plugin.binlogTailer.Close()
	log.Infof("[mysql_binlog_server] stopped binlogTailer")

	plugin.binlogChecker.Stop()
	log.Infof("[mysql_binlog_server] stopped binlogChecker")
}

type offsetStoreAdapter struct {
	delegate *OffsetStore
	name     string
	group    string
	topic    []string
}

func (store *offsetStoreAdapter) Start() error {
	return nil
}

func (store *offsetStoreAdapter) Close() {
	err := store.delegate.db.Close()
	if err != nil {
		log.Fatalf("db offset positionCache closes the DB connection with error. %s", err)
	}
	log.Info("db offset positionCache closes the DB connection")
}

func (store *offsetStoreAdapter) Stage() config.InputMode {
	return config.Stream
}

func (store *offsetStoreAdapter) Position() position_store.Position {
	req := &offsets.OffsetFetchRequest{
		ConsumerGroup: store.group,
	}
	resp, err := store.delegate.FetchOffset(req)
	if err != nil {
		log.Fatalf("[offsetStoreAdapter.Position] %s", errors.Trace(err))
	}

	return position_store.Position{
		Name:       store.name,
		Stage:      store.Stage(),
		Raw:        *resp,
		UpdateTime: resp.LastUpdate,
	}
}

func (store *offsetStoreAdapter) Update(pos position_store.Position) {
	log.Fatal("[offsetStoreAdapter.Update] unimplemented") //TODO
}

func (store *offsetStoreAdapter) Clear() {
	store.delegate.Clear(store.group, store.topic)
}
