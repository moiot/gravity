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

type tidbInput struct {
	pipelineName string

	emitter core.Emitter

	gravityServerID uint32

	cfg *config.SourceTiDBConfig

	ctx    context.Context
	cancel context.CancelFunc

	binlogTailer  *BinlogTailer
	store         position_store.PositionStore
	binlogChecker binlog_checker.BinlogChecker
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "tidbkafka", &tidbInput{}, false)
}

func (plugin *tidbInput) Configure(pipelineName string, data map[string]interface{}) error {
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
		return errors.Errorf("offset-store must be configured")
	}

	plugin.cfg = &cfg

	return nil
}

func (plugin *tidbInput) Start(emitter core.Emitter) error {
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

	plugin.store = &offsetStoreAdapter{
		name:     plugin.pipelineName,
		delegate: binlogTailer.consumer.OffsetStore().(*DBOffsetStore),
		group:    cfg.SourceKafka.GroupID,
		topic:    cfg.SourceKafka.Topics,
	}

	if err := plugin.binlogTailer.Start(); err != nil {
		return errors.Trace(err)
	}

	if err := plugin.binlogChecker.Start(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (plugin *tidbInput) Stage() config.InputMode {
	return config.Stream
}

func (plugin *tidbInput) PositionStore() position_store.PositionStore {
	return plugin.store
}

func (plugin *tidbInput) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.binlogTailer.Wait()
		c <- plugin.PositionStore().Position()
		close(c)
	}()
	return c
}

func (plugin *tidbInput) SendDeadSignal() error {
	db, err := utils.CreateDBConnection(plugin.cfg.SourceDB)
	if err != nil {
		return errors.Trace(err)
	}
	return mysql_test.SendDeadSignal(db, plugin.Identity())
}

func (plugin *tidbInput) Wait() {
	plugin.binlogTailer.Wait()
}

func (plugin *tidbInput) Identity() uint32 {
	return plugin.gravityServerID
}

func (plugin *tidbInput) Close() {
	log.Infof("[mysql_binlog_server] stop...")

	plugin.binlogTailer.Close()
	log.Infof("[mysql_binlog_server] stopped binlogTailer")

	plugin.binlogChecker.Stop()
	log.Infof("[mysql_binlog_server] stopped binlogChecker")
}

type offsetStoreAdapter struct {
	delegate *DBOffsetStore
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
		log.Fatalf("db offset store closes the DB connection with error. %s", err)
	}
	log.Info("db offset store closes the DB connection")
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
