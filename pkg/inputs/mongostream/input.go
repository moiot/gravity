package mongostream

import (
	"context"
	"sync"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
)

type PluginConfig struct {
	// MongoSource *config.MongoSource `mapstructure:"source" toml:"source" json:"source"`
	Source        *config.MongoConnConfig `mapstructure:"source" toml:"source" json:"source"`
	StartPosition *config.MongoPosition   `mapstructure:"start-position" toml:"start-position" json:"start-position"`
	GtmConfig     *config.GtmConfig       `mapstructure:"gtm-config" toml:"gtm-config" json:"gtm-config"`
}

type mongoStreamInput struct {
	pipelineName string

	cfg *PluginConfig

	emitter core.Emitter
	wg      sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	mongoSession  *mgo.Session
	oplogTailer   *OplogTailer
	oplogChecker  *OplogChecker
	positionStore position_store.PositionStore

	closeOnce sync.Once
}

const Name = "mongo-stream"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &mongoStreamInput{}, false)
}

// TODO position store, gtm config, etc
func (plugin *mongoStreamInput) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.Source == nil {
		return errors.Errorf("no mongo source configured")
	}
	plugin.cfg = &cfg
	return nil
}

func (plugin *mongoStreamInput) NewPositionStore() (position_store.PositionStore, error) {
	positionStore, err := position_store.NewMongoPositionStore(plugin.pipelineName, plugin.cfg.Source, plugin.cfg.StartPosition)
	if err != nil {
		return nil, errors.Trace(err)
	}
	plugin.positionStore = positionStore
	return positionStore, nil
}

func (plugin *mongoStreamInput) Start(emitter core.Emitter, router core.Router) error {
	plugin.emitter = emitter
	plugin.ctx, plugin.cancel = context.WithCancel(context.Background())

	session, err := mongo.CreateMongoSession(plugin.cfg.Source)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.mongoSession = session

	cfg := plugin.cfg

	// Create tailers, senders, oplog checkers
	checker := NewOplogChecker(session, cfg.Source.Host, plugin.pipelineName, plugin.ctx)

	tailerOpts := OplogTailerOpt{
		oplogChecker:   checker,
		session:        session,
		gtmConfig:      cfg.GtmConfig,
		emitter:        emitter,
		router:         router,
		ctx:            plugin.ctx,
		sourceHost:     cfg.Source.Host,
		timestampStore: plugin.positionStore.(position_store.MongoPositionStore),
		pipelineName:   plugin.pipelineName,
	}
	tailer := NewOplogTailer(&tailerOpts)

	plugin.oplogTailer = tailer
	plugin.oplogChecker = checker

	plugin.wg.Add(1)
	go func(t *OplogTailer) {
		defer plugin.wg.Done()
		t.Run()
	}(tailer)

	plugin.wg.Add(1)
	go func(c *OplogChecker) {
		defer plugin.wg.Done()
		c.Run()
	}(checker)

	return nil
}

func (plugin *mongoStreamInput) Stage() config.InputMode {
	return config.Stream
}

func (plugin *mongoStreamInput) PositionStore() position_store.PositionStore {
	return plugin.positionStore
}

func (plugin *mongoStreamInput) Done() chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.Wait()
		c <- plugin.positionStore.Position()
		close(c)
	}()
	return c
}

func (plugin *mongoStreamInput) Wait() {
	plugin.oplogTailer.Wait()
}

func (plugin *mongoStreamInput) SendDeadSignal() error {
	return errors.Trace(plugin.oplogTailer.SendDeadSignal())
}

func (plugin *mongoStreamInput) Close() {
	plugin.closeOnce.Do(func() {
		plugin.cancel()

		log.Infof("[mongoStreamInput] wait others")
		plugin.wg.Wait()
		plugin.mongoSession.Close()
	})
}
