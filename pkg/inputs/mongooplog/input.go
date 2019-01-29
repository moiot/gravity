package mongooplog

import (
	"context"
	"sync"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"

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

type mongoInputPlugin struct {
	pipelineName string

	cfg *PluginConfig

	emitter core.Emitter
	wg      sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	mongoSession *mgo.Session
	oplogTailer  *OplogTailer
	oplogChecker *OplogChecker
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mongooplog", &mongoInputPlugin{}, false)
}

// TODO position store, gtm config, etc
func (plugin *mongoInputPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.Source == nil {
		return errors.Errorf("no mongo source confgiured")
	}
	plugin.cfg = &cfg
	return nil
}

func (plugin *mongoInputPlugin) NewPositionCache() (position_store.PositionCacheInterface, error) {
	session, err := mongo.CreateMongoSession(plugin.cfg.Source)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionRepo, err := position_store.NewMongoPositionRepo(session)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_store.NewPositionCache(plugin.pipelineName, positionRepo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := InitPositionCache(positionCache, plugin.cfg.StartPosition); err != nil {
		return nil, errors.Trace(err)
	}

	return positionCache, nil
}

func (plugin *mongoInputPlugin) Start(emitter core.Emitter, positionCache position_store.PositionCacheInterface) error {
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
		oplogChecker:  checker,
		session:       session,
		gtmConfig:     cfg.GtmConfig,
		emitter:       emitter,
		ctx:           plugin.ctx,
		sourceHost:    cfg.Source.Host,
		positionCache: positionCache,
		pipelineName:  plugin.pipelineName,
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

func (plugin *mongoInputPlugin) Stage() config.InputMode {
	return config.Stream
}

func (plugin *mongoInputPlugin) Done(positionCache position_store.PositionCacheInterface) chan position_store.Position {
	c := make(chan position_store.Position)
	go func() {
		plugin.Wait()
		c <- positionCache.Get()
		close(c)
	}()
	return c
}

func (plugin *mongoInputPlugin) Wait() {
	plugin.oplogTailer.Wait()
}

func (plugin *mongoInputPlugin) SendDeadSignal() error {
	return errors.Trace(plugin.oplogTailer.SendDeadSignal())
}

func (plugin *mongoInputPlugin) Identity() uint32 {
	return 0
}

func (plugin *mongoInputPlugin) Close() {
	plugin.cancel()

	log.Infof("[mongoInputPlugin] wait others")
	plugin.wg.Wait()
	plugin.mongoSession.Close()
}
