package mongostream

import (
	"context"

	"sync"

	"github.com/moiot/gravity/pkg/position_repos"
	mgo "gopkg.in/mgo.v2"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/registry"
)

type PluginConfig struct {
	Source        *config.MongoConnConfig     `mapstructure:"source" toml:"source" json:"source"`
	PositionRepo  *config.GenericPluginConfig `mapstructure:"position-repo" toml:"position-repo" json:"position-repo"`
	StartPosition *config.MongoPosition       `mapstructure:"start-position" toml:"start-position" json:"start-position"`
	GtmConfig     *config.GtmConfig           `mapstructure:"gtm-config" toml:"gtm-config" json:"gtm-config"`
}

type mongoStreamInputPlugin struct {
	pipelineName string

	cfg *PluginConfig

	emitter core.Emitter
	wg      sync.WaitGroup

	ctx           context.Context
	cancel        context.CancelFunc
	positionRepo  position_repos.PositionRepo
	positionCache position_cache.PositionCacheInterface
	mongoSession  *mgo.Session
	oplogTailer   *OplogTailer
	oplogChecker  *OplogChecker
}

const Name = "mongo-stream"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &mongoStreamInputPlugin{}, false)
}

// TODO position store, gtm config, etc
func (plugin *mongoStreamInputPlugin) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := PluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.Source == nil {
		return errors.Errorf("no mongo source configured")
	}

	if cfg.PositionRepo == nil {
		cfg.PositionRepo = position_repos.NewMongoRepoConfig(cfg.Source)
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

func (plugin *mongoStreamInputPlugin) NewPositionCache() (position_cache.PositionCacheInterface, error) {
	if err := plugin.positionRepo.Init(); err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_cache.NewPositionCache(
		plugin.pipelineName,
		plugin.positionRepo,
		OplogPositionValueEncoder,
		OplogPositionValueDecoder,
		position_cache.DefaultFlushPeriod)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := SetupInitialPosition(positionCache, plugin.cfg.StartPosition); err != nil {
		return nil, errors.Trace(err)
	}

	return positionCache, nil
}

func (plugin *mongoStreamInputPlugin) Start(emitter core.Emitter, router core.Router, positionCache position_cache.PositionCacheInterface) error {
	plugin.emitter = emitter
	plugin.positionCache = positionCache
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
		router:        router,
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

func (plugin *mongoStreamInputPlugin) Stage() config.InputMode {
	return config.Stream
}

func (plugin *mongoStreamInputPlugin) Done() chan position_repos.Position {
	c := make(chan position_repos.Position)
	go func() {
		plugin.Wait()
		position, exist, err := plugin.positionCache.Get()
		if err != nil && exist {
			c <- position
		} else {
			log.Fatalf("[mongoStreamInputPlugin] failed to get position, exist: %v, err: %v", exist, errors.ErrorStack(err))
		}

		close(c)
	}()
	return c
}

func (plugin *mongoStreamInputPlugin) Wait() {
	plugin.oplogTailer.Wait()
}

func (plugin *mongoStreamInputPlugin) SendDeadSignal() error {
	return errors.Trace(plugin.oplogTailer.SendDeadSignal())
}

func (plugin *mongoStreamInputPlugin) Close() {
	plugin.cancel()

	log.Infof("[mongoStreamInputPlugin] wait others")
	plugin.wg.Wait()
	plugin.mongoSession.Close()
}
