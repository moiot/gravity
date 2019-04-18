package mongobatch

import (
	"fmt"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/inputs/mongostream"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo/gtm"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

type Config struct {
	Source         *config.MongoConnConfig     `mapstructure:"source" toml:"source" json:"source"`
	PositionRepo   *config.GenericPluginConfig `mapstructure:"position-repo" toml:"position-repo" json:"position-repo"`
	BatchSize      int                         `mapstructure:"batch-size"  toml:"batch-size" json:"batch-size"`
	WorkerCnt      int                         `mapstructure:"worker-cnt" toml:"worker-cnt" json:"worker-cnt"`
	ChunkThreshold int                         `mapstructure:"chunk-threshold"  toml:"chunk-threshold"  json:"chunk-threshold"`
	// IgnoreOplogError ignores error with oplog.
	// Some mongo cluster setup may not support oplog.
	IgnoreOplogError bool `mapstructure:"ignore-oplog-error" toml:"ignore-oplog-error" json:"ignore-oplog-error"`
}

func (c *Config) validateAndSetDefault() error {
	if c.Source == nil {
		return errors.Errorf("no mongo source configured")
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}

	if c.WorkerCnt <= 0 {
		c.WorkerCnt = 10
	}

	if c.ChunkThreshold <= 0 {
		c.ChunkThreshold = 500000
	}

	if c.PositionRepo == nil {
		c.PositionRepo = position_repos.NewMongoRepoConfig(c.Source)
	}

	return nil
}

const Name = "mongo-batch"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &mongoBatchInput{}, false)
}

type mongoBatchInput struct {
	pipelineName string
	cfg          Config

	emitter core.Emitter
	router  core.Router
	session *mgo.Session

	wg     sync.WaitGroup
	closeC chan struct{}

	positionRepo  position_repos.PositionRepo
	positionCache position_cache.PositionCacheInterface

	chunkMap map[string]int
	pos      PositionValue
	posMeta  position_repos.PositionMeta
	posLock  sync.Mutex
}

func (plugin *mongoBatchInput) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	cfg := Config{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if err := cfg.validateAndSetDefault(); err != nil {
		return errors.Trace(err)
	}

	positionRepo, err := registry.GetPlugin(registry.PositionRepo, cfg.PositionRepo.Type)
	if err != nil {
		return errors.Trace(err)
	}
	if err := positionRepo.Configure(pipelineName, cfg.PositionRepo.Config); err != nil {
		return errors.Trace(err)
	}

	plugin.positionRepo = positionRepo.(position_repos.PositionRepo)
	plugin.cfg = cfg
	plugin.closeC = make(chan struct{})
	return nil
}

func (plugin *mongoBatchInput) Start(emitter core.Emitter, router core.Router, positionCache position_cache.PositionCacheInterface) error {
	session, err := mongo.CreateMongoSession(plugin.cfg.Source)
	if err != nil {
		return errors.Trace(err)
	}
	plugin.session = session
	plugin.emitter = emitter
	plugin.router = router
	plugin.positionCache = positionCache

	if err := SetupInitialPosition(positionCache, session, router, plugin.cfg); err != nil {
		return errors.Trace(err)
	}

	rawPos, exists, err := positionCache.Get()
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Errorf("[mongoBatchInput.Start] position not initialized")
	}
	plugin.posMeta = rawPos.PositionMeta
	pos := rawPos.Value.(PositionValue)
	plugin.pos = pos

	plugin.chunkMap = make(map[string]int)
	for i, c := range pos.Chunks {
		plugin.chunkMap[c.key()] = i
	}

	log.Debugf("[mongoBatchInput] chunks: %v", pos.Chunks)

	taskC := make(chan chunk, len(plugin.pos.Chunks))
	for _, c := range plugin.pos.Chunks {
		if !c.Done {
			taskC <- c
		}
	}
	close(taskC)

	plugin.wg.Add(plugin.cfg.WorkerCnt)
	for i := 0; i < plugin.cfg.WorkerCnt; i++ {
		go plugin.runWorker(taskC)
	}
	return nil
}

func (plugin *mongoBatchInput) Close() {
	log.Infof("[mongoBatchInput] closing")
	close(plugin.closeC)
	plugin.wg.Wait()
	plugin.session.Close()
	log.Infof("[mongoBatchInput] closed")
}

func (plugin *mongoBatchInput) Stage() config.InputMode {
	return config.Batch
}

func (plugin *mongoBatchInput) NewPositionCache() (position_cache.PositionCacheInterface, error) {
	if err := plugin.positionRepo.Init(); err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_cache.NewPositionCache(
		plugin.pipelineName,
		plugin.positionRepo,
		Encode,
		Decode,
		position_cache.DefaultFlushPeriod)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return positionCache, nil
}

func (plugin *mongoBatchInput) Done() chan position_repos.Position {
	ret := make(chan position_repos.Position)
	go func() {
		plugin.Wait()
		select {
		case <-plugin.closeC:
			log.Info("[mongoBatchInput] canceled")
			close(ret)
		default:
			log.Info("[mongoBatchInput] done with start position: ", plugin.pos.Start)
			start := config.MongoPosition(plugin.pos.Start)
			ret <- position_repos.Position{
				PositionMeta: position_repos.PositionMeta{
					Version:    plugin.posMeta.Version,
					Name:       plugin.posMeta.Name,
					Stage:      config.Stream,
					UpdateTime: time.Time{},
				},
				Value: mongostream.OplogPositionValue{
					StartPosition:   &start,
					CurrentPosition: config.MongoPosition(plugin.pos.Start),
				},
			}
			close(ret)
		}
	}()
	return ret
}

func (plugin *mongoBatchInput) SendDeadSignal() error {
	plugin.Close()
	return nil
}

func (plugin *mongoBatchInput) Wait() {
	plugin.wg.Wait()
}

func (plugin *mongoBatchInput) runWorker(ch chan chunk) {
	defer plugin.wg.Done()

	for {
		select {
		case task, ok := <-ch:
			if !ok {
				log.Infof("[mongoBatchInput] no more chunk, exit worker.")
				return
			}
			if task.Current == nil {
				task.Current = task.Min
			}
			var actualCount int
			for {
				c := plugin.session.DB(task.Database).C(task.Collection)
				var query bson.D
				if task.Current != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$gt", Value: task.Current}}})
				}
				if task.Max != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$lte", Value: task.Max}}})
				}
				var results []map[string]interface{}
				err := c.Find(query).Sort("_id").Limit(plugin.cfg.BatchSize).Hint("_id").All(&results)
				if err != nil {
					log.Fatalf("[mongoBatchInput] error query for task. %s", errors.ErrorStack(err))
				}
				actualCount = len(results)
				if actualCount == 0 {
					log.Infof("[mongoBatchInput] done chunk %#v", task)
					plugin.finishChunk(task)
					break
				} else {
					log.Debugf("[mongoBatchInput] %d records returned from query %v", actualCount, query)
				}
				id := results[len(results)-1]["_id"].(bson.ObjectId)
				task.Current = &id
				task.Scanned += len(results)
				now := time.Now()
				metrics.InputCounter.WithLabelValues(plugin.pipelineName, task.Database, task.Collection, string(core.MsgDML), string(core.Insert)).Add(float64(len(results)))
				for _, result := range results {
					op := gtm.Op{
						Id:        result["_id"],
						Operation: "i",
						Namespace: fmt.Sprintf("%s.%s", task.Database, task.Collection),
						Data:      result,
						Row:       nil,
						Timestamp: bson.MongoTimestamp(now.Unix() << 32),
						Source:    gtm.DirectQuerySource,
					}

					msg := core.Msg{
						Phase: core.Phase{
							EnterInput: time.Now(),
						},
						Type:     core.MsgDML,
						Host:     plugin.cfg.Source.Host,
						Database: task.Database,
						Table:    task.Collection,
						DmlMsg: &core.DMLMsg{
							Operation: core.Insert,
							Data:      result,
							Old:       make(map[string]interface{}),
							Pks: map[string]interface{}{
								"_id": op.Id,
							},
						},
						AfterCommitCallback: plugin.AfterMsgCommit,
						InputContext:        task,
						Timestamp:           now,
						Oplog:               &op,
						Done:                make(chan struct{}),
						InputStreamKey:      utils.NewStringPtr(task.key()),
					}
					if err := plugin.emitter.Emit(&msg); err != nil {
						log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
					}

					select {
					case <-plugin.closeC:
						log.Infof("[mongoBatchInput] canceled")
						return
					default:
					}
				}
			}
		case <-plugin.closeC:
			return
		}
	}
}

func (plugin *mongoBatchInput) finishChunk(c chunk) {
	c.Done = true
	msg := &core.Msg{
		Phase: core.Phase{
			EnterInput: time.Now(),
		},
		Type:           core.MsgCtl,
		InputStreamKey: utils.NewStringPtr(c.key()),
		Done:           make(chan struct{}),
	}
	if err := plugin.emitter.Emit(msg); err != nil {
		log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
	}
	<-msg.Done
	msg = &core.Msg{
		Phase: core.Phase{
			EnterInput: time.Now(),
		},
		Type:           core.MsgCloseInputStream,
		InputStreamKey: utils.NewStringPtr(c.key()),
		Done:           make(chan struct{}),
	}
	metrics.InputCounter.WithLabelValues(plugin.pipelineName, msg.Database, msg.Table, string(msg.Type), "").Add(1)
	if err := plugin.emitter.Emit(msg); err != nil {
		log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
	}
	<-msg.Done
	if err := plugin.saveChunk(c); err != nil {
		log.Fatalf("failed to save chunk: %v", errors.ErrorStack(err))
	}
	if err := plugin.positionCache.Flush(); err != nil {
		log.Fatalf("failed to flush position: %v", errors.ErrorStack(err))
	}
}

func (plugin *mongoBatchInput) AfterMsgCommit(msg *core.Msg) error {
	c := msg.InputContext.(chunk)
	return plugin.saveChunk(c)
}

func (plugin *mongoBatchInput) saveChunk(c chunk) error {
	plugin.pos.Chunks[plugin.chunkMap[c.key()]] = c

	plugin.posLock.Lock()
	if err := plugin.positionCache.Put(position_repos.Position{
		PositionMeta: plugin.posMeta,
		Value:        plugin.pos,
	}); err != nil {
		return errors.Trace(err)
	}
	plugin.posLock.Unlock()
	return nil
}
