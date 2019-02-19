package mongobatch

import (
	"fmt"
	"sync"
	"time"

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
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

type Config struct {
	Source         *config.MongoConnConfig `mapstructure:"source" toml:"source" json:"source"`
	BatchSize      int                     `mapstructure:"batch-size"  toml:"batch-size" json:"batch-size"`
	WorkerCnt      int                     `mapstructure:"worker-cnt" toml:"worker-cnt" json:"worker-cnt"`
	ChunkThreshold int                     `mapstructure:"chunk-threshold"  toml:"chunk-threshold"  json:"chunk-threshold"`
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

	positionCache position_store.PositionCacheInterface

	chunkMap map[string]int
	pos      PositionValue
	posMeta  position_store.PositionMeta
	posLock  sync.Mutex
}

func (input *mongoBatchInput) Configure(pipelineName string, data map[string]interface{}) error {
	input.pipelineName = pipelineName

	cfg := Config{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if err := cfg.validateAndSetDefault(); err != nil {
		return errors.Trace(err)
	}

	input.cfg = cfg
	input.closeC = make(chan struct{})
	return nil
}

func (input *mongoBatchInput) Start(emitter core.Emitter, router core.Router, positionCache position_store.PositionCacheInterface) error {
	session, err := mongo.CreateMongoSession(input.cfg.Source)
	if err != nil {
		return errors.Trace(err)
	}
	input.session = session
	input.emitter = emitter
	input.router = router
	input.positionCache = positionCache

	if err := SetupInitialPosition(positionCache, session, router, input.cfg); err != nil {
		return errors.Trace(err)
	}

	rawPos, exists, err := positionCache.Get()
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Errorf("[mongoBatchInput.Start] position not initialized")
	}
	input.posMeta = rawPos.PositionMeta
	pos := rawPos.Value.(PositionValue)
	input.pos = pos

	input.chunkMap = make(map[string]int)
	for i, c := range pos.Chunks {
		input.chunkMap[c.key()] = i
	}

	log.Debugf("[mongoBatchInput] chunks: %v", pos.Chunks)

	taskC := make(chan chunk, len(input.pos.Chunks))
	for _, c := range input.pos.Chunks {
		if !c.Done {
			taskC <- c
		}
	}
	close(taskC)

	input.wg.Add(input.cfg.WorkerCnt)
	for i := 0; i < input.cfg.WorkerCnt; i++ {
		go input.runWorker(taskC)
	}
	return nil
}

func (input *mongoBatchInput) Close() {
	log.Infof("[mongoBatchInput] closing")
	close(input.closeC)
	input.wg.Wait()
	input.session.Close()
	log.Infof("[mongoBatchInput] closed")
}

func (input *mongoBatchInput) Stage() config.InputMode {
	return config.Batch
}

func (input *mongoBatchInput) NewPositionCache() (position_store.PositionCacheInterface, error) {
	session, err := mongo.CreateMongoSession(input.cfg.Source)
	if err != nil {
		return nil, errors.Trace(err)
	}
	positionRepo, err := position_store.NewMongoPositionRepo(session)
	if err != nil {
		return nil, errors.Trace(err)
	}

	positionCache, err := position_store.NewPositionCache(
		input.pipelineName,
		positionRepo,
		Encode,
		Decode,
		position_store.DefaultFlushPeriod)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return positionCache, nil
}

func (input *mongoBatchInput) Done() chan position_store.Position {
	ret := make(chan position_store.Position)
	go func() {
		input.Wait()
		select {
		case <-input.closeC:
			log.Info("[mongoBatchInput] canceled")
			close(ret)
		default:
			log.Info("[mongoBatchInput] done with start position: ", input.pos.Start)
			start := config.MongoPosition(input.pos.Start)
			ret <- position_store.Position{
				PositionMeta: position_store.PositionMeta{
					Version:    input.posMeta.Version,
					Name:       input.posMeta.Name,
					Stage:      config.Stream,
					UpdateTime: time.Time{},
				},
				Value: mongostream.OplogPositionValue{
					StartPosition:   &start,
					CurrentPosition: config.MongoPosition(input.pos.Start),
				},
			}
			close(ret)
		}
	}()
	return ret
}

func (input *mongoBatchInput) SendDeadSignal() error {
	input.Close()
	return nil
}

func (input *mongoBatchInput) Wait() {
	input.wg.Wait()
}

func (input *mongoBatchInput) runWorker(ch chan chunk) {
	defer input.wg.Done()

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
				c := input.session.DB(task.Database).C(task.Collection)
				var query bson.D
				if task.Current != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$gt", Value: task.Current}}})
				}
				if task.Max != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$lte", Value: task.Max}}})
				}
				var results []map[string]interface{}
				err := c.Find(query).Sort("_id").Limit(input.cfg.BatchSize).Hint("_id").All(&results)
				if err != nil {
					log.Fatalf("[mongoBatchInput] error query for task. %s", errors.ErrorStack(err))
				}
				actualCount = len(results)
				if actualCount == 0 {
					log.Infof("[mongoBatchInput] done chunk %#v", task)
					input.finishChunk(task)
					break
				} else {
					log.Debugf("[mongoBatchInput] %d records returned from query %v", actualCount, query)
				}
				id := results[len(results)-1]["_id"].(bson.ObjectId)
				task.Current = &id
				task.Scanned += len(results)
				now := time.Now()
				metrics.InputCounter.WithLabelValues(input.pipelineName, task.Database, task.Collection, string(core.MsgDML), string(core.Insert)).Add(float64(len(results)))
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
						Host:     input.cfg.Source.Host,
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
						AfterCommitCallback: input.AfterMsgCommit,
						InputContext:        task,
						Timestamp:           now,
						Oplog:               &op,
						Done:                make(chan struct{}),
						InputStreamKey:      utils.NewStringPtr(task.key()),
						OutputStreamKey:     utils.NewStringPtr(op.Namespace + "-" + fmt.Sprint(op.Id)),
					}
					if err := input.emitter.Emit(&msg); err != nil {
						log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
					}

					select {
					case <-input.closeC:
						log.Infof("[mongoBatchInput] canceled")
						return
					default:
					}
				}
			}
		case <-input.closeC:
			return
		}
	}
}

func (input *mongoBatchInput) finishChunk(c chunk) {
	c.Done = true
	msg := &core.Msg{
		Phase: core.Phase{
			EnterInput: time.Now(),
		},
		Type:            core.MsgCtl,
		InputStreamKey:  utils.NewStringPtr(c.key()),
		OutputStreamKey: utils.NewStringPtr(""),
		Done:            make(chan struct{}),
	}
	if err := input.emitter.Emit(msg); err != nil {
		log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
	}
	<-msg.Done
	msg = &core.Msg{
		Phase: core.Phase{
			EnterInput: time.Now(),
		},
		Type:            core.MsgCloseInputStream,
		InputStreamKey:  utils.NewStringPtr(c.key()),
		OutputStreamKey: utils.NewStringPtr(""),
		Done:            make(chan struct{}),
	}
	metrics.InputCounter.WithLabelValues(input.pipelineName, msg.Database, msg.Table, string(msg.Type), "").Add(1)
	if err := input.emitter.Emit(msg); err != nil {
		log.Fatalf("failed to emit: %v", errors.ErrorStack(err))
	}
	<-msg.Done
	if err := input.saveChunk(c); err != nil {
		log.Fatalf("failed to save chunk: %v", errors.ErrorStack(err))
	}
	if err := input.positionCache.Flush(); err != nil {
		log.Fatalf("failed to flush position: %v", errors.ErrorStack(err))
	}
}

func (input *mongoBatchInput) AfterMsgCommit(msg *core.Msg) error {
	c := msg.InputContext.(chunk)
	return input.saveChunk(c)
}

func (input *mongoBatchInput) saveChunk(c chunk) error {
	input.pos.Chunks[input.chunkMap[c.key()]] = c

	input.posLock.Lock()
	if err := input.positionCache.Put(position_store.Position{
		PositionMeta: input.posMeta,
		Value:        input.pos,
	}); err != nil {
		return errors.Trace(err)
	}
	input.posLock.Unlock()
	return nil
}
