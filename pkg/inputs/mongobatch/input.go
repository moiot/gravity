package mongobatch

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo/gtm"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/registry"
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

	*memPositionStore
}

type memPositionStore struct {
	name     string
	startPos bson.MongoTimestamp
	chunks   []chunk
}

func (ps *memPositionStore) Start() error {
	return nil
}

func (ps *memPositionStore) Close() {
}

func (ps *memPositionStore) Stage() config.InputMode {
	return config.Batch
}

func (ps *memPositionStore) Position() position_store.Position {
	return position_store.Position{
		Name:       ps.name,
		Stage:      config.Batch,
		Raw:        ps,
		UpdateTime: time.Now(),
	}
}

func (ps *memPositionStore) Update(pos position_store.Position) {
	*ps = *pos.Raw.(*memPositionStore)
}

func (ps *memPositionStore) Clear() {
}

type chunk struct {
	database   string
	collection string
	seq        int
	min        interface{}
	max        interface{}
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

func (input *mongoBatchInput) Start(emitter core.Emitter, router core.Router) error {
	session, err := mongo.CreateMongoSession(input.cfg.Source)
	if err != nil {
		return errors.Trace(err)
	}
	input.session = session
	input.emitter = emitter
	input.router = router

	// TODO restore position from store
	input.initPosition()
	log.Debugf("[mongoBatchInput] chunks: %v", input.chunks)

	taskC := make(chan chunk, len(input.chunks))
	for _, c := range input.chunks {
		taskC <- c
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

func (input *mongoBatchInput) NewPositionStore() (position_store.PositionStore, error) {
	input.memPositionStore = &memPositionStore{
		name: input.pipelineName,
	}
	return input.memPositionStore, nil
}

func (input *mongoBatchInput) PositionStore() position_store.PositionStore {
	return input.memPositionStore
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
			log.Info("[mongoBatchInput] done with start position: ", input.startPos)
			ret <- position_store.Position{
				Name:  input.name,
				Stage: config.Batch,
				Raw: position_store.MongoPosition{
					StartPosition:   config.MongoPosition(input.startPos),
					CurrentPosition: config.MongoPosition(input.startPos),
				},
				UpdateTime: time.Now(),
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

func (input *mongoBatchInput) initPosition() {
	options := gtm.DefaultOptions()
	options.Fill(input.session, "")
	input.startPos = gtm.LastOpTimestamp(input.session, options)

	collections := make(map[string][]string)
	for db, colls := range mongo.ListAllCollections(input.session) {
		for _, coll := range colls {
			msg := core.Msg{
				Database: db,
				Table:    coll,
			}
			if input.router.Exists(&msg) {
				log.Infof("add %s.%s to scan", db, coll)
				collections[db] = append(collections[db], coll)
			}
		}
	}

	input.chunks = calculateChunks(input.session, collections, input.cfg.ChunkThreshold, input.cfg.WorkerCnt)
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
			currentMin := task.min
			var actualCount int
			for {
				c := input.session.DB(task.database).C(task.collection)
				var query bson.D
				if currentMin != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$gt", Value: currentMin}}})
				}
				if task.max != nil {
					query = append(query, bson.DocElem{Name: "_id", Value: bson.D{{Name: "$lte", Value: task.max}}})
				}
				var results []map[string]interface{}
				err := c.Find(query).Sort("_id").Limit(input.cfg.BatchSize).Hint("_id").All(&results)
				if err != nil {
					log.Fatalf("[mongoBatchInput] error query for task. %s", errors.ErrorStack(err))
				}
				actualCount = len(results)
				if actualCount == 0 {
					log.Infof("[mongoBatchInput] done chunk %v", task)
					break
				} else {
					log.Debugf("[mongoBatchInput] %d records returned from query %v", actualCount, query)
				}
				currentMin = results[len(results)-1]["_id"]
				now := time.Now()
				for _, result := range results {
					op := gtm.Op{
						Id:        result["_id"],
						Operation: "i",
						Namespace: fmt.Sprintf("%s.%s", task.database, task.collection),
						Data:      result,
						Row:       nil,
						Timestamp: bson.MongoTimestamp(now.Unix()),
						Source:    gtm.DirectQuerySource,
					}

					msg := core.Msg{
						Type:     core.MsgDML,
						Host:     input.cfg.Source.Host,
						Database: task.database,
						Table:    task.collection,
						DmlMsg: &core.DMLMsg{
							Operation: core.Insert,
							Data:      result,
							Old:       make(map[string]interface{}),
							Pks: map[string]interface{}{
								"_id": op.Id,
							},
						},
						Timestamp:       now,
						Oplog:           &op,
						Done:            make(chan struct{}),
						InputStreamKey:  utils.NewStringPtr(op.Namespace + "-" + strconv.Itoa(task.seq)),
						OutputStreamKey: utils.NewStringPtr(op.Namespace + "-" + fmt.Sprint(op.Id)),
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

const maxSampleSize = 100000

func calculateChunks(session *mgo.Session, collections map[string][]string, chunkThreshold int, chunkCnt int) []chunk {
	var ret []chunk
	for db, colls := range collections {
		for _, coll := range colls {
			count := mongo.Count(session, db, coll)
			if count == 0 {
				continue
			} else if count > chunkThreshold {
				seq := 0
				sampleCnt := int(math.Min(maxSampleSize, float64(count)*0.05))
				for i, mm := range mongo.BucketAuto(session, db, coll, sampleCnt, chunkCnt) {
					if i == 0 {
						ret = append(ret, chunk{
							database:   db,
							collection: coll,
							min:        nil,
							max:        mm.Min,
							seq:        seq,
						})
						seq++
					}
					ret = append(ret, chunk{
						database:   db,
						collection: coll,
						min:        mm.Min,
						max:        mm.Max,
						seq:        seq,
					})
					seq++
				}
				ret = append(ret, chunk{
					database:   db,
					collection: coll,
					min:        ret[len(ret)-1].max,
					max:        nil,
					seq:        seq,
				})
			} else {
				ret = append(ret, chunk{
					database:   db,
					collection: coll,
					min:        nil,
					max:        nil,
				})
			}
		}
	}
	return ret
}
