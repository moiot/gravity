package mongobatch

import (
	"fmt"
	"math"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo/gtm"
	"github.com/moiot/gravity/pkg/position_store"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type chunk struct {
	Database   string      `json:"database" bson:"database"`
	Collection string      `json:"collection" bson:"collection"`
	Seq        int         `json:"seq" bson:"seq"`
	Done       bool        `json:"done" bson:"done"`
	Min        interface{} `json:"min,omitempty" bson:"min,omitempty"`
	Max        interface{} `json:"max,omitempty" bson:"max,omitempty"`
	Current    interface{} `json:"current,omitempty" bson:"current,omitempty"`
}

func (c *chunk) key() string {
	return fmt.Sprintf("%s-%s-%d", c.Database, c.Collection, c.Seq)
}

type PositionValue struct {
	Start  bson.MongoTimestamp `bson:"start" json:"start"`
	Chunks []chunk             `bson:"chunks"  json:"chunks"`
}

func Encode(v interface{}) (string, error) {
	return myJson.MarshalToString(v)
}

func Decode(s string) (interface{}, error) {
	v := PositionValue{}
	if err := myJson.UnmarshalFromString(s, &v); err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

func SetupInitialPosition(cache position_store.PositionCacheInterface, session *mgo.Session, router core.Router, cfg Config) error {
	_, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		options := gtm.DefaultOptions()
		options.Fill(session, "")
		startPos := gtm.LastOpTimestamp(session, options)

		collections := make(map[string][]string)
		for db, colls := range mongo.ListAllCollections(session) {
			for _, coll := range colls {
				msg := core.Msg{
					Database: db,
					Table:    coll,
				}
				if router.Exists(&msg) {
					log.Infof("add %s.%s to scan", db, coll)
					collections[db] = append(collections[db], coll)
				}
			}
		}

		chunks := calculateChunks(session, collections, cfg.ChunkThreshold, cfg.WorkerCnt)
		batchPositionValue := PositionValue{
			Start:  startPos,
			Chunks: chunks,
		}
		position := position_store.Position{}
		position.Value = batchPositionValue
		position.Stage = config.Batch
		position.UpdateTime = time.Now()
		if err := cache.Put(position); err != nil {
			return errors.Trace(err)
		}
		if err := cache.Flush(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
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
							Database:   db,
							Collection: coll,
							Min:        nil,
							Max:        mm.Min,
							Seq:        seq,
						})
						seq++
					}
					ret = append(ret, chunk{
						Database:   db,
						Collection: coll,
						Min:        mm.Min,
						Max:        mm.Max,
						Seq:        seq,
					})
					seq++
				}
				ret = append(ret, chunk{
					Database:   db,
					Collection: coll,
					Min:        ret[len(ret)-1].Max,
					Max:        nil,
					Seq:        seq,
				})
			} else {
				ret = append(ret, chunk{
					Database:   db,
					Collection: coll,
					Min:        nil,
					Max:        nil,
				})
			}
		}
	}
	return ret
}
