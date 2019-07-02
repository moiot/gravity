package mongobatch

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/pkg/position_repos"

	jsoniter "github.com/json-iterator/go"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo/gtm"
	"github.com/moiot/gravity/pkg/position_cache"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

const (
	PlainString  = "string"
	PlainInt     = "int"
	PlainUInt    = "uint"
	BsonObjectID = "bsonID"
)

type IDValue struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

func (idValue IDValue) MarshalJSON() ([]byte, error) {
	var vs string
	var t string
	switch v := idValue.Value.(type) {
	case bson.ObjectId:
		vs = string(v)
		t = BsonObjectID
	case string:
		vs = v
		t = PlainString
	case uint8:
		vs = strconv.FormatUint(uint64(v), 10)
		t = PlainUInt
	case uint16:
		vs = strconv.FormatUint(uint64(v), 10)
		t = PlainUInt
	case uint32:
		vs = strconv.FormatUint(uint64(v), 10)
		t = PlainUInt
	case uint64:
		vs = strconv.FormatUint(uint64(v), 10)
		t = PlainUInt
	case uint:
		vs = strconv.FormatUint(uint64(v), 10)
		t = PlainUInt
	case int8:
		vs = strconv.FormatInt(int64(v), 10)
		t = PlainInt
	case int16:
		vs = strconv.FormatInt(int64(v), 10)
		t = PlainInt
	case int32:
		vs = strconv.FormatInt(int64(v), 10)
		t = PlainInt
	case int64:
		vs = strconv.FormatInt(int64(v), 10)
		t = PlainInt
	case int:
		vs = strconv.FormatInt(int64(v), 10)
		t = PlainInt
	default:
		panic(fmt.Sprintf("not supported yet: %v", reflect.TypeOf(v)))
	}

	ret := make(map[string]string)
	ret["type"] = t
	ret["value"] = vs
	return myJson.Marshal(ret)
}

func (idValue *IDValue) UnmarshalJSON(value []byte) error {
	m := make(map[string]string)
	if err := myJson.Unmarshal(value, &m); err != nil {
		return errors.Trace(err)
	}
	switch m["type"] {
	case PlainString:
		idValue.Type = PlainString
		idValue.Value = m["value"]
	case PlainUInt:
		v, err := strconv.ParseUint(m["value"], 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		idValue.Type = PlainUInt
		idValue.Value = v
	case PlainInt:
		v, err := strconv.ParseInt(m["value"], 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		idValue.Type = PlainInt
		idValue.Value = v
	case BsonObjectID:
		idValue.Type = BsonObjectID
		idValue.Value = bson.ObjectId(m["value"])
	default:
		panic(fmt.Sprintf("unknown type: %v, value: %v", m["type"], m["value"]))
	}

	return nil
}

type Chunk struct {
	Database   string   `json:"database" bson:"database"`
	Collection string   `json:"collection" bson:"collection"`
	Seq        int      `json:"seq" bson:"seq"`
	Done       bool     `json:"done" bson:"done"`
	Min        *IDValue `json:"min,omitempty" bson:"min,omitempty"`
	Max        *IDValue `json:"max,omitempty" bson:"max,omitempty"`
	Current    *IDValue `json:"current,omitempty" bson:"current,omitempty"`
	Scanned    int64    `json:"scanned" bson:"scanned"`
}

func (c *Chunk) key() string {
	return fmt.Sprintf("%s-%s-%d", c.Database, c.Collection, c.Seq)
}

type PositionValue struct {
	Start  bson.MongoTimestamp `bson:"start" json:"start"`
	Chunks []Chunk             `bson:"chunks"  json:"chunks"`
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

func SetupInitialPosition(
	cache position_cache.PositionCacheInterface,
	session *mgo.Session,
	router core.Router,
	cfg Config) error {

	_, exist, err := cache.Get()
	if err != nil {
		return errors.Trace(err)
	}

	if !exist {
		startPos, err := getStartPosition(session)
		if err != nil {
			if !cfg.IgnoreOplogError {
				return errors.Trace(err)
			}
		}

		collections := make(map[string][]string)
		for db, colls := range utils.ListAllUserCollections(session) {
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

		chunks, err := calculateChunks(session, collections, cfg.ChunkThreshold, cfg.WorkerCnt)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("[SetupInitialPosition] got %d chunks", len(chunks))
		batchPositionValue := PositionValue{
			Start:  startPos,
			Chunks: chunks,
		}
		position := position_repos.Position{}
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

func calculateChunks(
	session *mgo.Session,
	collections map[string][]string,
	chunkThreshold int,
	chunkCnt int) ([]Chunk, error) {

	var ret []Chunk
	for db, colls := range collections {
		for _, coll := range colls {
			count := utils.Count(session, db, coll)
			if count == 0 {
				continue
			} else if count > int64(chunkThreshold) {
				seq := 0
				// If all the following conditions are met, $sample uses a pseudo-random cursor to select documents:
				//
				// - $sample is the first stage of the pipeline
				// - N is less than 5% of the total documents in the collection
				// - The collection contains more than 100 documents
				//
				// Reference: https://docs.mongodb.com/manual/reference/operator/aggregation/sample/#pipe._S_sample
				sampleCnt := int(math.Min(maxSampleSize, float64(count)*0.05))
				buckets, err := utils.BucketAuto(session, db, coll, sampleCnt, chunkCnt)
				if err != nil {
					return []Chunk{}, errors.Trace(err)
				}
				for i, mm := range buckets {
					if i == 0 {
						ret = append(ret, Chunk{
							Database:   db,
							Collection: coll,
							Min:        nil,
							Max:        &IDValue{Value: mm.Min},
							Seq:        seq,
						})
						seq++
					}
					ret = append(ret, Chunk{
						Database:   db,
						Collection: coll,
						Min:        &IDValue{Value: mm.Min},
						Max:        &IDValue{Value: mm.Max},
						Seq:        seq,
					})
					seq++
				}
				ret = append(ret, Chunk{
					Database:   db,
					Collection: coll,
					Min:        ret[len(ret)-1].Max,
					Max:        nil,
					Seq:        seq,
				})
			} else {
				mm, err := utils.GetMinMax(session, db, coll)
				if err != nil {
					return ret, errors.Trace(err)
				}
				ret = append(ret, Chunk{
					Database:   db,
					Collection: coll,
					Min:        &IDValue{Value: mm.Min},
					Max:        &IDValue{Value: mm.Max},
				})
			}
		}
	}
	return ret, nil
}

func getStartPosition(session *mgo.Session) (bson.MongoTimestamp, error) {
	options := gtm.DefaultOptions()
	err := options.Fill(session, "")
	if err != nil {
		return 0, errors.Trace(err)
	}

	startPos, err := gtm.LastOpTimestamp(session, options)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return startPos, nil
}
