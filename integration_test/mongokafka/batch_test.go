package mongokafka_test

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core/encoding"
	"github.com/moiot/gravity/pkg/inputs"
	"github.com/moiot/gravity/pkg/inputs/mongobatch"
	"github.com/moiot/gravity/pkg/mongo_test"
	"github.com/moiot/gravity/pkg/outputs"
	"github.com/moiot/gravity/pkg/outputs/async_kafka"
	"github.com/moiot/gravity/pkg/utils"
)

func TestChunkedBatch(t *testing.T) {
	r := require.New(t)

	collection := strings.ToLower(t.Name())
	topic := collection

	mongoCfg := mongo_test.TestConfig()
	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.Mongo,
			Mode: config.Batch,
			Config: utils.Struct2Map(
				mongobatch.Config{
					Source:         &mongoCfg,
					ChunkThreshold: 200,
					BatchSize:      100,
				},
			),
		},
		OutputPlugin: config.GenericConfig{
			Type: outputs.AsyncKafka,
			Config: utils.Struct2Map(async_kafka.AsyncKafkaPluginConfig{
				KafkaConfig: &config.KafkaGlobalConfig{
					BrokerAddrs: kafkaBroker,
					Mode:        "async",
				},
				Routes: []map[string]interface{}{
					{
						"match-schema": db.Name,
						"match-table":  collection,
						"dml-topic":    topic,
					},
				},
				OutputFormat:  "json",
				SchemaVersion: encoding.Version01,
			},
			),
		},
	}

	const operations = 500
	coll := db.C(collection)
	for i := 0; i < operations; i++ {
		r.NoError(coll.Insert(&bson.M{"name": "Foo" + strconv.Itoa(i)}))
	}

	messageC := newKafkaConsumer(topic)

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())
	server.Input.Wait()
	server.Close()

	var numbers []int
	for i := 0; i < operations; i++ {
		select {
		case val := <-messageC:
			j := encoding.JsonMsg01{}
			r.NoError(json.Unmarshal([]byte(val), &j))
			r.Equal(encoding.Version01, j.Version)
			r.Equal(db.Name, j.Database)
			r.Equal(collection, j.Collection)
			a := strings.TrimPrefix(j.Oplog.Data["name"].(string), "Foo")
			i, err := strconv.Atoi(a)
			r.NoError(err)
			numbers = append(numbers, i)

		case <-time.After(5 * time.Second):
			log.Panic("time out no message received")
		}
	}
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})
	for i := 0; i < operations; i++ {
		r.Equal(i, numbers[i])
	}

	select {
	case m := <-messageC:
		log.Panicf("receive unexpected msg %s", m)

	case <-time.After(time.Second):
	}
}

func TestNonChunkBatch(t *testing.T) {
	r := require.New(t)

	collection := strings.ToLower(t.Name())
	topic := collection

	mongoCfg := mongo_test.TestConfig()
	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.Mongo,
			Mode: config.Batch,
			Config: utils.Struct2Map(
				mongobatch.Config{
					Source:    &mongoCfg,
					BatchSize: 100,
				},
			),
		},
		OutputPlugin: config.GenericConfig{
			Type: outputs.AsyncKafka,
			Config: utils.Struct2Map(async_kafka.AsyncKafkaPluginConfig{
				KafkaConfig: &config.KafkaGlobalConfig{
					BrokerAddrs: kafkaBroker,
					Mode:        "async",
				},
				Routes: []map[string]interface{}{
					{
						"match-schema": db.Name,
						"match-table":  collection,
						"dml-topic":    topic,
					},
				},
				OutputFormat:  "json",
				SchemaVersion: encoding.Version01,
			},
			),
		},
	}

	const operations = 500
	coll := db.C(collection)
	for i := 0; i < operations; i++ {
		r.NoError(coll.Insert(&bson.M{"name": "Foo" + strconv.Itoa(i)}))
	}

	messageC := newKafkaConsumer(topic)

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())
	server.Input.Wait()
	server.Close()

	var numbers []int
	for i := 0; i < operations; i++ {
		select {
		case val := <-messageC:
			j := encoding.JsonMsg01{}
			r.NoError(json.Unmarshal([]byte(val), &j))
			r.Equal(encoding.Version01, j.Version)
			r.Equal(db.Name, j.Database)
			r.Equal(collection, j.Collection)
			a := strings.TrimPrefix(j.Oplog.Data["name"].(string), "Foo")
			i, err := strconv.Atoi(a)
			r.NoError(err)
			numbers = append(numbers, i)

		case <-time.After(5 * time.Second):
			log.Panic("time out no message received")
		}
	}
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})
	for i := 0; i < operations; i++ {
		r.Equal(i, numbers[i])
	}

	select {
	case m := <-messageC:
		log.Panicf("receive unexpected msg %s", m)

	case <-time.After(time.Second):
	}
}
