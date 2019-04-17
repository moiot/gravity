package mongokafka_test

import (
	"encoding/json"
	"fmt"
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

func TestReplication(t *testing.T) {
	r := require.New(t)

	collection := strings.ToLower(t.Name())
	topic := collection

	mongoCfg := mongo_test.TestConfig()
	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.Mongo,
			Mode: config.Replication,
			Config: utils.MustAny2Map(
				mongobatch.Config{
					Source: &mongoCfg,
				},
			),
		},
		OutputPlugin: config.GenericPluginConfig{
			Type: outputs.AsyncKafka,
			Config: utils.MustAny2Map(async_kafka.AsyncKafkaPluginConfig{
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

	operations := int32(100)
	stop := make(chan struct{})
	stopped := make(chan struct{})
	coll := db.C(collection)
	var inserted = make(map[int32]bool)
	for i := int32(0); i < operations; i++ {
		r.NoError(coll.Insert(&bson.M{"name": fmt.Sprint(i)}))
		inserted[i] = true
	}

	go func() {
		defer close(stopped)
		for {
			select {
			case <-stop:
				return

			case <-time.After(10 * time.Millisecond):
				r.NoError(coll.Insert(&bson.M{"name": fmt.Sprint(operations)}))
				inserted[operations] = true
				operations++
			}
		}
	}()

	messageC := newKafkaConsumer(topic)

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())
	for {
		time.Sleep(100 * time.Millisecond)

		if server.Input.Stage() == config.Stream {
			time.Sleep(500 * time.Millisecond)
			break
		}
	}
	close(stop)
	<-stopped
	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	received := make(map[int32]bool)
loop:
	for {
		select {
		case val := <-messageC:
			j := encoding.JsonMsg01{}
			r.NoError(json.Unmarshal([]byte(val), &j))
			r.Equal(encoding.Version01, j.Version)
			r.Equal(db.Name, j.Database)
			r.Equal(collection, j.Collection)
			rec, err := strconv.Atoi(j.Oplog.Data["name"].(string))
			r.NoError(err)
			received[int32(rec)] = true

		case <-time.After(5 * time.Second):
			break loop
		}
	}
	r.Equal(inserted, received)
}
