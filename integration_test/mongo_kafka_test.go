package integration_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/gravity"
	gravityConfig "github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/kafka_test"
	"github.com/moiot/gravity/pkg/sarama_cluster"

	"github.com/moiot/gravity/pkg/mongo"

	"github.com/moiot/gravity/pkg/mongo_test"
)

// see https://stackoverflow.com/a/44342358 for mgo and mongo replication init
func initReplica() {
	mongoCfg := mongo_test.TestConfig()
	session, err := mongo.CreateMongoSession(&mongoCfg)
	if err != nil {
		panic(err)
	}

	// Session mode should be monotonic as the default session used by mgo is primary which performs all operations on primary.
	// Since the replica set has not been initialized yet, there wont be a primary and the operation (in this case, replSetInitiate) will just timeout
	session.SetMode(mgo.Monotonic, true)

	result := bson.M{}
	err = session.Run("replSetInitiate", &result)
	if err != nil {
		if (result["codeName"] != "AlreadyInitialized") && result["code"] != 23 {
			panic(err.Error())
		}
	}

	log.Infof("%+v", result)
	session.Close()

	fmt.Println("mongo replSet initialized")
}

type person struct {
	Name string
	Time string
}

func TestMongoJson(t *testing.T) {
	initReplica()

	r := require.New(t)

	mongoCfg := mongo_test.TestConfig()
	kafkaBroker := kafka_test.TestBroker()
	pipelineConfig := gravityConfig.PipelineConfigV2{
		PipelineName: t.Name(),
		InputPlugins: map[string]interface{}{
			"mongooplog": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     mongoCfg.Host,
					"port":     mongoCfg.Port,
					"username": mongoCfg.Username,
					"password": mongoCfg.Password,
					"database": mongoCfg.Database,
				},
			},
		},
		OutputPlugins: map[string]interface{}{
			"async-kafka": map[string]interface{}{
				"kafka-global-config": map[string]interface{}{
					"broker-addrs": kafkaBroker,
					"mode":         "async",
				},
				"routes": []map[string]interface{}{
					{
						"match-schema": t.Name(),
						"match-table":  t.Name(),
						"dml-topic":    t.Name(),
					},
				},
			},
		},
	}

	kafkaConfig := sarama_cluster.NewConfig()
	kafkaConfig.Version = kafka.MsgVersion
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Return.Errors = true

	consumer, err := sarama_cluster.NewConsumer(
		kafkaBroker,
		strconv.Itoa(int(time.Now().Unix())),
		[]string{t.Name()},
		kafkaConfig,
		nil,
	)
	r.NoError(err)

	const operations = 100
	done := make(chan struct{})
	var received []string
	go func() {
		i := 0
		for msg := range consumer.Messages() {
			i = i + 1
			received = append(received, string(msg.Value))
			fmt.Println(string(msg.Value))
			consumer.MarkOffset(msg, "")
			if i == operations {
				break
			}
		}
		close(done)
	}()
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()

	server, err := gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	server.Input.PositionStore().Clear()

	r.NoError(server.Start())

	session, err := mongo.CreateMongoSession(&mongoCfg)
	session.SetMode(mgo.Primary, true)
	r.NoError(err)
	defer session.Close()
	defer session.DB(t.Name()).DropDatabase()

	coll := session.DB(t.Name()).C(t.Name())

	for i := 0; i < operations; i++ {
		r.NoError(coll.Insert(&person{"Foo" + strconv.Itoa(i), time.Now().Format(time.RFC3339)}))
	}

	<-done
	r.NoError(consumer.Close())

	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	r.Len(received, operations)
}
