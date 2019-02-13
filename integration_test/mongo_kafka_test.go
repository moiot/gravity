package integration_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/app"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	mgo "gopkg.in/mgo.v2"

	gravityConfig "github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/kafka_test"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo_test"
	"github.com/moiot/gravity/pkg/sarama_cluster"
)

type person struct {
	Name string
	Time string
}

func TestMongoJson(t *testing.T) {
	mongo_test.InitReplica()

	r := require.New(t)

	mongoCfg := mongo_test.TestConfig()
	kafkaBroker := kafka_test.TestBroker()
	pipelineConfig := gravityConfig.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      gravityConfig.PipelineConfigV3Version,
		InputPlugin: gravityConfig.InputConfig{
			Type: "mongo",
			Mode: "stream",
			Config: map[string]interface{}{
				"source": map[string]interface{}{
					"host":     mongoCfg.Host,
					"port":     mongoCfg.Port,
					"username": mongoCfg.Username,
					"password": mongoCfg.Password,
					"database": mongoCfg.Database,
				},
			},
		},
		OutputPlugin: gravityConfig.GenericConfig{
			Type: "async-kafka",
			Config: map[string]interface{}{
				"kafka-global-config": map[string]interface{}{
					"broker-addrs": kafkaBroker,
					"mode":         "async"},
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
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				received = append(received, string(msg.Value))
				fmt.Println(string(msg.Value))
				consumer.MarkOffset(msg, "")
			}

		case <-time.After(time.Second):
		}
		close(done)
	}()
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)

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

	// should ignore
	coll = session.DB(t.Name()).C(t.Name() + "_2")
	r.NoError(coll.Insert(&person{"Foo", time.Now().Format(time.RFC3339)}))

	<-done
	r.NoError(consumer.Close())

	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	r.Len(received, operations)
}
