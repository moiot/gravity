package mongokafka_test

import (
	"os"
	"testing"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/Shopify/sarama"

	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/kafka_test"
	"github.com/moiot/gravity/pkg/sarama_cluster"

	mgo "gopkg.in/mgo.v2"

	"github.com/moiot/gravity/pkg/mongo_test"
)

var session *mgo.Session
var db *mgo.Database
var kafkaBroker []string
var kafkaAdmin sarama.ClusterAdmin

func TestMain(m *testing.M) {
	mongoCfg := mongo_test.TestConfig()
	s, err := utils.CreateMongoSession(&mongoCfg)
	if err != nil {
		panic(err)
	}
	session = s
	mongo_test.InitReplica(session)
	defer session.Close()

	db = s.DB("mongokafka")
	if err := db.DropDatabase(); err != nil {
		panic(err)
	}
	if err := s.DB(consts.GravityDBName).DropDatabase(); err != nil {
		panic(err)
	}

	kafkaBroker = kafka_test.TestBroker()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	kafkaAdmin, err = sarama.NewClusterAdmin(kafkaBroker, cfg)
	if err != nil {
		panic(err)
	}

	ret := m.Run()

	if err := kafkaAdmin.Close(); err != nil {
		panic(err)
	}
	for _, c := range consumers {
		if err = c.Close(); err != nil {
			panic(err)
		}
	}
	os.Exit(ret)
}

var consumers []*sarama_cluster.Consumer

func newKafkaConsumer(topic string) chan string {
	err := kafkaAdmin.DeleteTopic(topic)
	if err != nil && err != sarama.ErrUnknownTopicOrPartition {
		panic(err)
	}

	kafkaConfig := sarama_cluster.NewConfig()
	kafkaConfig.Version = kafka.MsgVersion
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Return.Errors = true

	consumer, err := sarama_cluster.NewConsumer(
		kafkaBroker,
		topic,
		[]string{topic},
		kafkaConfig,
		nil,
	)
	if err != nil {
		panic(err)
	}
	received := make(chan string, 100)
	go func() {
		for msg := range consumer.Messages() {
			received <- string(msg.Value)
			consumer.MarkOffset(msg, "")
		}
	}()
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()
	consumers = append(consumers, consumer)
	return received
}
