package async_kafka

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/core/encoding"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/outputs/routers"
	"github.com/moiot/gravity/pkg/registry"
)

const Name = "async-kafka"

var (
	KafkaMsgSizeGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gravity",
		Subsystem: "output_async_kafka",
		Name:      "binlog_msg_size",
		Help:      "binlog msg size",
	}, []string{metrics.PipelineTag, "topic"})
)

type AsyncKafkaPluginConfig struct {
	KafkaConfig    *config.KafkaGlobalConfig `mapstructure:"kafka-global-config" json:"kafka-global-config"`
	Routes         []map[string]interface{}  `mapstructure:"routes" json:"routes"`
	OutputFormat   string                    `mapstructure:"output-format" json:"output-format"`
	SchemaVersion  string                    `mapstructure:"schema-version" json:"schema-version"`
	IgnoreLargeMsg int                       `mapstructure:"ignore-large-msg" json:"ignore-large-msg"`
}

type AsyncKafka struct {
	pipelineName string

	cfg                *AsyncKafkaPluginConfig
	routes             []*routers.KafkaRoute
	kafkaClient        sarama.Client
	kafkaAsyncProducer *kafka.KafkaAsyncProducer
	msgAcker           core.MsgAcker
	wg                 sync.WaitGroup
	inFlight           sync.WaitGroup

	sync.Mutex
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &AsyncKafka{}, false)

	prometheus.MustRegister(KafkaMsgSizeGaugeVec)
}

func (output *AsyncKafka) Configure(pipelineName string, data map[string]interface{}) error {
	// setup plugin config
	pluginConfig := AsyncKafkaPluginConfig{}

	err := mapstructure.Decode(data, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	if pluginConfig.KafkaConfig == nil {
		return errors.Errorf("empty kafka config")
	}

	if len(pluginConfig.KafkaConfig.BrokerAddrs) == 0 {
		return errors.Errorf("empty kafka broker addrs")
	}

	if pluginConfig.OutputFormat == "" {
		pluginConfig.OutputFormat = "json"
	}

	if pluginConfig.SchemaVersion == "" {
		pluginConfig.SchemaVersion = encoding.Version01
	}

	// setup output
	output.pipelineName = pipelineName
	output.cfg = &pluginConfig

	output.routes, err = routers.NewKafkaRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (output *AsyncKafka) Start(msgAcker core.MsgAcker) error {
	output.msgAcker = msgAcker

	kafkaAsyncProducer, err := kafka.NewKafkaAsyncProducer(output.cfg.KafkaConfig, sarama.NewManualPartitioner)
	if err != nil {
		return errors.Trace(err)
	}
	output.kafkaAsyncProducer = kafkaAsyncProducer

	kafkaClient, err := kafka.NewKafkaClient(output.cfg.KafkaConfig.BrokerAddrs, output.cfg.KafkaConfig.Net)
	if err != nil {
		return errors.Trace(err)
	}
	output.kafkaClient = kafkaClient
	output.wg.Add(1)
	go func() {
		defer output.wg.Done()

		for msg := range output.kafkaAsyncProducer.Successes() {
			topic := msg.Topic
			partition := msg.Partition
			msg, ok := msg.Metadata.(*core.Msg)
			if !ok {
				log.Fatalf("[kafka_target_worker] failed to get meta data")
			}

			if err := output.msgAcker.AckMsg(msg); err != nil {
				log.Fatalf("failed to ack job, err: %v", errors.ErrorStack(err))
			}
			metrics.OutputCounter.WithLabelValues(output.pipelineName, topic, fmt.Sprint(partition), "", "").Add(1)
		}
		log.Infof("[server] kafkaAsync success ack exit")
	}()

	output.wg.Add(1)
	go func() {
		defer output.wg.Done()

		for err := range output.kafkaAsyncProducer.Errors() {
			log.Fatalf("[server] kafka failed at topic: %v, err: %v", err.Msg.Topic, err.Error())
		}

		log.Infof("[server] kafkaAsync err ack exit")
	}()

	return nil
}

func (output *AsyncKafka) GetRouter() core.Router {
	return routers.KafkaRouter(output.routes)
}

func (output *AsyncKafka) Execute(msgs []*core.Msg) error {
	for _, msg := range msgs {
		if msg.Type == core.MsgDDL {
			if err := output.msgAcker.AckMsg(msg); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		matched := false
		var topic string
		for _, route := range output.routes {
			if route.Match(msg) {
				matched = true
				topic = route.DMLTargetTopic
				break
			}
		}

		if !matched {
			if err := output.msgAcker.AckMsg(msg); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		var encoder encoding.Encoder
		if msg.Oplog != nil {
			encoder = encoding.NewEncoder("mongo", output.cfg.OutputFormat)
		} else {
			encoder = encoding.NewEncoder("mysql", output.cfg.OutputFormat)
		}

		b, err := encoder.Serialize(msg, output.cfg.SchemaVersion)
		if err != nil {
			return errors.Trace(err)
		}

		// partition by primary key data by default
		partitions, err := output.kafkaClient.Partitions(topic)
		if err != nil {
			return errors.Annotatef(err, "topic: %v", topic)
		}
		partition := msg.OutputHash() % uint(len(partitions))
		kafkaMsg := sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(b),
			Partition: int32(partition),
			Metadata:  msg,
		}

		size := byteSize(&kafkaMsg)
		if size > 1024*1024 {
			log.Warnf("[output_async_kafka] large msg size %d.", size)
		}
		if output.cfg.IgnoreLargeMsg > 0 && size > output.cfg.IgnoreLargeMsg {
			log.Warnf("[output_async_kafka] ignore msg size %d", size)
			if err := output.msgAcker.AckMsg(msg); err != nil {
				log.Fatalf("failed to ack job, err: %v", errors.ErrorStack(err))
			}
			continue
		}

		KafkaMsgSizeGaugeVec.WithLabelValues(output.pipelineName, topic).Set(float64(size))
		output.kafkaAsyncProducer.Input() <- &kafkaMsg
	}

	return nil
}

func (output *AsyncKafka) Close() {
	log.Infof("[output-async-kafka] closing")
	output.inFlight.Wait()
	output.kafkaAsyncProducer.Close()
	output.kafkaClient.Close()
	output.wg.Wait()
	log.Infof("[output-async-kafka] closed")
}

func byteSize(m *sarama.ProducerMessage) int {
	var size = 26
	if m.Key != nil {
		size += m.Key.Length()
	}
	if m.Value != nil {
		size += m.Value.Length()
	}
	return size
}
