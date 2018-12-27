package async_kafka

import (
	"fmt"

	"github.com/moiot/gravity/pkg/core/encoding"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/gravity/outputs/routers"
	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/metrics"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/utils"

	"sync"

	"github.com/mitchellh/mapstructure"
)

const AsyncKafkaPluginName = "async-kafka"

var (
	KafkaMsgSizeGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "output_async_kafka",
			Name:      "binlog_msg_size",
			Help:      "binlog msg size",
		}, []string{metrics.PipelineTag, metrics.TopicTag},
	)

	KafkaEnqueuedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "output_async_kafka",
		Name:      "kafka_enqueue",
		Help:      "Number of enqueued message of kafka by topic",
	}, []string{metrics.PipelineTag, metrics.TopicTag})

	KafkaSuccessCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "output_async_kafka",
		Name:      "kafka_success",
		Help:      "Number of success message of kafka",
	}, []string{metrics.PipelineTag, metrics.TopicTag})

	KafkaPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "drc_v2",
			Subsystem: "output_async_kafka",
			Name:      "partition_counter",
			Help:      "the number of message sent to each partition",
		}, []string{metrics.PipelineTag, metrics.TopicTag, metrics.PartitionTag})
)

type AsyncKafkaPluginConfig struct {
	KafkaConfig   *config.KafkaGlobalConfig `mapstructure:"kafka-global-config"`
	Routes        []map[string]interface{}  `mapstructure:"routes"`
	OutputFormat  string                    `mapstructure:"output-format"`
	SchemaVersion string                    `mapstructure:"schema-version"`
}

type AsyncKafka struct {
	pipelineName string

	cfg                *AsyncKafkaPluginConfig
	routes             []*routers.KafkaRoute
	kafkaClient        sarama.Client
	kafkaAsyncProducer *kafka.KafkaAsyncProducer
	msgAcker           core.MsgAcker
	wg                 sync.WaitGroup

	msgSet map[int64]*core.Msg
	sync.Mutex
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, AsyncKafkaPluginName, &AsyncKafka{}, false)

	prometheus.MustRegister(KafkaMsgSizeGaugeVec)
	prometheus.MustRegister(KafkaEnqueuedCount)
	prometheus.MustRegister(KafkaSuccessCount)
	prometheus.MustRegister(KafkaPartitionCounter)
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
		pluginConfig.SchemaVersion = encoding.Version20Alpha
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
	output.msgSet = make(map[int64]*core.Msg)
	output.wg.Add(1)
	go func() {
		defer output.wg.Done()

		for msg := range output.kafkaAsyncProducer.Successes() {
			topic := msg.Topic
			partition := msg.Partition
			meta, ok := msg.Metadata.(kafka.KafkaMetaData)
			if !ok {
				log.Fatalf("[kafka_target_worker] failed to get meta data")
			}

			msg, err := output.delMsgSet(meta.InputSequenceNumber)
			if err != nil {
				log.Fatalf("failed to delete job, err: %v", errors.ErrorStack(err))
			}

			if err := output.msgAcker.AckMsg(msg); err != nil {
				log.Fatalf("failed to ack job, err: %v", errors.ErrorStack(err))
			}

			KafkaSuccessCount.WithLabelValues(output.pipelineName, topic).Add(1)
			KafkaPartitionCounter.WithLabelValues(output.pipelineName, topic, fmt.Sprintf("%v", partition)).Add(1)
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

func (output *AsyncKafka) Execute(msgs []*core.Msg) error {
	for _, msg := range msgs {
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
			output.msgAcker.AckMsg(msg)
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
		partition := int(utils.GenHashKey(*msg.OutputStreamKey)) % len(partitions)
		metadata := kafka.KafkaMetaData{
			InputSequenceNumber: *msg.InputSequence,
			InputStreamKey:      *msg.InputStreamKey,
		}
		kafkaMsg := sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(b),
			Partition: int32(partition),
			Metadata:  metadata,
		}

		size := byteSize(&kafkaMsg)
		if size > 1024*1024 {
			log.Warnf("[output_async_kafka] large msg size %d.", size)
		}

		KafkaMsgSizeGaugeVec.WithLabelValues(output.pipelineName, topic).Set(float64(size))
		output.addMsgSet(msg)
		output.kafkaAsyncProducer.Input() <- &kafkaMsg

		KafkaEnqueuedCount.WithLabelValues(output.pipelineName, topic).Add(1)
	}

	return nil
}

func (output *AsyncKafka) Close() {
	log.Infof("[output-async-kafka] closing")
	output.kafkaAsyncProducer.Close()
	output.kafkaClient.Close()
	output.wg.Wait()
	log.Infof("[output-async-kafka] closed")
}

func (output *AsyncKafka) addMsgSet(msg *core.Msg) {
	output.Lock()
	defer output.Unlock()

	output.msgSet[*msg.InputSequence] = msg
}

func (output *AsyncKafka) delMsgSet(seq int64) (*core.Msg, error) {
	output.Lock()
	defer output.Unlock()
	msg, ok := output.msgSet[seq]
	if !ok {
		return nil, errors.Errorf("job not in set, seq: %d", seq)
	}

	delete(output.msgSet, seq)
	return msg, nil
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
