package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
)

var MsgVersion = sarama.V0_10_2_0

// createTlsConfiguration creates TLS configuration.
func createTlsConfiguration(certFile string, keyFile string, caFile string, verifySsl bool) (t *tls.Config) {
	if certFile != "" && keyFile != "" && caFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySsl,
		}
	}
	return t
}

type KafkaAsyncProducer struct {
	sarama.AsyncProducer
	successes int
	errors    int
	enqueued  int
}

func NewKafkaAsyncProducer(mqConfig *config.KafkaGlobalConfig, partitionerConstructor sarama.PartitionerConstructor) (*KafkaAsyncProducer, error) {
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration(mqConfig.CertFile, mqConfig.KeyFile, mqConfig.CaFile, mqConfig.VerifySSL)
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	if partitionerConstructor != nil {
		config.Producer.Partitioner = partitionerConstructor
	} else {
		config.Producer.Partitioner = sarama.NewManualPartitioner
	}

	config.ClientID = "drc_gravity"
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	config.Producer.MaxMessageBytes = 1 * 1024 * 1024
	config.Version = MsgVersion

	// Net settings
	if mqConfig.Net != nil {
		config.Net.SASL.Enable = mqConfig.Net.SASL.Enable
		config.Net.SASL.User = mqConfig.Net.SASL.User
		config.Net.SASL.Password = mqConfig.Net.SASL.Password
	}

	// Producer settings
	if mqConfig.Producer != nil {
		config.Producer.Flush.Bytes = mqConfig.Producer.Flush.Bytes
		config.Producer.Flush.Messages = mqConfig.Producer.Flush.Messages
		config.Producer.Flush.MaxMessages = mqConfig.Producer.Flush.MaxMessages
		frequency, err := time.ParseDuration(mqConfig.Producer.Flush.Frequency)
		if err != nil {
			return nil, errors.Errorf("failed to parse frequency")
		}
		config.Producer.Flush.Frequency = frequency
		config.Producer.Retry.Backoff = 2 * time.Second
	}

	producer, err := sarama.NewAsyncProducer(mqConfig.BrokerAddrs, config)
	if err != nil {
		return nil, errors.Annotatef(err, "can't connect to %s", mqConfig.BrokerAddrs)
	}

	p := &KafkaAsyncProducer{AsyncProducer: producer}
	return p, nil
}

func NewKafkaClient(brokers []string, netConfig *config.KafkaNetConfig) (sarama.Client, error) {
	saramaKafkaConfig := sarama.NewConfig()
	saramaKafkaConfig.Version = MsgVersion
	saramaKafkaConfig.Metadata.Full = false
	if netConfig != nil {
		saramaKafkaConfig.Net.SASL.Enable = netConfig.SASL.Enable
		saramaKafkaConfig.Net.SASL.User = netConfig.SASL.User
		saramaKafkaConfig.Net.SASL.Password = netConfig.SASL.Password
	}
	return sarama.NewClient(brokers, saramaKafkaConfig)
}
