package protocol

import (
	"fmt"
	"time"

	"github.com/moiot/gravity/metrics"
)

type MsgTimestamp int

const (
	MsgTime MsgTimestamp = iota
	MsgStartTime
	MsgEnqueueTime
	MsgDequeueTime
	MsgSentTime
)

const (
	MsgDurationStartEnqueue   = "start_enqueue"
	MsgDurationEnqueueDequeue = "enqueue_dequeue"
	MsgDurationDequeueSent    = "dequeue_sent"
	MsgDurationAll            = "all"
)

type KafkaMsgMeta struct {
	PipelineName string
	StartTime    time.Time
	EnqueueTime  time.Time
	DequeueTime  time.Time
	SentTime     time.Time
	KafkaTopic   string
	// KafkaPartitions is an array of partitions, DdlMsg message might be sent to all partitions
	KafkaPartitions []int32
}

type MessageRouter interface {
	GetTopic() string
	GetPartitions() []int32
}

type MessageTracer interface {
	AddTimestamp(t MsgTimestamp)
	AddMetrics()
	MetricsString() string
}

func (m *KafkaMsgMeta) SetPipelineName(name string) {
	m.PipelineName = name
}

func (m *KafkaMsgMeta) SetKafkaTopic(topic string) {
	m.KafkaTopic = topic
}

func (m *KafkaMsgMeta) SetKafkaPartitions(partitions []int32) {
	m.KafkaPartitions = partitions
}

func (m *KafkaMsgMeta) GetTopic() string {
	return m.KafkaTopic
}

func (m *KafkaMsgMeta) GetPartitions() []int32 {
	return m.KafkaPartitions
}

func (m *KafkaMsgMeta) AddTimestamp(t MsgTimestamp) {
	switch t {
	case MsgStartTime:
		m.StartTime = time.Now()
	case MsgEnqueueTime:
		m.EnqueueTime = time.Now()
	case MsgDequeueTime:
		m.DequeueTime = time.Now()
	case MsgSentTime:
		m.SentTime = time.Now()
	}
}

func (m *KafkaMsgMeta) AddMetrics() {
	if m.KafkaTopic == "" {
		return
	}
	metrics.GravityMsgHistogram.WithLabelValues(m.PipelineName, MsgDurationStartEnqueue).Observe(m.EnqueueTime.Sub(m.StartTime).Seconds())
	metrics.GravityMsgHistogram.WithLabelValues(m.PipelineName, MsgDurationEnqueueDequeue).Observe(m.DequeueTime.Sub(m.EnqueueTime).Seconds())
	metrics.GravityMsgHistogram.WithLabelValues(m.PipelineName, MsgDurationDequeueSent).Observe(m.SentTime.Sub(m.DequeueTime).Seconds())
	metrics.GravityMsgHistogram.WithLabelValues(m.PipelineName, MsgDurationAll).Observe(m.SentTime.Sub(m.StartTime).Seconds())
}

func (m *KafkaMsgMeta) MetricsString() string {
	return fmt.Sprintf("Start Time: %v, Enqueue Time: %v, Dequeue Time: %v, Sent Time: %v", m.StartTime, m.EnqueueTime, m.DequeueTime, m.SentTime)
}
