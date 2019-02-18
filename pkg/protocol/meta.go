package protocol

type MsgTimestamp int

type MessageRouter interface {
	GetTopic() string
	GetPartitions() []int32
}

type MessageTracer interface {
	AddTimestamp(t MsgTimestamp)
	AddMetrics()
	MetricsString() string
}
