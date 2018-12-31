package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	PipelineTag = "pipeline"

	TopicTag     = "topic"
	PartitionTag = "partition"
	stageTag     = "stage"

	PadderTag = "padder"
)

var testCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "test_counter",
	Help: "test_counter",
})

//common
var WaterMarkHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "drc_v2",
	Name:      "watermark_seconds",
	Help:      "Histogram of watermark in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
}, []string{PipelineTag, "type"})

// Gravity metrics
var GravityKafkaEnqueuedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "drc_v2",
	Subsystem: "gravity",
	Name:      "kafka_enqueue",
	Help:      "Number of enqueued message of kafka by topic",
}, []string{PipelineTag, TopicTag})

var GravityKafkaSuccessCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "drc_v2",
	Subsystem: "gravity",
	Name:      "kafka_success",
	Help:      "Number of success message of kafka",
}, []string{PipelineTag, TopicTag})

var GravityKafkaErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "drc_v2",
	Subsystem: "gravity",
	Name:      "kafka_error",
	Help:      "Number of error message of kafka",
}, []string{PipelineTag})

var GravityKafkaPartitionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "partition_counter",
		Help:      "the number of message sent to each partition",
	}, []string{PipelineTag, TopicTag, PartitionTag})

var GravityMsgSentHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "msg_sent_duration_seconds",
		Help:      "Bucketed histogram of processing time (s) of receive message sent success from mq.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag})

var GravitySchedulerEnqueueCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "drc_v2",
	Subsystem: "gravity",
	Name:      "scheduler_enqueue",
	Help:      "Number of enqueued job of scheduler",
}, []string{PipelineTag})

var GravitySchedulerDequeueCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "drc_v2",
	Subsystem: "gravity",
	Name:      "scheduler_dequeue",
	Help:      "Number of dequeued job of scheduler",
}, []string{PipelineTag})

var GravityOplogLagLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "oplog_lag_latency",
		Help:      "oplog lag latency",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag, "source"},
)

var GravityOplogTimeoutCountHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "drc",
	Subsystem: "gravity",
	Name:      "mongo_oplog_timeout_count",
	Help:      "Bucketed histogram of timeout count of mongo oplog",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
}, []string{PipelineTag})

var GravityOplogFetchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "drc",
	Subsystem: "gravity",
	Name:      "mongo_oplog_fetch_duration",
	Help:      "Bucketed histogram of duration of mongo oplog to channel",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
}, []string{PipelineTag})

var (
	GravityPipelineCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "gravity_pipeline_counter",
		Help:      "gravity pipeline counter",
	}, []string{PipelineTag})

	GravityBinlogProbeLagGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "binlog_probe_lag",
		Help:      "Number of offset lag for binlog probe message",
	}, []string{PipelineTag})

	GravityBinlogProbeSentGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "binlog_probe_sent_offset",
		Help:      "binlog probe sent offset",
	}, []string{PipelineTag})

	GravityBinlogProbeReceivedGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "binlog_probe_received_offset",
		Help:      "binlog probe received offset",
	}, []string{PipelineTag})

	GravityBinlogDurationFromGravity = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "binlog_duration_seconds_from_gravity",
			Help:      "Bucketed histogram of two rtt between gravity and source",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 22),
		}, []string{PipelineTag})

	GravityBinlogDurationFromSource = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "binlog_duration_seconds_from_source",
			Help:      "Bucketed histogram of rtt from source to gravity",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 22),
		}, []string{PipelineTag})

	GravityBinlogGTIDGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "gtid",
			Help:      "current transaction id",
		}, []string{PipelineTag, "node"})

	GravityBinlogGTIDLatencyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "gtid_latency",
		Help:      "transaction id latency between gravity and database",
	}, []string{PipelineTag})

	GravityBinlogPosGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{PipelineTag, "node"})

	GravityBinlogFileGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "gravity",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{PipelineTag, "node"})
)

var GravityMsgHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "handle_msg_duration_seconds",
		Help:      "Bucketed histogram of processing time (s) of handled messages for different stages.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag, stageTag},
)

// Nuclear related metrics
var NuclearOffsetLagGaugeVec = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "offset_lag",
		Help:      "current offset lag with high water mark",
	}, []string{PipelineTag, TopicTag, PartitionTag})

var NuclearKafkaMsgCounterVec = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "kafka_msg_count",
		Help:      "kafka msg count consumed by partition",
	}, []string{PipelineTag, TopicTag, PartitionTag})

var NuclearE2eMsgHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "e2e_handle_msg_duration_seconds",
		Help:      "Bucketed histogram of e2e message process time",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag})

var NuclearReceiveMsgHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "receive_msg_duration_seconds",
		Help:      "Bucketed histogram of processing time (s) of receive message from mq.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag})

var NuclearWarningCounterVec = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "warning",
		Help:      "warning needs attention",
	}, []string{PipelineTag, "type"})

var NuclearSqlCommitHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sql_commit_duration",
		Help:      "Bucketed histogram of processing time (s) of sql commit duration",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
	}, []string{PipelineTag})

var NuclearSqlRetriesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sql_retries_total_times",
		Help:      "The total time the the sql retries",
	}, []string{PipelineTag})

var NuclearSqlCommitCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sql_commit_count",
		Help:      "sql commit count",
	}, []string{PipelineTag})

var NuclearBatchCommitSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sql_commit_batch_size",
		Help:      "sql commit batch size",
	}, []string{PipelineTag, "db", "table"})

var NuclearSlidingWindowRatio = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sliding_window_ratio",
		Help:      "sliding window ratio",
	}, []string{PipelineTag, "topic", "partition"})

var NuclearSlidingWindowSize = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "nuclear",
		Name:      "sliding_window_size",
		Help:      "sliding window ratio",
	}, []string{PipelineTag, "topic", "partition"})

// scanner metrics
var (
	ScannerBatchQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "scanner",
			Name:      "batch_query_duration",
			Help:      "bucketed histogram of batch fetch duration time",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	ScannerSingleScanDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "scanner",
			Name:      "single_scan_duration",
			Help:      "bucketed histogram of single scan duration",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	ScannerSendJobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "drc_v2",
			Subsystem: "scanner",
			Name:      "send_job_duration",
			Help:      "bucketed histogram of send job duration",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{PipelineTag})

	ScannerQueueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "scanner",
			Name:      "queue_length",
			Help:      "queue length",
		}, []string{PipelineTag})

	ScannerJobFetchedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "scanner",
		Name:      "job_fetched_count",
		Help:      "Number of data rows fetched by scanner",
	}, []string{PipelineTag})

	ScannerKafkaEnqueuedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "scanner",
		Name:      "kafka_enqueue",
		Help:      "Number of enqueued message of kafka by topic",
	}, []string{PipelineTag, TopicTag})

	ScannerKafkaSuccessCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "scanner",
		Name:      "kafka_success",
		Help:      "Number of success message of kafka",
	}, []string{PipelineTag, TopicTag})

	ScannerKafkaPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "drc_v2",
			Subsystem: "scanner",
			Name:      "partition_counter",
			Help:      "the number of message sent to each partition",
		}, []string{PipelineTag, TopicTag, PartitionTag})
)

func init() {
	prometheus.MustRegister(testCounter)

	prometheus.MustRegister(WaterMarkHistogram)

	prometheus.MustRegister(GravityKafkaEnqueuedCount)
	prometheus.MustRegister(GravityKafkaSuccessCount)
	prometheus.MustRegister(GravityKafkaErrorCount)
	prometheus.MustRegister(GravityKafkaPartitionCounter)
	prometheus.MustRegister(GravityMsgSentHistogram)
	prometheus.MustRegister(GravitySchedulerEnqueueCount)
	prometheus.MustRegister(GravitySchedulerDequeueCount)
	prometheus.MustRegister(GravityOplogLagLatency)
	prometheus.MustRegister(GravityPipelineCounter)
	prometheus.MustRegister(GravityBinlogProbeLagGauge)
	prometheus.MustRegister(GravityBinlogGTIDGaugeVec)
	prometheus.MustRegister(GravityBinlogGTIDLatencyGauge)
	prometheus.MustRegister(GravityBinlogProbeSentGauge)
	prometheus.MustRegister(GravityBinlogProbeReceivedGauge)
	prometheus.MustRegister(GravityBinlogDurationFromGravity)
	prometheus.MustRegister(GravityBinlogDurationFromSource)
	prometheus.MustRegister(GravityBinlogPosGaugeVec)
	prometheus.MustRegister(GravityBinlogFileGaugeVec)
	prometheus.MustRegister(GravityOplogTimeoutCountHistogram)
	prometheus.MustRegister(GravityOplogFetchDuration)
	prometheus.MustRegister(GravityMsgHistogram)

	prometheus.MustRegister(ScannerBatchQueryDuration)
	prometheus.MustRegister(ScannerSingleScanDuration)
	prometheus.MustRegister(ScannerSendJobDuration)
	prometheus.MustRegister(ScannerQueueLength)
	prometheus.MustRegister(ScannerKafkaEnqueuedCount)
	prometheus.MustRegister(ScannerKafkaSuccessCount)
	prometheus.MustRegister(ScannerKafkaPartitionCounter)
	prometheus.MustRegister(ScannerJobFetchedCount)
}
