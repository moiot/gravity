package metrics

import (
	"github.com/moiot/gravity/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	WorkerPoolWorkerCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "scheduler_worker_pool",
		Name:      "nr_worker",
		Help:      "number of workers",
	}, []string{metrics.PipelineTag})

	WorkerPoolQueueSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "scheduler_worker_pool",
		Name:      "queue_size",
		Help:      "size of queue",
	}, []string{metrics.PipelineTag, "idx"})

	WorkerPoolProcessedMsgCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "scheduler_worker_pool",
		Name:      "processed_msg_count",
		Help:      "processed msg count of this worker",
	}, []string{metrics.PipelineTag, "idx"})

	WorkerPoolMsgExecLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "drc_v2",
		Subsystem: "scheduler_worker_pool",
		Name:      "msg_exec_latency",
		Help:      "latency of process a job",
	}, []string{metrics.PipelineTag, "idx"})

	//
	// batch scheduler specific metrics
	//
	WorkerPoolJobBatchSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "scheduler_worker_pool",
		Name:      "batch_size",
		Help:      "batch size",
	}, []string{metrics.PipelineTag, "idx"})

	WorkerPoolSlidingWindowSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "scheduler_worker_pool",
			Name:      "sliding_window_size",
			Help:      "sliding window size",
		}, []string{metrics.PipelineTag, "input_stream_key"})

	WorkerPoolSlidingWindowRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "drc_v2",
			Subsystem: "scheduler_worker_pool",
			Name:      "sliding_window_ratio",
			Help:      "sliding window ratio",
		}, []string{metrics.PipelineTag, "input_stream_key"})
)

func init() {
	prometheus.MustRegister(WorkerPoolWorkerCountGauge)
	prometheus.MustRegister(WorkerPoolQueueSizeGauge)
	prometheus.MustRegister(WorkerPoolProcessedMsgCount)
	prometheus.MustRegister(WorkerPoolMsgExecLatency)
	prometheus.MustRegister(WorkerPoolJobBatchSizeGauge)
	prometheus.MustRegister(WorkerPoolSlidingWindowSize)
	prometheus.MustRegister(WorkerPoolSlidingWindowRatio)
}
