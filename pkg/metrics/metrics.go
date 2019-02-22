package metrics

import "github.com/prometheus/client_golang/prometheus"

//(counter)input ->(counter, latency) emitter (counter, latency)-> scheduler (counter, submit latency, ack latency)-> output(counter, latency)
// e2e process time, event time latency

const (
	PipelineTag = "pipeline"
)

var ProbeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "input",
	Name:      "probe_latency",
	Help:      "Latency of input probe in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22), // ~ 17min
}, []string{PipelineTag})

var InputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "input",
	Name:      "counter",
	Help:      "Number of message input received(generated)",
}, []string{PipelineTag, "db", "table", "type", "subtype"})

var Input2EmitterCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "input",
	Name:      "emitter_counter",
	Help:      "Number of message input sends to emitter",
}, []string{PipelineTag})

var InputHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "input",
	Name:      "latency",
	Help:      "Latency of input in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 15), // ~ 8s
}, []string{PipelineTag})

var Emitter2SchedulerCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "emitter",
	Name:      "scheduler_counter",
	Help:      "Number of message emitter sends to scheduler",
}, []string{PipelineTag})

var EmitterHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "emitter",
	Name:      "latency",
	Help:      "Latency of emitter in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 15), // ~ 8s
}, []string{PipelineTag})

var Scheduler2OutputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "scheduler",
	Name:      "output_counter",
	Help:      "Number of message scheduler sends to output",
}, []string{PipelineTag})

var SchedulerTotalHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "scheduler",
	Name:      "total_latency",
	Help:      "Latency of scheduler from the beginning of submit to the end of ack in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 15), // ~ 8s
}, []string{PipelineTag})

var SchedulerSubmitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "scheduler",
	Name:      "submit_latency",
	Help:      "Latency of scheduler submit phrase in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 15), // ~ 8s
}, []string{PipelineTag})

var SchedulerAckHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "scheduler",
	Name:      "ack_latency",
	Help:      "Latency of scheduler ack phrase in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 15), // ~ 8s
}, []string{PipelineTag})

var OutputCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gravity",
	Subsystem: "output",
	Name:      "counter",
	Help:      "Number of message output sends",
}, []string{PipelineTag, "cat0", "cat1", "cat2", "cat3"})

var OutputHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Subsystem: "output",
	Name:      "latency",
	Help:      "Latency of output in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18), // ~ 65s
}, []string{PipelineTag})

var End2EndEventTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Name:      "event_time_latency",
	Help:      "Latency of end to end event time in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 30), // ~ 74h
}, []string{PipelineTag})

var End2EndProcessTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "gravity",
	Name:      "process_time_latency",
	Help:      "Latency of end to end process time in seconds.",
	Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 19), // ~ 2min
}, []string{PipelineTag})

var QueueLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "gravity",
	Name:      "queue_length",
	Help:      "Length of specific queue.",
}, []string{PipelineTag, "type", "subtype"})

func init() {
	prometheus.MustRegister(
		ProbeHistogram,
		InputCounter, Input2EmitterCounter, InputHistogram,
		Emitter2SchedulerCounter, EmitterHistogram,
		Scheduler2OutputCounter, SchedulerTotalHistogram, SchedulerSubmitHistogram, SchedulerAckHistogram,
		OutputCounter, OutputHistogram,
		End2EndEventTimeHistogram, End2EndProcessTimeHistogram,
		QueueLength,
	)
}
