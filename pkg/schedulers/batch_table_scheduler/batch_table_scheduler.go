package batch_table_scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/sliding_window"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/utils/retry"
)

const (
	DefaultNrRetries        = 3
	DefaultRetrySleepString = "1s"
	DefaultRetrySleep       = time.Second
)

var DefaultConfig = map[string]interface{}{
	"nr-worker":           100,
	"batch-size":          1,
	"queue-size":          1024,
	"sliding-window-size": 1024 * 10,
	"nr-retries":          DefaultNrRetries,
	"retry-sleep":         DefaultRetrySleepString,
}

var (
	WorkerPoolWorkerCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gravity",
		Subsystem: "scheduler",
		Name:      "nr_worker",
		Help:      "number of workers",
	}, []string{metrics.PipelineTag})

	WorkerPoolJobBatchSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gravity",
		Subsystem: "scheduler",
		Name:      "batch_size",
		Help:      "batch size",
	}, []string{metrics.PipelineTag, "idx"})

	WorkerPoolSlidingWindowRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gravity",
		Subsystem: "scheduler",
		Name:      "sliding_window_ratio",
		Help:      "sliding window ratio",
	}, []string{metrics.PipelineTag, "input_stream_key"})
)

// batch_scheduler package implements scheduler that dispatch job to workers that
// can handle batch of job.
//
// Concurrency of job processing happens at two level:
//  1. table level. Different table's job can be processed concurrently
//  2. row level. Different row inside a table can be processed concurrently.
//
// Our goal is to achieve high concurrency, and also keep SAFETY property for a single row:
//  ONE ROW'S DmlMsg(INSERT/UPDATE/DELETE) INSIDE A TABLE MUST HAPPENS WITH THE SAME
// SEQUENCE AS THE EVENT SOURCE.
//
//

const BatchTableSchedulerName = "batch-table-scheduler"

type BatchSchedulerConfig struct {
	NrWorker          int           `mapstructure:"nr-worker"json:"nr-worker"`
	MaxBatchPerWorker int           `mapstructure:"batch-size"json:"batch-size"`
	QueueSize         int           `mapstructure:"queue-size"json:"queue-size"`
	SlidingWindowSize int           `mapstructure:"sliding-window-size"json:"sliding-window-size"`
	NrRetries         int           `mapstructure:"nr-retries" json:"nr-retries"`
	RetrySleepString  string        `mapstructure:"retry-sleep" json:"retry-sleep"`
	HealthyThreshold  int           `mapstructure:"healthy-threshold" json:"healthy-threshold"`
	RetrySleep        time.Duration `mapstructure:"-" json:"-"`
}

type batchScheduler struct {
	pipelineName string
	cfg          *BatchSchedulerConfig

	syncOutput  core.SynchronousOutput
	asyncOutput core.AsynchronousOutput

	slidingWindows map[string]sliding_window.Window
	windowMutex    sync.Mutex

	buffersMutex sync.Mutex
	buffers      map[string]chan *core.Msg

	bufferWg sync.WaitGroup

	workerQueues []chan []*core.Msg
	workerWg     sync.WaitGroup

	workingSet *workingSet
}

// batch-table-scheduler makes sure:
// - core.Msg with the same table name is processed by the same worker

func init() {
	registry.RegisterPlugin(registry.SchedulerPlugin, BatchTableSchedulerName, &batchScheduler{}, false)

	prometheus.MustRegister(WorkerPoolWorkerCountGauge)
	prometheus.MustRegister(WorkerPoolJobBatchSizeGauge)
	prometheus.MustRegister(WorkerPoolSlidingWindowRatio)
}

func (scheduler *batchScheduler) Configure(pipelineName string, configData map[string]interface{}) error {
	scheduler.pipelineName = pipelineName

	schedulerConfig := BatchSchedulerConfig{}
	err := mapstructure.Decode(configData, &schedulerConfig)
	if err != nil {
		return errors.Trace(err)
	}

	if schedulerConfig.NrRetries <= 0 {
		schedulerConfig.NrRetries = DefaultNrRetries
	}

	if schedulerConfig.RetrySleepString != "" {
		d, err := time.ParseDuration(schedulerConfig.RetrySleepString)
		if err != nil {
			return errors.Trace(err)
		}
		schedulerConfig.RetrySleep = d
	} else {
		schedulerConfig.RetrySleep = DefaultRetrySleep
	}

	if schedulerConfig.HealthyThreshold > 0 {
		sliding_window.DefaultHealthyThreshold = float64(schedulerConfig.HealthyThreshold)
	}

	scheduler.cfg = &schedulerConfig

	WorkerPoolWorkerCountGauge.WithLabelValues(pipelineName).Set(float64(schedulerConfig.NrWorker))

	log.Infof("[batchScheduler] %+v", schedulerConfig)
	return nil
}

func (scheduler *batchScheduler) Healthy() bool {
	if scheduler.cfg.SlidingWindowSize == 0 {
		return true
	}

	scheduler.windowMutex.Lock()
	defer scheduler.windowMutex.Unlock()
	for k, w := range scheduler.slidingWindows {
		watermark := w.Watermark()
		if !watermark.Healthy() {
			log.Warnf("[batchScheduler.Healthy] sliding window %s not healthy, watermark: %s", k, watermark)
			return false
		}
	}
	return true
}

func (scheduler *batchScheduler) Start(output core.Output) error {
	if scheduler.cfg.MaxBatchPerWorker > config.TxnBufferLimit-1 {
		return errors.Errorf("max batch per worker exceed txn buffer limit")
	}

	if scheduler.cfg.NrWorker <= 0 {
		return errors.Errorf("# of worker is 0, config: %#v", scheduler.cfg)
	}

	scheduler.slidingWindows = make(map[string]sliding_window.Window)

	switch o := output.(type) {
	case core.SynchronousOutput:
		scheduler.syncOutput = o
	case core.AsynchronousOutput:
		scheduler.asyncOutput = o
	default:
		return errors.Errorf("invalid output type")
	}

	scheduler.workerQueues = make([]chan []*core.Msg, scheduler.cfg.NrWorker)
	for i := 0; i < scheduler.cfg.NrWorker; i++ {
		scheduler.workerQueues[i] = make(chan []*core.Msg, scheduler.cfg.QueueSize)
		scheduler.workerWg.Add(1)
		go func(q chan []*core.Msg, workerIndex string) {
			defer scheduler.workerWg.Done()

			for msgBatch := range q {
				metrics.QueueLength.WithLabelValues(scheduler.pipelineName, "worker", workerIndex).Set(float64(len(q)))
				WorkerPoolJobBatchSizeGauge.WithLabelValues(scheduler.pipelineName, workerIndex).Set(float64(len(msgBatch)))
				metrics.Scheduler2OutputCounter.WithLabelValues(core.PipelineName).Add(float64(len(msgBatch)))
				now := time.Now()
				for _, m := range msgBatch {
					m.EnterOutput = now
				}
				if scheduler.syncOutput != nil {
					err := retry.Do(func() error {
						return scheduler.syncOutput.Execute(msgBatch)
					}, scheduler.cfg.NrRetries, scheduler.cfg.RetrySleep)

					if err != nil {
						log.Fatalf("[batchScheduler] output exec error: %v", errors.ErrorStack(err))
					}

					// synchronous output should ack msg here.
					for _, msg := range msgBatch {
						if err := scheduler.AckMsg(msg); err != nil {
							log.Fatalf("[batchScheduler] err: %v", errors.ErrorStack(err))
						}

					}
				} else if scheduler.asyncOutput != nil {
					if err := scheduler.asyncOutput.Execute(msgBatch); err != nil {
						log.Fatalf("[batchScheduler] err: %v", errors.ErrorStack(err))
					}

				}
			}

		}(scheduler.workerQueues[i], fmt.Sprintf("%d", i))
	}

	scheduler.buffers = make(map[string]chan *core.Msg)
	scheduler.workingSet = newWorkingSet()
	return nil
}

func (scheduler *batchScheduler) SubmitMsg(msg *core.Msg) error {
	msg.EnterScheduler = time.Now()
	if msg.Type == core.MsgDML && msg.Table == "" {
		return errors.Errorf("batch-table-scheduler requires a valid table name for dml msg")
	}

	if scheduler.cfg.SlidingWindowSize > 0 {
		key := *msg.InputStreamKey
		scheduler.windowMutex.Lock()
		window, ok := scheduler.slidingWindows[key]
		if !ok {
			window = sliding_window.NewStaticSlidingWindow(scheduler.cfg.SlidingWindowSize)
			scheduler.slidingWindows[key] = window
			log.Infof("[batchScheduler.SubmitMsg] added new sliding window, key=%s, type=%s", key, msg.Type)
			if msg.DdlMsg != nil {
				log.Infof("[batchScheduler.SubmitMsg] added new sliding window, ddl: %v", msg.DdlMsg.Statement)
			}

		} else {
			// Do not process MsgCloseInputStream, just close the sliding window
			if msg.Type == core.MsgCloseInputStream {
				delete(scheduler.slidingWindows, key)
				log.Infof("[batchScheduler.SubmitMsg] deleted new sliding window, key=%s", key)
				scheduler.windowMutex.Unlock()
				window.Close() // should release lock first, otherwise AckMsg might wait on the lock, lead to dead lock.
				close(msg.Done)
				log.Infof("[batchScheduler.SubmitMsg] closed stream: %v", key)
				return nil
			}
		}
		scheduler.windowMutex.Unlock()

		window.AddWindowItem(msg)
	}

	err := scheduler.dispatchMsg(msg, scheduler.cfg.MaxBatchPerWorker, scheduler.cfg.NrWorker)
	msg.LeaveSubmitter = time.Now()
	metrics.SchedulerSubmitHistogram.WithLabelValues(core.PipelineName).Observe(msg.LeaveSubmitter.Sub(msg.EnterScheduler).Seconds())
	return err
}

func (scheduler *batchScheduler) AckMsg(msg *core.Msg) error {
	msg.EnterAcker = time.Now()
	if msg.Type != core.MsgCtl { // control msg doesn't enter output
		metrics.OutputHistogram.WithLabelValues(core.PipelineName).Observe(msg.EnterAcker.Sub(msg.EnterOutput).Seconds())
	}

	if scheduler.cfg.MaxBatchPerWorker > 1 {
		k := workingSetKey(msg.Table, *msg.OutputStreamKey)
		if err := scheduler.workingSet.ack(k); err != nil {
			return errors.Trace(err)
		}
	}

	if scheduler.cfg.SlidingWindowSize > 0 {

		scheduler.windowMutex.Lock()
		window := scheduler.slidingWindows[*msg.InputStreamKey]
		scheduler.windowMutex.Unlock()

		window.AckWindowItem(msg.SequenceNumber())

		metrics.QueueLength.WithLabelValues(scheduler.pipelineName, "sliding-window", *msg.InputStreamKey).Set(float64(window.WaitingQueueLen()))
		WorkerPoolSlidingWindowRatio.WithLabelValues(scheduler.pipelineName, *msg.InputStreamKey).Set(float64(window.WaitingQueueLen() / window.Size()))
	}

	msg.LeaveScheduler = time.Now()
	metrics.SchedulerTotalHistogram.WithLabelValues(core.PipelineName).Observe(msg.LeaveScheduler.Sub(msg.EnterScheduler).Seconds())
	metrics.SchedulerAckHistogram.WithLabelValues(core.PipelineName).Observe(msg.LeaveScheduler.Sub(msg.EnterAcker).Seconds())
	return nil
}

func (scheduler *batchScheduler) Close() {
	// log.Infof("[batchScheduler] closing scheduler")
	scheduler.buffersMutex.Lock()
	for _, c := range scheduler.buffers {
		close(c)
	}
	scheduler.buffersMutex.Unlock()
	scheduler.bufferWg.Wait()

	log.Infof("[batchScheduler] buffers dispatcher closed")

	for _, q := range scheduler.workerQueues {
		close(q)
	}
	scheduler.workerWg.Wait()

	log.Infof("[batchScheduler] worker closed")

	for _, w := range scheduler.slidingWindows {
		w.Close()
	}

	log.Infof("[batchScheduler] sliding window closed")
}

func workingSetKey(tableKey string, workerKey string) string {
	return fmt.Sprintf("%s_%s", tableKey, workerKey)
}

func (scheduler *batchScheduler) dispatchMsg(msg *core.Msg, maxBatchPerWorker int, nrWorkers int) error {
	scheduler.buffersMutex.Lock()
	defer scheduler.buffersMutex.Unlock()

	tableKey := msg.Table

	_, ok := scheduler.buffers[tableKey]
	if ok {
		scheduler.buffers[tableKey] <- msg
		return nil
	}

	if len(scheduler.buffers) > 5000 {
		return errors.Errorf("max table reached")
	}

	scheduler.buffers[tableKey] = make(chan *core.Msg, maxBatchPerWorker*10)

	scheduler.bufferWg.Add(1)

	go func(c chan *core.Msg, key string) {
		defer scheduler.bufferWg.Done()

		var batch []*core.Msg
		for msg := range c {
			metrics.QueueLength.WithLabelValues(core.PipelineName, "table-buffer", key).Set(float64(len(c)))
			batch = append(batch, msg)
			if scheduler.needFlush(c, batch, maxBatchPerWorker) {
				// if we found any job in the working set, we need to wait.
				// note that we need to do this in batch, otherwise there might be deadlock,
				// since jobs are put into worker by batch
				if maxBatchPerWorker > 1 {
					workingSetBatchKeys := make([]string, len(batch))
					for i, item := range batch {
						workingSetBatchKeys[i] = workingSetKey(item.Table, *item.OutputStreamKey)
					}
					// log.Printf("workingSet batch keys: %v\n", workingSetBatchKeys)
					scheduler.workingSet.checkAndPutBatch(workingSetBatchKeys)
				}
				var realBatch []*core.Msg
				for _, msgInBatch := range batch {
					if msgInBatch.Type == core.MsgCtl {
						err := scheduler.AckMsg(msgInBatch)
						if err != nil {
							log.Fatalf("[batch_scheduler] failed to ack slide window, SequenceNumber: %v, err: %v",
								*msgInBatch.InputSequence, errors.ErrorStack(err))
						}
					} else {
						realBatch = append(realBatch, msgInBatch)
					}
				}
				// queueIdx is calculated from the last job's worker key in the batch
				queueIdx := int(utils.GenHashKey(*msg.OutputStreamKey)) % nrWorkers

				batchLen := len(realBatch)
				theBatch := make([]*core.Msg, batchLen)
				copy(theBatch, realBatch)

				scheduler.workerQueues[queueIdx] <- theBatch
				batch = nil
				realBatch = nil
			}

		}
	}(scheduler.buffers[tableKey], tableKey)

	scheduler.buffers[tableKey] <- msg

	return nil
}

func (scheduler *batchScheduler) needFlush(q chan *core.Msg, batch []*core.Msg, maxBatchSize int) bool {
	if len(batch) == maxBatchSize || len(q) == 0 {
		return true
	} else {
		return false
	}
}
