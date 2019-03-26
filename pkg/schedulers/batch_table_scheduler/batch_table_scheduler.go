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
	"nr-worker":           10,
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

	tableBuffersMutex sync.Mutex
	tableBuffers      map[string]chan *core.Msg
	latchC            map[string]chan uint
	bufferWg          sync.WaitGroup
	latchWg           sync.WaitGroup

	workerQueues []chan []*core.Msg
	workerWg     sync.WaitGroup
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

	scheduler.tableBuffers = make(map[string]chan *core.Msg)
	scheduler.latchC = make(map[string]chan uint)
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
	var err error
	if msg.Type == core.MsgCtl {
		msg.LeaveSubmitter = time.Now()
		err = scheduler.AckMsg(msg)
	} else {
		err = scheduler.dispatchMsg(msg)
		msg.LeaveSubmitter = time.Now()
	}
	metrics.SchedulerSubmitHistogram.WithLabelValues(core.PipelineName).Observe(msg.LeaveSubmitter.Sub(msg.EnterScheduler).Seconds())
	return err
}

func (scheduler *batchScheduler) AckMsg(msg *core.Msg) error {
	msg.EnterAcker = time.Now()
	if msg.Type != core.MsgCtl { // control msg doesn't enter output
		metrics.OutputHistogram.WithLabelValues(core.PipelineName).Observe(msg.EnterAcker.Sub(msg.EnterOutput).Seconds())
	}

	if msg.AfterAckCallback != nil {
		err := msg.AfterAckCallback()
		if err != nil {
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
	log.Infof("[batchScheduler] closing scheduler")

	for _, w := range scheduler.slidingWindows {
		w.Close()
	}
	log.Infof("[batchScheduler] sliding window closed")

	scheduler.tableBuffersMutex.Lock()
	for _, c := range scheduler.tableBuffers {
		close(c)
	}
	scheduler.tableBuffersMutex.Unlock()
	scheduler.bufferWg.Wait()

	log.Infof("[batchScheduler] table buffer closed")

	for _, q := range scheduler.workerQueues {
		close(q)
	}
	scheduler.workerWg.Wait()

	log.Infof("[batchScheduler] worker closed")

	for _, q := range scheduler.latchC {
		close(q)
	}
	scheduler.latchWg.Wait()

	log.Infof("[batchScheduler] table latch closed")
}

func (scheduler *batchScheduler) dispatchMsg(msg *core.Msg) error {
	if scheduler.cfg.MaxBatchPerWorker > 1 {
		tableKey := utils.TableIdentity(msg.Database, msg.Table)

		scheduler.tableBuffersMutex.Lock()
		defer scheduler.tableBuffersMutex.Unlock()

		_, ok := scheduler.tableBuffers[tableKey]
		if ok {
			scheduler.tableBuffers[tableKey] <- msg
			return nil
		}

		scheduler.tableBuffers[tableKey] = make(chan *core.Msg, scheduler.cfg.MaxBatchPerWorker*10)
		scheduler.latchC[tableKey] = make(chan uint, scheduler.cfg.MaxBatchPerWorker*10)
		scheduler.bufferWg.Add(1)
		scheduler.latchWg.Add(1)

		scheduler.startTableDispatcher(tableKey)

		scheduler.tableBuffers[tableKey] <- msg
	} else {
		queueIdx := msg.OutputHash() % uint(scheduler.cfg.NrWorker)
		scheduler.workerQueues[queueIdx] <- []*core.Msg{msg}
	}

	return nil
}

func (scheduler *batchScheduler) startTableDispatcher(tableKey string) {
	go func(c chan *core.Msg, latchC chan uint, key string) {
		var batch []*core.Msg
		var round uint
		var closing bool
		latch := make(map[uint]uint)

		flushFunc := func() {
			var curBatch []*core.Msg
			if len(batch) > scheduler.cfg.MaxBatchPerWorker {
				curBatch = make([]*core.Msg, scheduler.cfg.MaxBatchPerWorker)
				copy(curBatch, batch[:scheduler.cfg.MaxBatchPerWorker])
			} else {
				curBatch = make([]*core.Msg, len(batch))
				copy(curBatch, batch)
			}
			for _, item := range curBatch {
				hash := item.OutputHash()
				if item.OutputStreamKey != core.NoDependencyOutput {
					if latch[hash] > 0 {
						return
					}
				}
			}
			for _, m := range curBatch {
				h := m.OutputHash()
				latch[h]++
				oldCB := m.AfterAckCallback
				m.AfterAckCallback = func() error {
					latchC <- h
					if oldCB != nil {
						return oldCB()
					}
					return nil
				}
			}
			queueIdx := round % uint(scheduler.cfg.NrWorker)
			round++
			if !closing {
				scheduler.workerQueues[queueIdx] <- curBatch
				if len(batch) > scheduler.cfg.MaxBatchPerWorker {
					batch = batch[scheduler.cfg.MaxBatchPerWorker:]
				} else if cap(batch) <= scheduler.cfg.MaxBatchPerWorker*2 {
					batch = batch[:0]
				} else {
					batch = nil
				}
			}
		}

		var once sync.Once

		for {
			select {
			case msg, ok := <-c:
				if !ok {
					once.Do(func() {
						if len(batch) > 0 {
							flushFunc()
						}
						closing = true
						scheduler.bufferWg.Done()
					})
					continue
				}
				batch = append(batch, msg)
				batchLen := len(batch)
				qlen := len(c)
				metrics.QueueLength.WithLabelValues(core.PipelineName, "table-buffer", key).Set(float64(qlen))
				if scheduler.needFlush(qlen, batchLen, scheduler.cfg.MaxBatchPerWorker) {
					flushFunc()
				}

			case h, ok := <-latchC:
				if !ok {
					scheduler.latchWg.Done()
					return
				}
				latch[h]--
				if latch[h] == 0 {
					delete(latch, h)
					if len(batch) > 0 && len(latchC) == 0 {
						flushFunc()
					}
				}
				metrics.QueueLength.WithLabelValues(core.PipelineName, "table-latch", key).Set(float64(len(latchC)))
			}
		}
	}(scheduler.tableBuffers[tableKey], scheduler.latchC[tableKey], tableKey)
}

func (scheduler *batchScheduler) needFlush(qLen int, batch int, maxBatchSize int) bool {
	if batch >= maxBatchSize || qLen == 0 {
		return true
	} else {
		return false
	}
}
