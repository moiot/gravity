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
	"github.com/moiot/gravity/pkg/env"
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
	NrWorker          int           `mapstructure:"nr-worker" json:"nr-worker"`
	MaxBatchPerWorker int           `mapstructure:"batch-size" json:"batch-size"`
	QueueSize         int           `mapstructure:"queue-size" json:"queue-size"`
	SlidingWindowSize int           `mapstructure:"sliding-window-size" json:"sliding-window-size"`
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
	tableBufferWg     sync.WaitGroup
	tableLatchC       map[string]chan uint64

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

	if schedulerConfig.NrRetries < 0 {
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
		sliding_window.HealthyThreshold = float64(schedulerConfig.HealthyThreshold)
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
			log.Warnf("[batchScheduler.Healthy] sliding window %s not healthy, watermark: %s", k, time.Since(watermark.ProcessTime).String())
			return false
		}
	}

	duration := time.Since(env.StartTime)
	if len(scheduler.slidingWindows) == 0 && duration.Seconds() > sliding_window.HealthyThreshold {
		log.Warnf("[batchScheduler.Healthy] no window for %s", duration)
		return false
	} else {
		return true
	}
}

func (scheduler *batchScheduler) Watermarks() (result map[string]sliding_window.Watermark) {
	if scheduler.cfg.SlidingWindowSize == 0 {
		return
	}

	result = make(map[string]sliding_window.Watermark)

	scheduler.windowMutex.Lock()
	defer scheduler.windowMutex.Unlock()

	for k, w := range scheduler.slidingWindows {
		result[k] = w.Watermark()
	}

	return
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
		go func(q chan []*core.Msg, workerIndex int) {
			defer scheduler.workerWg.Done()

			workerName := fmt.Sprintf("%d", workerIndex)

			for msgBatch := range q {
				metrics.QueueLength.WithLabelValues(scheduler.pipelineName, "worker", workerName).Set(float64(len(q)))

				now := time.Now()
				hasCtl := false
				for _, m := range msgBatch {
					if m.Type == core.MsgCtl {
						if err := scheduler.AckMsg(m); err != nil {
							log.Fatalf("[batchScheduler] err: %v", errors.ErrorStack(err))
						}
						hasCtl = true
					} else {
						m.EnterOutput = now
					}
				}

				if hasCtl {
					continue
				}

				// should not take ctl message into account
				WorkerPoolJobBatchSizeGauge.WithLabelValues(scheduler.pipelineName, workerName).Set(float64(len(msgBatch)))
				metrics.Scheduler2OutputCounter.WithLabelValues(env.PipelineName).Add(float64(len(msgBatch)))

				if scheduler.syncOutput != nil {
					err := retry.Do(func() error {
						err := scheduler.syncOutput.Execute(msgBatch)
						if err != nil {
							metrics.SchedulerRetryCounter.WithLabelValues(env.PipelineName).Add(1)
							log.Warnf("error execute sync output, retry. %v", err)
						}
						return err
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

		}(scheduler.workerQueues[i], i)
	}

	scheduler.tableBuffers = make(map[string]chan *core.Msg)
	scheduler.tableLatchC = make(map[string]chan uint64)
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
			window = sliding_window.NewStaticSlidingWindow(scheduler.cfg.SlidingWindowSize, key)
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
		scheduler.workerQueues[msg.SequenceNumber()%int64(scheduler.cfg.NrWorker)] <- []*core.Msg{msg}
	} else {
		err = scheduler.dispatchMsg(msg)
		msg.LeaveSubmitter = time.Now()
	}
	metrics.SchedulerSubmitHistogram.WithLabelValues(env.PipelineName).Observe(msg.LeaveSubmitter.Sub(msg.EnterScheduler).Seconds())
	return err
}

func (scheduler *batchScheduler) AckMsg(msg *core.Msg) error {
	if msg.Type != core.MsgCtl { // control msg doesn't enter output
		msg.EnterAcker = time.Now()
		metrics.OutputHistogram.WithLabelValues(env.PipelineName).Observe(msg.EnterAcker.Sub(msg.EnterOutput).Seconds())
	}

	if msg.AfterAckCallback != nil {
		err := msg.AfterAckCallback(msg)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if scheduler.cfg.SlidingWindowSize > 0 {

		scheduler.windowMutex.Lock()
		window := scheduler.slidingWindows[*msg.InputStreamKey]
		scheduler.windowMutex.Unlock()

		window.AckWindowItem(msg.SequenceNumber())
		WorkerPoolSlidingWindowRatio.WithLabelValues(scheduler.pipelineName, *msg.InputStreamKey).Set(float64(window.WaitingQueueLen() / window.Size()))
	}

	if msg.Type != core.MsgCtl {
		msg.LeaveScheduler = time.Now()
		metrics.SchedulerTotalHistogram.WithLabelValues(env.PipelineName).Observe(msg.LeaveScheduler.Sub(msg.EnterScheduler).Seconds())
		metrics.SchedulerAckHistogram.WithLabelValues(env.PipelineName).Observe(msg.LeaveScheduler.Sub(msg.EnterAcker).Seconds())
	}
	return nil
}

func (scheduler *batchScheduler) Close() {
	log.Infof("[batchScheduler] closing scheduler")

	scheduler.tableBuffersMutex.Lock()
	for _, c := range scheduler.tableBuffers {
		close(c)
	}
	scheduler.tableBuffersMutex.Unlock()

	scheduler.tableBufferWg.Wait()

	log.Infof("[batchScheduler] table buffer closed")

	for _, q := range scheduler.workerQueues {
		close(q)
	}
	scheduler.workerWg.Wait()

	log.Infof("[batchScheduler] worker closed")

	for _, q := range scheduler.tableLatchC {
		close(q)
	}
	log.Infof("[batchScheduler] table latch closed")

	for _, w := range scheduler.slidingWindows {
		w.Close()
	}
	log.Infof("[batchScheduler] sliding window closed")
}

func (scheduler *batchScheduler) dispatchMsg(msg *core.Msg) error {

	tableKey := utils.TableIdentity(msg.Database, msg.Table)

	scheduler.tableBuffersMutex.Lock()
	defer scheduler.tableBuffersMutex.Unlock()

	_, ok := scheduler.tableBuffers[tableKey]
	if ok {
		scheduler.tableBuffers[tableKey] <- msg
		return nil
	}

	scheduler.startTableDispatcher(tableKey)

	scheduler.tableBuffers[tableKey] <- msg
	return nil
}

func (scheduler *batchScheduler) startTableDispatcher(tableKey string) {
	scheduler.tableBuffers[tableKey] = make(chan *core.Msg, scheduler.cfg.MaxBatchPerWorker*10)
	scheduler.tableLatchC[tableKey] = make(chan uint64, scheduler.cfg.SlidingWindowSize)
	scheduler.tableBufferWg.Add(1)

	go func(c chan *core.Msg, tableLatchC chan uint64, key string) {
		defer scheduler.tableBufferWg.Done()

		var batch []*core.Msg
		var round uint
		var closing bool

		// latches's key is message's output hash, and its value is the number of messages
		// having the same output hash.
		latches := make(map[uint64]int)

		defaultCB := func(message *core.Msg) error {
			for _, h := range message.OutputDepHashes {
				tableLatchC <- h.H
			}
			return nil
		}

		//
		// flushFunc will pick up messages that is not latched, assemble these messages into a "curBatch".
		// After a flush, the latched messages are kept in the "batch" for the next flush.
		//
		// With the reassembling, a message that has a latch won't block messages comes after it.
		//
		// For example:
		//
		// Assume the table latch is
		//
		//   {a: 1, d: 1, e: 1}
		//
		// and MaxBatchPerWorker = 2
		//
		// If the latch for "a", "b", "c" are not released for 3 rounds, then the result for each round are:
		//
		// Round 1 batch: [a, b, c, d, e, f, g]
		//         curBatch: [b, c]
		//
		// Round 2 batch: [a, d, e, f, g, h, i]
		//         curBatch: [f, g]
		//
		// Round 3 batch: [a, d, e]
		//
		flushFunc := func() {
			flushLen := scheduler.cfg.MaxBatchPerWorker
			if len(batch) < flushLen {
				flushLen = len(batch)
			}
			curBatch := make([]*core.Msg, 0, flushLen)
			var reminder []*core.Msg
			var latchedMsg []*core.Msg
			var latched bool
			curLatches := make(map[uint64]int)
			for i, m := range batch {
				latched = false
				for _, h := range m.OutputDepHashes {
					if latches[h.H] > 0 || curLatches[h.H] > 0 {
						latched = true
						break
					}
				}

				if !latched { // neither latched by previous batch nor current batch
					curBatch = append(curBatch, m)
					for _, h := range m.OutputDepHashes {
						latches[h.H] += 1
					}
					if len(m.OutputDepHashes) > 0 {
						if m.AfterAckCallback == nil {
							m.AfterAckCallback = defaultCB
						} else {
							oldCB := m.AfterAckCallback
							m.AfterAckCallback = func(message *core.Msg) error {
								if err := defaultCB(message); err != nil {
									return errors.Trace(err)
								}
								return errors.Trace(oldCB(message))
							}
						}
					}
					if len(curBatch) == flushLen {
						if len(batch) > i+1 {
							reminder = batch[i+1:]
						}
						break
					}
				} else {
					latchedMsg = append(latchedMsg, m)
					for _, h := range m.OutputDepHashes { // add to latch of current batch to intercept msg following
						curLatches[h.H] += 1
					}
				}
			}

			if len(curBatch) > 0 {
				if curBatch[0].Type == core.MsgDDL {
					ddlIdx := utils.GenHashKey(utils.TableIdentity(curBatch[0].Database, curBatch[0].Table)) % uint32(scheduler.cfg.NrWorker)
					scheduler.workerQueues[ddlIdx] <- curBatch
				} else {
					queueIdx := round % uint(scheduler.cfg.NrWorker)
					round++
					scheduler.workerQueues[queueIdx] <- curBatch
				}
			}

			// delete the delivered messages
			newBatch := batch[:0] // reuse batch storage
			newBatch = append(newBatch, latchedMsg...)
			newBatch = append(newBatch, reminder...)
			for i := len(newBatch); i < len(batch); i++ {
				batch[i] = nil
			}
			batch = newBatch
		}

		tryFlush := func() {
			queueLen := len(c)
			latchQLen := len(tableLatchC)

			metrics.QueueLength.WithLabelValues(env.PipelineName, "table-queue", key).Set(float64(queueLen))
			metrics.QueueLength.WithLabelValues(env.PipelineName, "table-buffer", key).Set(float64(len(batch)))
			metrics.QueueLength.WithLabelValues(env.PipelineName, "table-latch", key).Set(float64(len(latches)))
			metrics.QueueLength.WithLabelValues(env.PipelineName, "table-latch-ack", key).Set(float64(latchQLen))

			if len(batch) > 0 {
				if len(batch) >= scheduler.cfg.MaxBatchPerWorker {
					flushFunc()
				} else if (queueLen + latchQLen) == 0 {
					queueIdx := round % uint(scheduler.cfg.NrWorker)
					round++
					if len(scheduler.workerQueues[queueIdx]) < scheduler.cfg.QueueSize/2 {
						flushFunc()
					} else {
						// worker queue has many items pending, try to accumulate message in the batch.
					}
				}
			}
		}

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case msg, ok := <-c:
				if !ok {
					// no more incoming message now
					closing = true
					continue
				}
				batch = append(batch, msg)
				tryFlush()

			case h := <-tableLatchC:
				latches[h]--
				if latches[h] < 0 {
					log.Fatalf("latch operation bug: h = %v", h)
				}
				if latches[h] == 0 {
					delete(latches, h)
					tryFlush()
				}

			case <-ticker.C:
				// if there is no message will come, and the current batch is empty, and all latches are releases,
				// then we can are in a graceful shutdown; otherwise, we try to flush the batch.
				if closing && len(batch) == 0 && len(latches) == 0 {
					return
				} else {
					tryFlush()
				}
			}
		}
	}(scheduler.tableBuffers[tableKey], scheduler.tableLatchC[tableKey], tableKey)
}
