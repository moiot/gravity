package sliding_window

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/env"
	"github.com/moiot/gravity/pkg/metrics"
)

const ProcessDelayWarningThreshold = 30

type staticSlidingWindow struct {
	cap int

	name string

	// waitingItemC is used to holds sequence window,
	// the size of this queue determines the window size of the sliding window
	waitingItemC chan WindowItem

	nextItemToCommit WindowItem

	readyC chan int64

	readyCommitHeap *windowItemHeap

	wgForClose sync.WaitGroup

	// lastEnqueueProcessTime is the time when the last event
	// enqueued into sliding window
	lastEnqueueProcessTime int64

	// lastCommitProcessTime is the time when the last event
	// enqueued into sliding window
	lastCommitProcessTime int64

	// lastEnqueueEventTime is the time when the last even
	// t happens at the source, for example,
	// mysql binlog have this timestamp in binlog protocol.
	lastEnqueueEventTime int64

	// lastCommitEventTime is the time when the last event
	// happens at the source, for example, mysql binlog timestamp.
	lastCommitEventTime int64

	heapSize int64

	closeC chan struct{}
}

func (w *staticSlidingWindow) AddWindowItem(item WindowItem) {
	atomic.StoreInt64(&w.lastEnqueueProcessTime, item.ProcessTime().UnixNano())
	atomic.StoreInt64(&w.lastEnqueueEventTime, item.EventTime().UnixNano())
	w.waitingItemC <- item
}

func (w *staticSlidingWindow) AckWindowItem(seq int64) {
	w.readyC <- seq
}

func (w *staticSlidingWindow) Watermark() Watermark {
	// an approximate view which doesn't need synchronization
	lastEnqueuePT := atomic.LoadInt64(&w.lastEnqueueProcessTime)
	lastCommitPT := atomic.LoadInt64(&w.lastCommitProcessTime)
	lastEnqueueET := atomic.LoadInt64(&w.lastEnqueueEventTime)
	lastCommitET := atomic.LoadInt64(&w.lastCommitEventTime)

	var processTime int64
	if lastEnqueuePT <= lastCommitPT {
		processTime = lastEnqueuePT
	} else {
		processTime = lastCommitPT
	}

	var eventTime int64
	if lastEnqueueET <= lastCommitET {
		eventTime = lastEnqueueET
	} else {
		eventTime = lastCommitET
	}

	return Watermark{
		ProcessTime: time.Unix(0, processTime),
		EventTime:   time.Unix(0, eventTime),
	}
}

func (w *staticSlidingWindow) Size() int {
	return w.cap
}

func (w *staticSlidingWindow) WaitingQueueLen() int {
	return len(w.waitingItemC)
}

func (w *staticSlidingWindow) Close() {
	// log.Infof("[staticSlidingWindow] closing")
	close(w.closeC)

	close(w.waitingItemC)

	w.wgForClose.Wait()

	close(w.readyC)

	// log.Infof("[staticSlidingWindow] closed")
}

// start runs the loop that get item from readyC and put it into readyCommitHeap
// when the current smallest item is the same as the readyCommitHeap's smallest item
// we then call the commit method of the item.
func (w *staticSlidingWindow) start() {
	defer w.wgForClose.Done()

	// init the nextItemToCommit the first time
	nextItemToCommit, ok := <-w.waitingItemC
	if !ok {
		log.Infof("[staticSlidingWindow]: exist on init.")
		return
	} else {
		w.nextItemToCommit = nextItemToCommit
		log.Infof("[staticSlidingWindow] init nextItemToCommit: %s", w.nextItemToCommit)
	}

	for readyItem := range w.readyC {
		heap.Push(w.readyCommitHeap, readyItem)
		atomic.AddInt64(&w.heapSize, 1)
		for {
			smallestItem, err := w.readyCommitHeap.SmallestItem()

			// empty heap, break the loop, continue waiting for ready item
			if err != nil {
				break
			}

			//log.Infof("[sliding_window] smallestItem: %v, nextSequence: %v", smallestItem.SequenceNumber(), w.nextItemToCommit.SequenceNumber())

			// break the loop, continue waiting for ready item
			if smallestItem != w.nextItemToCommit.SequenceNumber() {
				break
			}

			// now we are ready to commit
			w.nextItemToCommit.BeforeWindowMoveForward()
			atomic.StoreInt64(&w.lastCommitProcessTime, w.nextItemToCommit.ProcessTime().UnixNano())
			atomic.StoreInt64(&w.lastCommitEventTime, w.nextItemToCommit.EventTime().UnixNano())

			//log.Infof("[sliding_window] pop: %v", smallestItem.SequenceNumber())

			heap.Pop(w.readyCommitHeap)
			atomic.AddInt64(&w.heapSize, -1)

			w.nextItemToCommit, ok = <-w.waitingItemC
			if !ok {
				return
			}
		}
	}
}

func (w *staticSlidingWindow) reportMetrics() {
	defer w.wgForClose.Done()

	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			waitingCnt := len(w.waitingItemC)

			if waitingCnt > 0 {
				watermark := w.Watermark()

				// ProcessTime can be seen as the duration that event are in the queue.
				seconds := time.Since(watermark.ProcessTime).Seconds()
				if seconds > ProcessDelayWarningThreshold {
					log.Warnf("[sliding_window] item not ack after %f seconds. %s", seconds, w.nextItemToCommit)
				}
				metrics.End2EndProcessTimeHistogram.WithLabelValues(env.PipelineName).Observe(seconds)

				// EventTime can be seen as the end to end duration of event process time.
				metrics.End2EndEventTimeHistogram.WithLabelValues(env.PipelineName).Observe(time.Since(watermark.EventTime).Seconds())
			} else {
				metrics.End2EndProcessTimeHistogram.WithLabelValues(env.PipelineName).Observe(0)
				metrics.End2EndEventTimeHistogram.WithLabelValues(env.PipelineName).Observe(0)
			}

			metrics.QueueLength.WithLabelValues(env.PipelineName, "sliding-window-waiting-chan", w.name).Set(float64(waitingCnt))
			metrics.QueueLength.WithLabelValues(env.PipelineName, "sliding-window-ready-chan", w.name).Set(float64(len(w.readyC)))
			metrics.QueueLength.WithLabelValues(env.PipelineName, "sliding-window-ready-heap", w.name).Set(float64(atomic.LoadInt64(&w.heapSize)))

		case <-w.closeC:
			ticker.Stop()
			return
		}
	}
}

func NewStaticSlidingWindow(windowSize int, name string) Window {
	h := &windowItemHeap{}
	heap.Init(h)

	w := &staticSlidingWindow{
		cap:                   windowSize,
		name:                  name,
		waitingItemC:          make(chan WindowItem, windowSize),
		readyCommitHeap:       h,
		readyC:                make(chan int64, windowSize),
		lastCommitProcessTime: 1<<63 - 1,
		closeC:                make(chan struct{}),
	}

	w.wgForClose.Add(2)
	go w.start()
	go w.reportMetrics()

	return w
}
