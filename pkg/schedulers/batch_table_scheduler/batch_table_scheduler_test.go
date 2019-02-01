package batch_table_scheduler

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/outputs"

	"github.com/stretchr/testify/require"

	log "github.com/sirupsen/logrus"

	"github.com/golang/mock/gomock"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

type rows map[string][]*core.Msg

func newMsg(sequence int64, inputStreamKey string, tableName string, pkData string, eventID int) *core.Msg {
	return &core.Msg{
		Type:  core.MsgDML,
		Table: tableName,
		DmlMsg: &core.DMLMsg{
			Data: map[string]interface{}{
				"rowName": pkData,
			},
		},
		InputSequence:   &sequence,
		InputStreamKey:  &inputStreamKey,
		OutputStreamKey: &pkData,
		InputContext:    eventID,
		Done:            make(chan struct{}),
	}
}

func tableName(i int) string {
	return fmt.Sprintf("test_%d", i)
}

func pkData(i int) string {
	return fmt.Sprintf("row_%d", i)
}

func chooseRandomInputStream(sources []string) string {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)

	if len(sources) > 0 {
		return sources[r.Int()%len(sources)]
	} else {
		return ""
	}
}

func submitTestMsgs(scheduler core.MsgSubmitter, inputStreamKeys []string, nrTables int, nrRows int, nrEventsPerRow int) map[string]rows {
	tableMsgs := make(map[string]rows)
	seq := 1
	for tableIdx := 0; tableIdx < nrTables; tableIdx++ {
		for rowIdx := 0; rowIdx < nrRows; rowIdx++ {
			for eventIdx := 0; eventIdx < nrEventsPerRow; eventIdx++ {
				t := tableName(tableIdx)
				if _, ok := tableMsgs[t]; !ok {
					tableMsgs[t] = make(map[string][]*core.Msg)
				}

				r := pkData(rowIdx)
				source := chooseRandomInputStream(inputStreamKeys)
				j := newMsg(
					int64(seq),
					source,
					t,
					r,
					eventIdx,
				)
				if _, ok := tableMsgs[t][r]; !ok {
					tableMsgs[t][r] = []*core.Msg{j}
				} else {
					tableMsgs[t][r] = append(tableMsgs[t][r], j)
				}
				// log.Printf("submit job source: %v, seq: %v\n", source, seq)
				if err := scheduler.SubmitMsg(j); err != nil {
					panic(fmt.Sprintf("submit failed, err: %v", errors.ErrorStack(err)))
				}
				seq++
			}
		}
	}
	return tableMsgs
}

func testTableMsgsEqual(t *testing.T, inputMsgs map[string]rows, outputMsgs map[string]rows) {
	assert := assert.New(t)
	for tableName, table := range inputMsgs {
		for rowName, rowMsgs := range table {

			if len(rowMsgs) != len(outputMsgs[tableName][rowName]) {
				assert.FailNow(fmt.Sprintf("%s, input events: %d, ouput evnets: %d", t.Name(), len(rowMsgs), len(outputMsgs[tableName][rowName])))
			}

			events := make([]int, len(outputMsgs[tableName][rowName]))
			for i, msg := range outputMsgs[tableName][rowName] {

				events[i] = msg.InputContext.(int)
			}

			// fmt.Printf("output job events: %v\n", events)

			for idx, j := range rowMsgs {
				outputMsg := outputMsgs[tableName][rowName][idx]
				if j.Table != outputMsg.Table {
					assert.FailNow(fmt.Sprintf("%s, inputTableName: %s, outputTableName: %s", t.Name(), j.Table, outputMsg.Table))
				}

				assert.Equal(*j.OutputStreamKey, *outputMsg.OutputStreamKey)

				if j.InputContext != outputMsg.InputContext {
					assert.FailNow(fmt.Sprintf("%s, input eventID %d, output eventID: %d", t.Name(), j.InputContext, outputMsg.InputContext))
				}
			}
		}
	}
}

type outputCollector struct {
	sync.Mutex

	// table name is Table filed in core.Msg
	// rowName is pkData
	receivedRows map[string]rows
}

func (output *outputCollector) GetRouter() core.Router {
	return core.EmptyRouter{}
}

func (output *outputCollector) Execute(msgs []*core.Msg) error {
	output.Lock()
	defer output.Unlock()

	for _, m := range msgs {
		table := m.Table
		if _, ok := output.receivedRows[table]; !ok {
			output.receivedRows[table] = make(map[string][]*core.Msg)
		}
		rowName := m.DmlMsg.Data["rowName"].(string)
		output.receivedRows[table][rowName] = append(output.receivedRows[table][rowName], m)
	}
	return nil
}

func (output *outputCollector) Start() error {
	output.receivedRows = make(map[string]rows)
	return nil
}

func (output *outputCollector) Close() {
	output.receivedRows = nil
}

func TestBatchScheduler(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	assert := assert.New(t)

	testCases := []struct {
		name             string
		schedulerConfigs map[string]interface{}
		eventSources     []string
		nrTables         int
		nrRows           int
		nrEventsPerRow   int
	}{
		{
			"simplest case",
			map[string]interface{}{
				"nr-worker":           10,
				"batch-size":          1,
				"queue-size":          1024,
				"sliding-window-size": 1024 * 10,
			},
			[]string{"mysqlstream"},
			1,
			1,
			1,
		},
		{
			"batch size 1",
			map[string]interface{}{
				"nr-worker":           100,
				"batch-size":          1,
				"queue-size":          1024,
				"sliding-window-size": 1024 * 10,
			},
			[]string{"mysqlstream"},
			20,
			100,
			15,
		},
		{
			"batch size 1 with multiple sources",
			map[string]interface{}{
				"nr-worker":           100,
				"batch-size":          1,
				"queue-size":          1024,
				"sliding-window-size": 1024 * 10,
			},
			[]string{"s1", "s2", "s3", "s4", "s5"},
			20,
			100,
			15,
		},
		{
			"batch size 5",
			map[string]interface{}{
				"nr-worker":           100,
				"batch-size":          5,
				"queue-size":          1024,
				"sliding-window-size": 1024 * 10,
			},
			[]string{"mysqlstream"},
			200,
			10,
			15,
		},
		{
			"batch size 5 with multiple sliding windows",
			map[string]interface{}{
				"nr-worker":           100,
				"batch-size":          5,
				"queue-size":          1024,
				"sliding-window-size": 1024 * 10,
			},
			[]string{"s1", "s2", "s3", "s4", "s5"},
			200,
			10,
			15,
		},
	}

	for _, tt := range testCases {
		plugin, err := registry.GetPlugin(registry.SchedulerPlugin, BatchTableSchedulerName)
		if err != nil {
			assert.FailNow(err.Error())
		}

		s, ok := plugin.(core.Scheduler)
		if !ok {
			assert.FailNow(err.Error())
		}

		if err := plugin.Configure("test", tt.schedulerConfigs); err != nil {
			assert.FailNow(err.Error())
		}

		output := &outputCollector{}
		assert.NoError(output.Start())
		assert.NoError(s.Start(output))

		inputMsgs := submitTestMsgs(s, tt.eventSources, tt.nrTables, tt.nrRows, tt.nrEventsPerRow)
		log.Infof("submitted all msgs")
		s.Close()
		log.Infof("scheduler closed")
		testTableMsgsEqual(t, inputMsgs, output.receivedRows)
	}

}

type benchmarkStreamConfig struct {
	workers int
	batch   int
	tables  int
}

func BenchmarkStream(b *testing.B) {
	workers := []int{1, 10, 100}
	batches := []int{1, 10}
	tables := []int{1, 32, 512}
	testCases := make([]benchmarkStreamConfig, 0, len(workers)*len(batches)*len(tables))
	for _, b := range batches {
		for _, t := range tables {
			for _, w := range workers {
				testCases = append(testCases, benchmarkStreamConfig{workers: w, batch: b, tables: t})
			}
		}
	}

	inputStream := "stream"
	const totalRecords = 100000
	const hotRecords = 1024
	log.SetOutput(ioutil.Discard)

	for _, tt := range testCases {
		name := fmt.Sprintf("%dworker-%dtable-%dbatch", tt.workers, tt.tables, tt.batch)
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			b.ResetTimer()
			b.ReportAllocs()

			r := require.New(b)
			plugin, err := registry.GetPlugin(registry.SchedulerPlugin, BatchTableSchedulerName)
			r.NoError(err)
			r.NoError(plugin.Configure(name, DefaultConfig))
			scheduler := plugin.(*batchScheduler)
			scheduler.cfg.NrWorker = tt.workers
			scheduler.cfg.MaxBatchPerWorker = tt.batch
			r.NoError(scheduler.Start(&outputs.DumpOutput{}))

			for i := 0; i < b.N; i++ {
				wg := &sync.WaitGroup{}
				messages := make([]*core.Msg, totalRecords, totalRecords)
				for i := 0; i < totalRecords; i++ {
					seq := int64(i + 1)
					pk := strconv.Itoa(rand.Intn(hotRecords))
					msg := &core.Msg{
						Type:            core.MsgDML,
						Table:           strconv.Itoa(rand.Intn(tt.tables)),
						InputSequence:   &seq,
						InputStreamKey:  &inputStream,
						OutputStreamKey: &pk,
						Done:            make(chan struct{}),
						AfterCommitCallback: func(m *core.Msg) error {
							wg.Done()
							return nil
						},
					}
					messages[i] = msg
				}
				wg.Add(totalRecords)

				b.StartTimer()
				for i := range messages {
					r.NoError(scheduler.SubmitMsg(messages[i]))
				}
				wg.Wait()
				b.StopTimer()
			}
		})
	}
}

func BenchmarkBatch(b *testing.B) {
	workers := []int{1, 10, 20}
	batches := []int{1, 10}
	tables := []int{1, 32, 64}
	testCases := make([]benchmarkStreamConfig, 0, len(workers)*len(batches)*len(tables))
	for _, b := range batches {
		for _, t := range tables {
			for _, w := range workers {
				testCases = append(testCases, benchmarkStreamConfig{workers: w, batch: b, tables: t})
			}
		}
	}

	log.SetOutput(ioutil.Discard)
	const minRecords = 5000
	const totalRecords = 100000

	for _, tt := range testCases {
		name := fmt.Sprintf("%dworker-%dtable-%dbatch", tt.workers, tt.tables, tt.batch)
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			b.ResetTimer()
			b.ReportAllocs()

			r := require.New(b)
			plugin, err := registry.GetPlugin(registry.SchedulerPlugin, BatchTableSchedulerName)
			r.NoError(err)
			r.NoError(plugin.Configure(name, DefaultConfig))
			scheduler := plugin.(*batchScheduler)
			scheduler.cfg.NrWorker = tt.workers
			scheduler.cfg.MaxBatchPerWorker = tt.batch
			r.NoError(scheduler.Start(&outputs.DumpOutput{}))

			for i := 0; i < b.N; i++ {
				wg := &sync.WaitGroup{}

				messages := make([]*core.Msg, 0, minRecords)
				if tt.tables == 1 {
					recordsPerTable := totalRecords
					tableName := "table"
					for i := 0; i < recordsPerTable; i++ {
						seq := int64(i + 1)
						pk := fmt.Sprint(seq) // batch mode has no conflict OutputStreamKey
						msg := &core.Msg{
							Type:            core.MsgDML,
							Table:           tableName,
							InputSequence:   &seq,
							InputStreamKey:  &tableName,
							OutputStreamKey: &pk,
							Done:            make(chan struct{}),
							AfterCommitCallback: func(m *core.Msg) error {
								wg.Done()
								return nil
							},
						}
						messages = append(messages, msg)
					}
				} else {
					c := make(chan *core.Msg, totalRecords)
					wwg := &sync.WaitGroup{}
					wwg.Add(tt.tables)
					go func() {
						wwg.Wait()
						close(c)
					}()
					for tbl := 0; tbl < tt.tables; tbl++ {
						go func(tbl int) {
							defer wwg.Done()
							recordsPerTable := minRecords + rand.Intn(minRecords*10)
							tableName := fmt.Sprint("table-", tbl)
							for i := 0; i < recordsPerTable; i++ {
								seq := int64(i + 1)
								pk := fmt.Sprint(seq) // batch mode has no conflict OutputStreamKey
								msg := &core.Msg{
									Type:            core.MsgDML,
									Table:           tableName,
									InputSequence:   &seq,
									InputStreamKey:  &tableName,
									OutputStreamKey: &pk,
									Done:            make(chan struct{}),
									AfterCommitCallback: func(m *core.Msg) error {
										wg.Done()
										return nil
									},
								}
								c <- msg
							}
						}(tbl)
					}
					for m := range c {
						messages = append(messages, m)
					}
					messages = messages[:totalRecords]
				}
				wg.Add(totalRecords)
				b.StartTimer()
				for i := range messages {
					r.NoError(scheduler.SubmitMsg(messages[i]))
				}
				wg.Wait()
				b.StopTimer()
			}
		})
	}
}
