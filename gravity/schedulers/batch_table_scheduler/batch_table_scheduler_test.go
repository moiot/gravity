package batch_table_scheduler

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/golang/mock/gomock"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
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

func (output *outputCollector) Execute(msgs []*core.Msg) error {
	for _, m := range msgs {
		table := m.Table

		output.Lock()
		if output.receivedRows == nil {
			output.receivedRows = make(map[string]rows)
		}

		if _, ok := output.receivedRows[table]; !ok {
			output.receivedRows[table] = make(map[string][]*core.Msg)
		}

		rowName := m.DmlMsg.Data["rowName"].(string)
		if _, ok := output.receivedRows[table][rowName]; !ok {
			output.receivedRows[table][rowName] = []*core.Msg{m}
		} else {
			output.receivedRows[table][rowName] = append(output.receivedRows[table][rowName], m)
		}
		output.Unlock()
	}
	return nil
}

func (output *outputCollector) Start() error {
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
			200,
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
			200,
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
		err = s.Start(output)
		if err != nil {
			assert.FailNow(err.Error())
		}

		inputMsgs := submitTestMsgs(s, tt.eventSources, tt.nrTables, tt.nrRows, tt.nrEventsPerRow)
		log.Infof("submitted all msgs")
		s.Close()
		log.Infof("scheduler closed")
		testTableMsgsEqual(t, inputMsgs, output.receivedRows)
	}

}
