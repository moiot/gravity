package emitter

import (
	"testing"

	"github.com/moiot/gravity/pkg/config"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/filters"
	"github.com/moiot/gravity/pkg/utils"
)

type fakeMsgSubmitter struct {
	msgs []*core.Msg
}

func (submitter *fakeMsgSubmitter) SubmitMsg(msg *core.Msg) error {
	submitter.msgs = append(submitter.msgs, msg)
	return nil
}

func TestEmitterWithFilters(t *testing.T) {
	assert := assert.New(t)
	filtersConfigData := []config.GenericConfig{
		{
			Type: "reject",
			Config: map[string]interface{}{
				"match-schema": "bad_db",
				"match-table":  "test_table",
			},
		},
		{
			Type: "rename-dml-column",
			Config: map[string]interface{}{
				"match-schema": "test",
				"match-table":  "test_table",
				"from":         []string{"a", "b"},
				"to":           []string{"c", "d"},
			}},
		{
			Type: "delete-dml-column",
			Config: map[string]interface{}{
				"match-schema": "test",
				"match-table":  "test_table",
				"columns":      []string{"h", "i"},
			},
		},
	}

	fs, err := filters.NewFilters(filtersConfigData)
	if err != nil {
		assert.FailNow(err.Error())
	}

	submitter := &fakeMsgSubmitter{}
	emitter, err := NewEmitter(fs, submitter)
	if err != nil {
		assert.FailNow(err.Error())
	}

	var dmlMsgs []*core.DMLMsg
	for i := 0; i < 5; i++ {
		dmlMsg := core.DMLMsg{
			Data: map[string]interface{}{
				"a": i,
				"b": i + 1,
				"h": i,
			},
		}
		dmlMsgs = append(dmlMsgs, &dmlMsg)
	}

	msgs := []*core.Msg{
		{
			Database:       "bad_db",
			Table:          "test_table",
			DmlMsg:         dmlMsgs[0],
			InputStreamKey: utils.NewStringPtr("test"),
			Done:           make(chan struct{}),
		},
		{
			Database:       "test",
			Table:          "test_table",
			DmlMsg:         dmlMsgs[1],
			InputStreamKey: utils.NewStringPtr("test"),
			Done:           make(chan struct{})},
		{
			Database:       "test",
			Table:          "test_table",
			DmlMsg:         dmlMsgs[2],
			InputStreamKey: utils.NewStringPtr("test"),
			Done:           make(chan struct{}),
		},
		{
			Database:       "test",
			Table:          "test_table",
			DmlMsg:         dmlMsgs[3],
			InputStreamKey: utils.NewStringPtr("test"),
			Done:           make(chan struct{}),
		},
		{
			Database:       "test",
			Table:          "test_table",
			DmlMsg:         dmlMsgs[4],
			InputStreamKey: utils.NewStringPtr("test"),
			Done:           make(chan struct{}),
		},
	}

	for _, m := range msgs {
		err := emitter.Emit(m)
		if err != nil {
			assert.FailNow(err.Error())
		}
	}

	assert.Equal(4, len(submitter.msgs))
	_, ok := submitter.msgs[0].DmlMsg.Data["a"]
	assert.False(ok)

	_, ok = submitter.msgs[0].DmlMsg.Data["b"]
	assert.False(ok)

	_, ok = submitter.msgs[0].DmlMsg.Data["c"]
	assert.True(ok)

	_, ok = submitter.msgs[0].DmlMsg.Data["d"]
	assert.True(ok)

	_, ok = submitter.msgs[0].DmlMsg.Data["h"]
	assert.False(ok)
}
