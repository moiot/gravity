package encoding

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

func TestSerializeVersion10JsonMsg(t *testing.T) {
	a := assert.New(t)

	tsString := "2018-01-01T22:35:35+08:00"
	ts, err := time.Parse(time.RFC3339Nano, tsString)
	if err != nil {
		a.FailNow(err.Error())
	}
	de, err := decimal.NewFromString("1000")
	a.NoError(err)

	cases := []struct {
		name       string
		msg        core.Msg
		jsonString string
	}{
		{
			"dml",
			core.Msg{
				Type: core.MsgDML,
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id":   1,
						"name": nil,
						"ts":   ts,
						"f":    1000000000000.000345678,
						"de":   de,
						"a":    "中文",
						"b":    "😄",
						"c":    true,
						"d":    false,
					},
				},
			},
			`{"version":"0.1","database":"","table":"","type":"insert","ts":-62135596800,"time_zone":"Asia/Shanghai","host":"","data":{"a":"中文","b":"😄","c":"true","d":"false","f":10000.345678,"id":1,"name":null,"ts":"2018-01-01T22:35:35+08:00"},"old":null,"Pks":null}`,
		},
	}

	for _, c := range cases {
		b, err := serializeVersion01JsonMsg(&c.msg)
		if err != nil {
			a.FailNow(err.Error())
		}
		a.Equal(c.jsonString, string(b))
	}
}

func TestSerializeVersion20JsonMsg(t *testing.T) {
	assert := assert.New(t)

	tsString := "2018-01-01T22:35:35+08:00"
	ts, err := time.Parse(time.RFC3339Nano, tsString)
	if err != nil {
		assert.FailNow(err.Error())
	}
	cases := []struct {
		name       string
		msg        core.Msg
		jsonString string
	}{
		{
			"dml",
			core.Msg{
				Type: core.MsgDML,
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id":   1,
						"name": nil,
						"ts":   ts,
						"f":    10.345678,
						"a":    "中文",
						"b":    "😄",
						"c":    true,
						"d":    false,
					},
				},
			},
			`{"version":"2.0.alpha","database":"","table":"","type":"insert","Data":{"a":"中文","b":"😄","c":true,"d":false,"f":10.345678,"id":1,"name":null,"ts":"2018-01-01T22:35:35+08:00"},"Old":null,"Pks":null}`,
		},
	}

	for _, c := range cases {
		b, err := serializeVersion20JsonMsg(&c.msg)
		if err != nil {
			assert.FailNow(err.Error())
		}
		assert.Equal(c.jsonString, string(b))
	}
}
