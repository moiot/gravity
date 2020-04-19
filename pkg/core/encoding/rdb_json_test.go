package encoding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/core"
)

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
