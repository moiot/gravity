package mysqlstream

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_mysqlInputPlugin_Configure(t *testing.T) {
	r := require.New(t)
	raw := `{
      "ignore-bidirectional-data": true,
      "source": {
        "host": "localhost"
      },
      "start-position": {
        "binlog-gtid": "gtid"
      }
    }`

	var m map[string]interface{}
	err := json.Unmarshal([]byte(raw), &m)
	r.NoError(err)

	p := mysqlStreamInputPlugin{}
	err = p.Configure("123", m)
	r.NoError(err)

	cfg := p.cfg
	r.Equal(true, cfg.IgnoreBiDirectionalData)
	r.Equal("localhost", cfg.Source.Host)
	r.Equal("gtid", cfg.StartPosition.BinlogGTID)
}
