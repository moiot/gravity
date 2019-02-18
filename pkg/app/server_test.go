package app

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/config"
)

func TestServerConfigStructureV2(t *testing.T) {
	at := assert.New(t)
	tomlConfig := `
[input.mysql]
mode = "replication"
nr-scanner = 10
table-scan-batch = 10000

[input.mysql.source]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306

[[input.mysql.table-configs]]
schema = "test_1"
table = "test_source_*"

[[input.mysql.table-configs]]
schema = "test_2"
table = "test_source_*"

[[filters]]
type = "reject"
match-schema = "test"
match-table = "test_table_*"

[output.mysql.target]
host = "127.0.0.1"
username = "root"
password = ""
port = 3306

[[output.mysql.routes]]
match-schema = "test"
match-table = "test_source_table"
target-schema = "test"
target-table = "test_target_table"

[scheduler.batch-table-scheduler]
nr-worker = 2
batch-size = 1
queue-size = 1024
sliding-window-size = 1024
`
	pipelineConfig := config.PipelineConfigV2{}

	_, err := toml.Decode(tomlConfig, &pipelineConfig)
	if err != nil {
		at.FailNow(errors.ErrorStack(err))
	}

	at.False(pipelineConfig.IsV3())
	server, err := ParsePlugins(pipelineConfig.ToV3())
	at.NoError(err, errors.ErrorStack(err))
	at.True(len(server.filters) == 1)
}

func TestServerConfigStructureV3(t *testing.T) {
	at := assert.New(t)
	tomlConfig := `
[input]
type = "mysql"
mode = "replication"

[input.config]
nr-scanner = 10
table-scan-batch = 10000

[input.config.source]
host = "127.0.0.1"
username = "drc"
password = ""
port = 3306

[[input.config.table-configs]]
schema = "test_1"
table = "test_source_*"

[[input.config.table-configs]]
schema = "test_2"
table = "test_source_*"

[[filters]]
type = "reject"

[filters.config]
match-schema = "test"
match-table = "test_table_*"

[[filters]]
type = "reject"

[filters.config]
match-dml-op = ["insert", "update", "delete"]

[output]
type = "mysql"

[output.config.target]
host = "127.0.0.1"
username = "root"
password = ""
port = 3306

[[output.config.routes]]
match-schema = "test"
match-table = "test_source_table"
target-schema = "test"
target-table = "test_target_table"

[scheduler]
type = "batch-table-scheduler"
[scheduler.config]
nr-worker = 2
batch-size = 1
queue-size = 1024
sliding-window-size = 1024
`
	pipelineConfig := config.PipelineConfigV3{}

	_, err := toml.Decode(tomlConfig, &pipelineConfig)
	if err != nil {
		at.FailNow(errors.ErrorStack(err))
	}

	server, err := ParsePlugins(pipelineConfig)
	at.NoError(err, errors.ErrorStack(err))
	at.True(len(server.filters) == 2)
}
