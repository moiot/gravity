package gravity

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	assert "github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/gravity/config"
)

func TestServerConfigStructure(t *testing.T) {
	assert := assert.New(t)
	tomlConfig := `
[input.dump-input]
test-key-1 = "dump"

[[filters]]
type = "reject"
match-schema = "test_db"
match-table = "test_table"

[output.dump-output]
test-key-1 = "dump"

[system.scheduler]
type = "batch-table-scheduler"
nr-worker = 1
batch-size = 2
queue-size = 1024
sliding-window-size = 1024
`
	pipelineConfig := config.PipelineConfigV2{}

	_, err := toml.Decode(tomlConfig, &pipelineConfig)
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

	server, err := NewServer(&pipelineConfig)
	assert.Nilf(err, errors.ErrorStack(err))
	assert.True(len(server.filters) == 1)
}
