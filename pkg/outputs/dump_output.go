package outputs

import (
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

type pluginConfig struct {
	TestKey1 string `mapstructure:"test-key-1"`
}

type DumpOutput struct {
	pipelineName string
	cfg          *pluginConfig
}

func (plugin *DumpOutput) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := pluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (plugin *DumpOutput) GetRouter() core.Router {
	return core.EmptyRouter{}
}

func (plugin *DumpOutput) Start() error {
	return nil
}

func (plugin *DumpOutput) Close() {

}

func (plugin *DumpOutput) Execute(_ int, msgs []*core.Msg) error {
	return nil
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, "dump-output", &DumpOutput{}, false)
}
