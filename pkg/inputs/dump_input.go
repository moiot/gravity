package inputs

import (
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/position_store"

	"github.com/moiot/gravity/pkg/registry"
)

type pluginConfig struct {
	TestKey1 string `mapstructure:"test-key-1"`
}

type dumpInput struct {
	pipelineName string
	cfg          *pluginConfig
}

func (plugin *dumpInput) Configure(pipelineName string, data map[string]interface{}) error {
	cfg := pluginConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (plugin *dumpInput) NewPositionStore() (position_store.PositionCacheInterface, error) {
	return nil, nil
}

func (plugin *dumpInput) Start(emitter core.Emitter, positionCache position_store.PositionCacheInterface) error {
	return nil
}

func (plugin *dumpInput) Close() {

}

func (plugin *dumpInput) Stage() config.InputMode {
	return config.Stream
}

func (plugin *dumpInput) SendDeadSignal() error {
	return nil
}

func (plugin *dumpInput) Done(positionCache position_store.PositionCacheInterface) chan position_store.Position {
	return make(chan position_store.Position)
}

func (plugin *dumpInput) Wait() {

}

func (plugin *dumpInput) Identity() uint32 {
	return 1
}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "dump-input", &dumpInput{}, false)
}
