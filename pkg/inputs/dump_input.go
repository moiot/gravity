package inputs

import (
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/position_repos"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/position_cache"

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

func (plugin *dumpInput) NewPositionStore() (position_cache.PositionCacheInterface, error) {
	return nil, nil
}

func (plugin *dumpInput) Start(emitter core.Emitter, router core.Router, positionCache position_cache.PositionCacheInterface) error {
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

func (plugin *dumpInput) Done(positionCache position_cache.PositionCacheInterface) chan position_repos.Position {
	return make(chan position_repos.Position)
}

func (plugin *dumpInput) Wait() {

}

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "dump-input", &dumpInput{}, false)
}
