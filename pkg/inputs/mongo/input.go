package mongo

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/position_store"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

// TODO: remove duplicate with inputs/mysql
func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mongo", &input{}, false)
}

type input struct {
	core.Input
}

func (i *input) Configure(pipelineName string, data map[string]interface{}) error {
	mode := data["mode"]
	if mode == nil {
		return errors.Errorf("mongo input should have mode %s", config.Stream)
	}

	var err error

	switch mode.(config.InputMode) {
	case config.Batch:
		return errors.Errorf("mongo does not support 'batch' right now")

	case config.Stream:
		i.Input, err = getDelegate("mongooplog", pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case config.Replication:
		return errors.Errorf("mongo does not support 'replication' mode right now")

	default:
		log.Panic("unknown mode ", mode)
	}

	return nil
}

func (i *input) NewPositionCache() (position_store.PositionCacheInterface, error) {
	newer, ok := i.Input.(core.PositionCacheCreator)
	if !ok {
		return nil, errors.Errorf("not a PositionCacheCreator")
	}

	return newer.NewPositionCache()
}

// TODO: remove duplicate with inputs/mysql
func getDelegate(pluginName string, pipelineName string, data map[string]interface{}) (core.Input, error) {
	plugin, err := registry.GetPlugin(registry.InputPlugin, pluginName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = plugin.Configure(pipelineName, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return plugin.(core.Input), nil
}
