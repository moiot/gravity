package helper

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

func GetInputFromPlugin(pluginName string, pipelineName string, data map[string]interface{}) (core.Input, error) {
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
