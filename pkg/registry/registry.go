package registry

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type PluginType string

const (
	InputPlugin              PluginType = "input"
	PositionRepo             PluginType = "positionRepo"
	OutputPlugin             PluginType = "output"
	FilterPlugin             PluginType = "filters"
	MatcherPlugin            PluginType = "matcher"
	SchedulerPlugin          PluginType = "scheduler"
	SQLExecutionEnginePlugin PluginType = "sqlExecutionEngine"
)

type Plugin interface {
	Configure(pipelineName string, data map[string]interface{}) error
}

type PluginFactory func() Plugin

var registry map[PluginType]map[string]PluginFactory
var mutex sync.Mutex

func RegisterPluginFactory(pluginType PluginType, name string, v PluginFactory) {
	mutex.Lock()
	defer mutex.Unlock()

	log.Debugf("[RegisterPlugin] type: %v, name: %v", pluginType, name)
	if registry == nil {
		registry = make(map[PluginType]map[string]PluginFactory)
	}

	_, ok := registry[pluginType]
	if !ok {
		registry[pluginType] = make(map[string]PluginFactory)
	}

	_, ok = registry[pluginType][name]
	if ok {
		panic(fmt.Sprintf("plugin already exists, type: %v, name: %v", pluginType, name))
	}
	registry[pluginType][name] = v
}

func RegisterPlugin(pluginType PluginType, name string, v Plugin, singleton bool) {
	var pf PluginFactory
	if singleton {
		pf = func() Plugin {
			return v
		}
	} else {
		pf = func() Plugin {
			return reflect.New(reflect.TypeOf(v).Elem()).Interface().(Plugin)
		}
	}
	RegisterPluginFactory(pluginType, name, pf)
}

func GetPlugin(pluginType PluginType, name string) (Plugin, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if registry == nil {
		return nil, errors.Errorf("empty registry")
	}

	plugins, ok := registry[pluginType]
	if !ok {
		return nil, errors.Errorf("empty plugin type: %v, name: %v", pluginType, name)
	}

	p, ok := plugins[name]
	if !ok {
		return nil, errors.Errorf("empty plugin, type: %v, name: %v", pluginType, name)
	}
	return p(), nil
}
