package mysql

import (
	log "github.com/sirupsen/logrus"

	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/inputs/helper"
	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
)

func init() {
	registry.RegisterPlugin(registry.InputPlugin, "mysql", &input{}, false)
}

const (
	batch       = "batch"
	stream      = "stream"
	replication = "replication" // scan + binlog
)

type input struct {
	core.Input
}

func (i *input) Configure(pipelineName string, data map[string]interface{}) error {
	mode := data["mode"]
	if mode == nil {
		return errors.Errorf("mysql input should have mode %s, %s or %s", batch, stream, replication)
	}

	var err error

	switch mode.(string) {
	case batch:
		i.Input, err = getDelegate("mysqlbatch", pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case stream:
		i.Input, err = getDelegate("mysqlstream", pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case replication:
		scan, err := getDelegate("mysqlbatch", pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

		binlog, err := getDelegate("mysqlstream", pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

		i.Input, err = helper.NewTwoStageInputPlugin(scan, binlog)
		if err != nil {
			return errors.Trace(err)
		}

	default:
		log.Panic("unknown mode ", mode)
	}

	return nil
}

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
