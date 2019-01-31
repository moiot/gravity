package mysql

import (
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/inputs/mysqlbatch"
	"github.com/moiot/gravity/pkg/inputs/mysqlstream"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs/helper"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const Name = "mysql"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &input{}, false)
}

type input struct {
	core.Input
}

func (i *input) Configure(pipelineName string, data map[string]interface{}) error {
	mode := data["mode"]
	if mode == nil {
		return errors.Errorf("mysql input should have mode %s, %s or %s", config.Batch, config.Stream, config.Replication)
	}

	var err error

	switch mode.(config.InputMode) {
	case config.Batch:
		i.Input, err = helper.GetInputFromPlugin(mysqlbatch.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case config.Stream:
		i.Input, err = helper.GetInputFromPlugin(mysqlstream.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case config.Replication:
		scan, err := helper.GetInputFromPlugin(mysqlbatch.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

		binlog, err := helper.GetInputFromPlugin(mysqlstream.Name, pipelineName, data)
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
