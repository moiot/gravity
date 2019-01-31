package mongo

import (
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/inputs/helper"
	"github.com/moiot/gravity/pkg/inputs/mongobatch"
	"github.com/moiot/gravity/pkg/inputs/mongostream"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

// TODO: remove duplicate with inputs/mysql
const Name = "mongo"

func init() {
	registry.RegisterPlugin(registry.InputPlugin, Name, &input{}, false)
}

type input struct {
	core.Input
}

func (i *input) Configure(pipelineName string, data map[string]interface{}) error {
	mode := data["mode"]
	if mode == nil {
		return errors.Errorf("mongo input should have mode")
	}

	var err error

	switch mode.(config.InputMode) {
	case config.Batch:
		i.Input, err = helper.GetInputFromPlugin(mongobatch.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case config.Stream:
		i.Input, err = helper.GetInputFromPlugin(mongostream.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

	case config.Replication:
		batch, err := helper.GetInputFromPlugin(mongobatch.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

		stream, err := helper.GetInputFromPlugin(mongostream.Name, pipelineName, data)
		if err != nil {
			return errors.Trace(err)
		}

		i.Input, err = helper.NewTwoStageInputPlugin(batch, stream)
		if err != nil {
			return errors.Trace(err)
		}

	default:
		log.Panic("unknown mode ", mode)
	}

	return nil
}
