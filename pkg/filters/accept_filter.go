package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const AcceptFilterName = "accept"

type acceptFilterType struct {
	BaseFilter
}

func (f *acceptFilterType) Configure(configData map[string]interface{}) error {
	err := f.ConfigureMatchers(configData)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (f *acceptFilterType) Filter(msg *core.Msg) (continueNext bool, err error) {
	if f.Matchers.Match(msg) {
		return true, nil
	}
	return false, nil
}

type acceptFilterFactoryType struct{}

func (factory *acceptFilterFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (factory *acceptFilterFactoryType) NewFilter() core.IFilter {
	return &acceptFilterType{}
}

func init() {
	registry.RegisterPlugin(registry.FilterPlugin, AcceptFilterName, &acceptFilterFactoryType{}, true)
}
