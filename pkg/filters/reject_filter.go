package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const RejectFilterName = "reject"

type rejectFilterType struct {
	BaseFilter
}

func (f *rejectFilterType) Configure(configData map[string]interface{}) error {
	err := f.ConfigureMatchers(configData)
	if err != nil {
		return errors.Trace(err)
	}
	// we can add more validation for filter's args here.
	return nil
}

func (f *rejectFilterType) Filter(msg *core.Msg) (continueNext bool, err error) {
	if f.Matchers.Match(msg) {
		return false, nil
	}

	return true, nil
}

type rejectFilterFactoryType struct{}

func (factory *rejectFilterFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (factory *rejectFilterFactoryType) NewFilter() core.IFilter {
	return &rejectFilterType{}
}

func init() {
	registry.RegisterPlugin(registry.FilterPlugin, RejectFilterName, &rejectFilterFactoryType{}, true)
}
