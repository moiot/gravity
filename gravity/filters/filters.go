package filters

import (
	"reflect"

	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/registry"

	"github.com/moiot/gravity/pkg/core"
)

func NewFilters(filterConfigs []interface{}) ([]core.IFilter, error) {
	var retFilters []core.IFilter
	for _, filterData := range filterConfigs {
		filterDataMap, ok := filterData.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("filter should be a map")
		}

		filterType, ok := filterDataMap["type"]
		if !ok {
			return nil, errors.Errorf("unknown filter type: %v", filterDataMap["type"])
		}

		filterTypeString, ok := filterType.(string)
		if !ok {
			return nil, errors.Errorf("filter type should be a string")
		}

		if filterTypeString == "go-plugin" {
			name, p, err := registry.DownloadGoPlugin(filterDataMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
			registry.RegisterPlugin(registry.FilterPlugin, name, p, true)
		}

		factory, err := registry.GetPlugin(registry.FilterPlugin, filterTypeString)
		if err != nil {
			return nil, errors.Trace(err)
		}

		filterFactory, ok := factory.(core.IFilterFactory)
		if !ok {
			return nil, errors.Errorf("wrong type: %v", reflect.TypeOf(factory))
		}

		f := filterFactory.NewFilter()

		delete(filterDataMap, "type")
		if err := f.Configure(filterDataMap); err != nil {
			return nil, errors.Trace(err)
		}

		retFilters = append(retFilters, f)
	}
	return retFilters, nil
}
