package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

// [[filters]]
// type = "delete-dml-column"
// [[filters.config]]
// match-schema = "test"
// match-table = "test_table"
// columns = ["e", "f"]

const DeleteDMLColumnFilterName = "delete-dml-column"

type deleteDmlColumnFilter struct {
	BaseFilter
	columns []string
}

func (f *deleteDmlColumnFilter) Configure(data map[string]interface{}) error {
	err := f.ConfigureMatchers(data)
	if err != nil {
		return errors.Trace(err)
	}

	columns, ok := data["columns"]
	if !ok {
		return errors.Errorf("'column' is not configured")
	}

	// columns can be any type of slice, for example:
	// []interface{}, []string{}
	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Errorf("'column' should be an array")
	}

	columnStrings, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Errorf("'column' should be an array of string")
	}

	f.columns = columnStrings
	return nil
}

func (f *deleteDmlColumnFilter) Filter(msg *core.Msg) (continueNext bool, err error) {
	if !f.Matchers.Match(msg) {
		return true, nil
	}

	if msg.DmlMsg == nil {
		return false, errors.Errorf("DmlMsg is null")
	}

	for _, name := range f.columns {
		delete(msg.DmlMsg.Data, name)

		if msg.DmlMsg.Old != nil {
			delete(msg.DmlMsg.Old, name)
		}

		if msg.DmlMsg.Pks != nil {
			delete(msg.DmlMsg.Pks, name)
		}

	}

	return true, nil
}

func (f *deleteDmlColumnFilter) Close() error {
	return nil
}

type deleteDMLColumnFilterFactoryType struct{}

func (factory *deleteDMLColumnFilterFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (factory *deleteDMLColumnFilterFactoryType) NewFilter() core.IFilter {
	return &deleteDmlColumnFilter{}
}

func init() {
	registry.RegisterPlugin(registry.FilterPlugin, DeleteDMLColumnFilterName, &deleteDMLColumnFilterFactoryType{}, true)
}
