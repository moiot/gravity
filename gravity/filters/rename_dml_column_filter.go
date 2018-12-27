package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
)

// [[filters]]
// type = "rename-dml-column"
// match-schema = "test"
// match-table = "test_table_2"
// from = ["b"]
// to = ["d"]

const RenameColumnFilterName = "rename-dml-column"

type renameDmlColumnFilter struct {
	BaseFilter
	from []string
	to   []string
}

func (f *renameDmlColumnFilter) Configure(data map[string]interface{}) error {
	err := f.ConfigureMatchers(data)
	if err != nil {
		return errors.Trace(err)
	}

	from, ok := data["from"]
	if !ok {
		return errors.Errorf("\"from\" is not configured")
	}

	to, ok := data["to"]
	if !ok {
		return errors.Errorf("\"to\" is not configured")
	}

	fromColumns, ok := from.([]string)
	if !ok {
		return errors.Errorf("\"from\" must be an array of string")
	}

	toColumns, ok := to.([]string)
	if !ok {
		return errors.Errorf("\"to\" must be an array of string")
	}

	if len(fromColumns) != len(toColumns) {
		return errors.Errorf("\"from\" should have the same length of \"to\"")
	}

	f.from = fromColumns
	f.to = toColumns

	return nil
}

func (f *renameDmlColumnFilter) Filter(msg *core.Msg) (continueNext bool, err error) {
	if !f.matchers.Match(msg) {
		return true, nil
	}

	if msg.DmlMsg == nil {
		return false, errors.Errorf("DmlMsg is null")
	}

	for i, fromColumn := range f.from {
		toColumn := f.to[i]

		// Data
		msg.DmlMsg.Data[toColumn] = msg.DmlMsg.Data[fromColumn]
		delete(msg.DmlMsg.Data, fromColumn)

		// Pks
		if msg.DmlMsg.Pks != nil {
			if _, ok := msg.DmlMsg.Pks[fromColumn]; ok {
				msg.DmlMsg.Pks[toColumn] = msg.DmlMsg.Pks[fromColumn]
				delete(msg.DmlMsg.Pks, fromColumn)
			}
		}
	}

	return true, nil
}

type renameDMLColumnFilterFactoryType struct{}

func (factory *renameDMLColumnFilterFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (factory *renameDMLColumnFilterFactoryType) NewFilter() core.IFilter {
	return &renameDmlColumnFilter{}
}

func init() {
	registry.RegisterPlugin(registry.FilterPlugin, RenameColumnFilterName, &renameDMLColumnFilterFactoryType{}, true)
}
