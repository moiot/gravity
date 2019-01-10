package filters

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/utils"

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
		return errors.Errorf("'from' is not configured")
	}

	to, ok := data["to"]
	if !ok {
		return errors.Errorf("'to' is not configured")
	}

	fromColumns, ok := utils.CastToSlice(from)
	if !ok {
		return errors.Errorf("'from' must be an array")
	}

	fromColumnStrings, err := utils.CastSliceInterfaceToSliceString(fromColumns)
	if err != nil {
		return errors.Errorf("'from' should be an array of string")
	}

	toColumns, ok := utils.CastToSlice(to)
	if !ok {
		return errors.Errorf("'to' must be an array")
	}

	toColumnStrings, err := utils.CastSliceInterfaceToSliceString(toColumns)
	if err != nil {
		return errors.Errorf("'to' should be an array of string")
	}

	if len(fromColumns) != len(toColumns) {
		return errors.Errorf("\"from\" should have the same length of \"to\"")
	}

	f.from = fromColumnStrings
	f.to = toColumnStrings

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

		// Old
		if msg.DmlMsg.Old != nil {
			msg.DmlMsg.Old[toColumn] = msg.DmlMsg.Old[fromColumn]
			delete(msg.DmlMsg.Old, fromColumn)
		}

		// Pks
		if msg.DmlMsg.Pks != nil {
			if _, ok := msg.DmlMsg.Pks[fromColumn]; ok {
				msg.DmlMsg.Pks[toColumn] = msg.DmlMsg.Pks[fromColumn]
				delete(msg.DmlMsg.Pks, fromColumn)
			}
		}

		// pkColumns
		for j, c := range msg.DmlMsg.PkColumns {
			if fromColumn == c {
				msg.DmlMsg.PkColumns[j] = toColumn
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
