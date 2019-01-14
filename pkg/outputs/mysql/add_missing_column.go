package mysql

import (
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/schema_store"
)

func AddMissingColumn(msg *core.Msg, targetTableDef *schema_store.Table) (bool, error) {
	if msg.DmlMsg == nil || targetTableDef == nil {
		return true, nil
	}

	targetColumns := targetTableDef.Columns
	for i := range targetColumns {
		column := targetColumns[i]

		name := column.Name
		_, ok := msg.DmlMsg.Data[name]
		if !ok && !column.DefaultVal.IsNull {
			msg.DmlMsg.Data[name] = targetColumns[i].DefaultVal.ValueString
		}
	}
	return true, nil
}
