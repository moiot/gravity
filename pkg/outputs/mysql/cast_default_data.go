package mysql

// import (
// 	"github.com/json-iterator/go"
// 	"github.com/juju/errors"
//
// 	"github.com/moiot/gravity/pkg/core"
//
// 	"github.com/moiot/gravity/schema_store"
// )
//
//
//
// func CastDefaultData(msg *core.Msg, targetTableDef *schema_store.Table) (bool, error) {
// 	if msg.DmlMsg == nil || targetTableDef == nil {
// 		return true, nil
// 	}
//
// 	for columnName := range msg.DmlMsg.Data {
// 		targetColumn, ok := targetTableDef.Column(columnName)
// 		if !ok {
// 			return false, errors.Errorf("column %s not found", columnName)
// 		}
//
// 		if msg.TableDef == nil {
// 			s, _ := jsoniter.MarshalToString(msg)
// 			return false, errors.Errorf("source table def is nil. msg: %s", s)
// 		}
//
// 		columnDef := msg.TableDef.MustColumn(columnName)
// 		value := msg.DmlMsg.Data[columnName]
// 		if value == nil && columnDef.DefaultVal.IsNull && !targetColumn.DefaultVal.IsNull {
// 			msg.DmlMsg.Data[columnName] = targetColumn.DefaultVal.ValueString
// 		}
// 	}
// 	return true, nil
// }
