package mysql

import (
	"database/sql"
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/schema_store"
)

func BuildPkColumns(table *schema_store.Table, tableConfig *config.TableConfig) []schema_store.Column {
	if tableConfig == nil || len(tableConfig.PkOverride) == 0 {
		return table.PrimaryKeyColumns
	}

	ret := make([]schema_store.Column, len(tableConfig.PkOverride))
	for i, c := range tableConfig.PkOverride {
		col, ok := table.Column(c)
		if !ok {
			log.Fatalf("pk override column [%s] not found on source table.", c)
		}
		ret[i] = *col
	}
	return ret
}

func GenPrimaryKeys(pkColumns []schema_store.Column, rowData map[string]interface{}) (map[string]interface{}, error) {
	pks := make(map[string]interface{})
	for i := 0; i < len(pkColumns); i++ {
		pkName := pkColumns[i].Name
		pks[pkName] = rowData[pkName]
		if pks[pkName] == nil {
			return nil, errors.Errorf("primary key nil, pkName: %v", pkName)
		}
	}
	return pks, nil
}

//
// +	scanTypeFloat32   = reflect.TypeOf(float32(0))
// +	scanTypeFloat64   = reflect.TypeOf(float64(0))
// +	scanTypeInt8      = reflect.TypeOf(int8(0))
// +	scanTypeInt16     = reflect.TypeOf(int16(0))
// +	scanTypeInt32     = reflect.TypeOf(int32(0))
// +	scanTypeInt64     = reflect.TypeOf(int64(0))
// +	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
// +	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
// +	scanTypeNullTime  = reflect.TypeOf(NullTime{})
// +	scanTypeUint8     = reflect.TypeOf(uint8(0))
// +	scanTypeUint16    = reflect.TypeOf(uint16(0))
// +	scanTypeUint32    = reflect.TypeOf(uint32(0))
// +	scanTypeUint64    = reflect.TypeOf(uint64(0))
// +	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
// +	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
func SQLDataPtrs2Val(dataPtrs []interface{}, columnTypes []*sql.ColumnType) map[string]interface{} {
	ret := make(map[string]interface{})

	for i := range dataPtrs {
		columnName := columnTypes[i].Name()
		columnData := reflect.ValueOf(dataPtrs[i]).Elem().Interface()
		switch v := columnData.(type) {
		case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
			ret[columnName] = v
		case float32, float64:
			ret[columnName] = v
		case sql.NullInt64:
			if !v.Valid {
				ret[columnName] = nil
			} else {
				ret[columnName] = v.Int64
			}
		case sql.NullBool:
			if !v.Valid {
				ret[columnName] = nil
			} else {
				ret[columnName] = v.Bool
			}
		case sql.NullFloat64:
			if !v.Valid {
				ret[columnName] = nil
			} else {
				ret[columnName] = v.Float64
			}
		case sql.NullString:
			if !v.Valid {
				ret[columnName] = nil
			} else {
				ret[columnName] = v.String
			}
		case sql.RawBytes:
			if v == nil {
				ret[columnName] = nil
			} else {
				b := make([]byte, len(v))
				copy(b, v)
				ret[columnName] = b
			}
		case mysql.NullTime:
			if !v.Valid {
				ret[columnName] = nil
			} else {
				ret[columnName] = v.Time
			}
		default:
			log.Fatalf("failed to catch columnName: %v, type: %v", columnName, reflect.TypeOf(columnData))
		}
	}
	return ret
}

func MySQLDataEquals(a interface{}, b interface{}) bool {
	if reflect.TypeOf(a) == reflect.TypeOf(b) {
		return reflect.DeepEqual(a, b)
	}

	normalizedA := NormalizeSQLType(a)
	normalizedB := NormalizeSQLType(b)

	if reflect.TypeOf(normalizedA) != reflect.TypeOf(normalizedB) {
		log.Fatalf("MySQLDataEquals normalized type not match, a type: %v, b type: %v", reflect.TypeOf(a), reflect.TypeOf(b))
	}

	return reflect.DeepEqual(normalizedA, normalizedB)
}

func NormalizeSQLType(a interface{}) interface{} {
	switch v := a.(type) {
	case string:
		return sql.NullString{String: v, Valid: true}
	case int8:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case int16:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case int32:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case int64:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case uint8:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case uint16:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case uint32:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case uint64:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case int:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	default:
		return a
	}
}
