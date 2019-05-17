package schema_store

import (
	"fmt"
	"strings"

	"sync"

	"github.com/sirupsen/logrus"
)

// ColumnValueString is the same as sql.NullString
// We define it here in case we want to extend it.
type ColumnValueString struct {
	ValueString string `json:"value"`
	IsNull      bool   `json:"is_null"`
}

func (v1 *ColumnValueString) Equals(v2 *ColumnValueString) bool {
	if v1.IsNull && v2.IsNull {
		return true
	}

	if !v1.IsNull && !v2.IsNull && v1.ValueString == v2.ValueString {
		return true
	}

	return false
}

func (col Column) EqualsDefault(value interface{}) bool {
	if col.DefaultVal.IsNull && value == nil {
		return true
	}

	if !col.DefaultVal.IsNull && col.DefaultVal.ValueString == fmt.Sprint(value) { //TODO serialize value to string
		return true
	}

	return false
}

func (col Column) IsTimestamp() bool {
	return strings.Contains(strings.ToLower(col.ColType), "timestamp")
}

func (col Column) IsDatetime() bool {
	return strings.Contains(strings.ToLower(col.ColType), "datetime")
}

// Column
type Column struct {
	//Idx          int               `json:"idx"`
	Name         string            `json:"name"`
	ColType      string            `json:"col_type"`
	DefaultVal   ColumnValueString `json:"default_value_string"`
	IsNullable   bool              `json:"is_nullable"`
	IsUnsigned   bool              `json:"is_unsigned"`
	IsPrimaryKey bool              `json:"is_primary_key"`
	IsGenerated  bool              `json:"is_generated"`
}

// Table
type Table struct {
	Version string `json:"version"`
	Schema  string `json:"db_name"`
	Name    string `json:"table_name"`

	Columns            []Column            `json:"columns"`
	PrimaryKeyColumns  []Column            `json:"primary_key_columns"`
	UniqueKeyColumnMap map[string][]string `json:"unique_key_columns"`
	columnMap          map[string]*Column
	once               sync.Once
}

type Schema map[string]*Table

func (t *Table) RenameColumn(origin, target string) {
	c, ok := t.Column(origin)
	if ok {
		c.Name = target
		delete(t.columnMap, origin)
		t.columnMap[target] = c
	}
}

func (t *Table) ColumnNames() []string {
	var names []string
	for _, column := range t.Columns {
		names = append(names, column.Name)

	}
	return names
}

func (t *Table) Column(col string) (c *Column, ok bool) {
	t.once.Do(func() {
		if t.columnMap == nil {
			t.columnMap = make(map[string]*Column)
			for _, c := range t.Columns {
				t.columnMap[c.Name] = new(Column)
				*t.columnMap[c.Name] = c
			}
		}
	})
	c, ok = t.columnMap[col]
	return
}

func (t *Table) MustColumn(col string) *Column {
	c, ok := t.Column(col)
	if !ok {
		logrus.Fatalf("can't find column %s", col)
	}
	return c
}

func Deserialize(raw interface{}, column Column) interface{} {
	// fix issue: https://github.com/siddontang/go-mysql/issues/242
	if raw == nil {
		return nil
	}

	if column.ColType == "text" {
		return string(raw.([]uint8))
	} else {
		return raw
	}
}

type SchemaStore interface {
	GetSchema(database string) (Schema, error)
	InvalidateSchemaCache(schema string)
	InvalidateCache()
	IsInCache(dbName string) bool
	Close()
}
