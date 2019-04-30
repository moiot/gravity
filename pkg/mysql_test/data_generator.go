package mysql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/mysql_test/types"
)

type MysqlTableDataGenerator struct {
	db               *sql.DB
	schema           string
	table            string
	columns          []Column
	generators       []types.ColumnValGenerator
	colIndex         map[string]int
	primaryKeyName   string
	columnNames      string
	placeholders     string
	generatedPrimary []interface{}
	*sync.Mutex
}

type Column struct {
	name     string
	sType    string
	nullable bool
	primary  bool
}

func NewMysqlTableDataGenerator(db *sql.DB, schema string, table string) MysqlTableDataGenerator {
	ret := MysqlTableDataGenerator{
		db:       db,
		schema:   schema,
		table:    table,
		colIndex: make(map[string]int),
		Mutex:    &sync.Mutex{},
	}

	rows, err := db.Query(fmt.Sprintf("show columns from `%s`.`%s`", schema, table))
	if err != nil {
		log.Fatal(errors.Trace(err))
	}

	var columnName string
	var columnType string
	var columnKey string
	var isNullableString string
	var defaultVal sql.NullString
	var extra sql.NullString

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &isNullableString, &columnKey, &defaultVal, &extra)
		if err != nil {
			log.Fatal(errors.Trace(err))
		}
		c := Column{
			name:  columnName,
			sType: columnType,
		}
		if strings.ToUpper(isNullableString) == "YES" {
			c.nullable = true
		}
		if columnKey == "PRI" {
			c.primary = true
			ret.primaryKeyName = columnName
		}
		ret.columns = append(ret.columns, c)
		ret.colIndex[columnName] = len(ret.columns) - 1

		generator := newColValGenerator(columnType)
		if c.nullable {
			generator = types.Nullable(generator)
		}
		ret.generators = append(ret.generators, generator)
	}

	columnNames := make([]string, len(ret.columns), len(ret.columns))
	placeholders := make([]string, len(ret.columns), len(ret.columns))
	for i, c := range ret.columns {
		columnNames[i] = c.name
		placeholders[i] = "?"
	}
	ret.columnNames = strings.Join(columnNames, ", ")
	ret.placeholders = strings.Join(placeholders, ", ")
	return ret
}

func (g *MysqlTableDataGenerator) InitData(num int, r *rand.Rand) (stmt string, args []interface{}) {
	g.generatedPrimary = make([]interface{}, 0, num)
	args = make([]interface{}, len(g.columns)*num, len(g.columns)*num)
	placeholder := fmt.Sprintf("(%s)", g.placeholders)
	b := bytes.NewBufferString(fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES ", g.schema, g.table, g.columnNames))

	for i := 0; i < num; i++ {
		b.WriteString(placeholder)
		if i < num-1 {
			b.WriteString(", ")
		}

		for j := 0; j < len(g.columns); j++ {
			v := g.generators[j].Generate(r)
			args[i*len(g.columns)+j] = v
			if g.columns[j].primary {
				g.generatedPrimary = append(g.generatedPrimary, v)
			}
		}
	}

	stmt = b.String()
	return
}

func (g *MysqlTableDataGenerator) RandomStmt(deleteRatio float32, insertRatio float32, r *rand.Rand) (string, []interface{}) {
	g.Lock()
	defer g.Unlock()

	ratio := r.Float32()
	if len(g.generatedPrimary) > 0 {
		pkIdx := r.Intn(len(g.generatedPrimary))
		pk := g.generatedPrimary[pkIdx]

		if ratio < deleteRatio {
			// delete
			g.generatedPrimary = append(g.generatedPrimary[:pkIdx], g.generatedPrimary[pkIdx+1:]...)
			return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` = ?", g.schema, g.table, g.primaryKeyName), []interface{}{pk}
		} else if ratio > (deleteRatio + insertRatio) {
			if ratio < deleteRatio+insertRatio+0.05 {
				// update pk
				stmt := fmt.Sprintf("UPDATE `%s`.`%s` set `%s` = ? WHERE `%s` = ?", g.schema, g.table, g.primaryKeyName, g.primaryKeyName)
				args := make([]interface{}, 2, 2)
				args[1] = pk
				for i := 0; i < len(g.columns); i++ {
					if g.columns[i].primary {
						args[0] = g.generators[i].Generate(r)
						g.generatedPrimary[pkIdx] = args[0]
						break
					}
				}
				return stmt, args
			}
			// normal update
			args := make([]interface{}, len(g.columns), len(g.columns))
			for i := 0; i < len(g.columns); i++ {
				if g.columns[i].primary {
					args[i] = pk
				} else {
					v := g.generators[i].Generate(r)
					args[i] = v
				}
			}
			stmt := fmt.Sprintf("REPLACE INTO `%s`.`%s`(%s) VALUES (%s)", g.schema, g.table, g.columnNames, g.placeholders)
			return stmt, args
		}
	}

	// insert
	args := make([]interface{}, len(g.columns), len(g.columns))
	for i := 0; i < len(g.columns); i++ {
		v := g.generators[i].Generate(r)
		args[i] = v
		if g.columns[i].primary {
			g.generatedPrimary = append(g.generatedPrimary, v)
		}
	}
	stmt := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES (%s)", g.schema, g.table, g.columnNames, g.placeholders)
	return stmt, args
}

func newColValGenerator(colType string) types.ColumnValGenerator {
	colType = strings.ToLower(colType)
	if strings.HasPrefix(colType, "tinyint") {
		return types.NewIntegerColumn(colType, isSigned(colType), 1)
	} else if strings.HasPrefix(colType, "smallint") {
		return types.NewIntegerColumn(colType, isSigned(colType), 2)
	} else if strings.HasPrefix(colType, "mediumint") {
		return types.NewIntegerColumn(colType, isSigned(colType), 3)
	} else if strings.HasPrefix(colType, "int") {
		return types.NewIntegerColumn(colType, isSigned(colType), 4)
	} else if strings.HasPrefix(colType, "bigint") {
		return types.NewIntegerColumn(colType, isSigned(colType), 8)
	} else if strings.HasPrefix(colType, "decimal") {
		return types.NewDecimalCol(colType)
	} else if strings.HasPrefix(colType, "float") || strings.HasPrefix(colType, "double") {
		return types.NewFloatCol(colType)
	} else if strings.HasPrefix(colType, "bit") {
		return types.NewBitCol(colType)
	} else if strings.HasPrefix(colType, "datetime") {
		return types.NewDateTimeCol(colType)
	} else if strings.HasPrefix(colType, "timestamp") {
		return types.NewTimestampCol(colType)
	} else if strings.HasPrefix(colType, "char") || strings.HasPrefix(colType, "varchar") {
		return types.NewStringCol(colType)
	} else if strings.HasPrefix(colType, "tinytext") {
		return types.NewTextCol(colType, 1<<8-1)
	} else if strings.HasPrefix(colType, "text") {
		return types.NewTextCol(colType, 1<<16-1)
	} else if strings.HasPrefix(colType, "mediumtext") {
		return types.NewTextCol(colType, 1<<24-1)
	} else if strings.HasPrefix(colType, "longtext") {
		return types.NewTextCol(colType, 1<<32-1)
	} else if strings.HasPrefix(colType, "tinyblob") {
		return types.NewBlobCol(colType, 1<<8-1)
	} else if strings.HasPrefix(colType, "blob") {
		return types.NewBlobCol(colType, 1<<16-1)
	} else if strings.HasPrefix(colType, "json") {
		return types.NewJSONCol(colType)
	}

	log.Fatalf("newColDataGenerator unknown column type %s", colType)
	return nil
}

func isSigned(col string) bool {
	return !strings.Contains(col, "unsigned")
}
