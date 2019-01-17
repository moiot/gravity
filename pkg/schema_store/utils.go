package schema_store

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils"
)

func GetCreateSchemaStatement(db *sql.DB, schemaName string) (error, string) {
	var createDBStatement string
	var name string
	query := fmt.Sprintf("SHOW CREATE DATABASE `%s`", schemaName)
	tableQueryRow := db.QueryRow(query)
	err := tableQueryRow.Scan(&name, &createDBStatement)
	if err != nil {
		return errors.Trace(err), ""
	}

	return nil, createDBStatement
}

func GetSchemaFromDB(db *sql.DB, dbName string) (Schema, error) {
	schema := make(map[string]*Table)

	tables, err := GetTablesFromDB(db, dbName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var wg sync.WaitGroup
	var tableDefErr error
	var mux sync.Mutex

	for _, tableName := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()

			table, err := GetTableDefFromDB(db, dbName, t)
			if err != nil {
				e := errors.Cause(err)
				switch e := e.(type) {
				case *mysql.MySQLError:
					if e.Number == 1146 {
						log.Error(err)
					} else {
						tableDefErr = err
					}
				default:
					tableDefErr = err
				}
				return
			}

			mux.Lock()
			schema[t] = table
			mux.Unlock()
		}(tableName)
	}

	wg.Wait()

	if tableDefErr != nil {
		return nil, errors.Trace(tableDefErr)
	}

	return schema, nil
}

func GetTablesFromDB(db *sql.DB, schema string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("show full tables from `%s` where Table_type != 'VIEW'", schema))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var tableName string
	var tableType string
	var ret []string
	for rows.Next() {
		err := rows.Scan(&tableName, &tableType)
		if err != nil {
			return nil, errors.Trace(err)
		}

		ret = append(ret, tableName)
	}

	return ret, nil
}

func getUniqueKeysFromDB(db *sql.DB, dbName string, tableName string) (map[string][]string, error) {
	resultRows, err := utils.GetIndexRows(db, fmt.Sprintf("SHOW INDEX FROM `%s`.`%s` WHERE Non_unique = 0", dbName, tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	uniqueKeyMap := map[string][]string{}
	for _, value := range resultRows {
		keyName := value[2].String
		columnName := value[4].String
		if columns, ok := uniqueKeyMap[keyName]; ok {
			columns = append(columns, columnName)
		} else {
			uniqueKeyMap[keyName] = []string{columnName}
		}

	}
	return uniqueKeyMap, nil
}

func GetTableDefFromDB(db *sql.DB, dbName string, tableName string) (*Table, error) {

	var columnName string
	var columnType string
	var columnKey string
	var ordinalPos = 0
	var isNullableString string
	var defaultVal sql.NullString
	var extra sql.NullString
	var err error
	var t = Table{Schema: dbName, Name: tableName, Columns: make([]Column, 0), PrimaryKeyColumns: make([]Column, 0), columnMap: make(map[string]*Column)}

	t.UniqueKeyColumnMap, err = getUniqueKeysFromDB(db, dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stmt := fmt.Sprintf("show columns from `%s`.`%s`", dbName, tableName)
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, errors.Annotatef(err, "error %s", stmt)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &isNullableString, &columnKey, &defaultVal, &extra)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var column = Column{Idx: ordinalPos, Name: columnName, ColType: columnType}

		// There might be a situation when the column cannot be NULL,
		// but there is no default value defined
		column.DefaultVal.IsNull = !defaultVal.Valid
		column.DefaultVal.ValueString = defaultVal.String

		ordinalPos++

		var isNullable bool
		var isUnsigned bool
		if isNullableString == "NO" {
			isNullable = false
		} else {
			isNullable = true
		}

		if strings.Contains(columnType, "unsigned") {
			isUnsigned = true
		} else {
			isUnsigned = false
		}
		column.IsUnsigned = isUnsigned
		column.IsNullable = isNullable
		if columnKey == "PRI" {
			t.PrimaryKeyColumns = append(t.PrimaryKeyColumns, column)
			column.IsPrimaryKey = true
		} else {
			column.IsPrimaryKey = false
		}
		// some validation
		//if !column.IsNullable && column.DefaultVal.IsNull {
		//	return errors.Errorf("column %v cannot be NULL, but default value is NULL", column.Name), nil
		//}

		t.Columns = append(t.Columns, column)
		t.columnMap[column.Name] = &t.Columns[len(t.Columns)-1]
	}

	return &t, nil
}
