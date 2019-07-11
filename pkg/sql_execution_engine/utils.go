package sql_execution_engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
)

func GenerateSingleDeleteSQL(msg *core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {
	primaryKeyData := msg.DmlMsg.Pks
	var whereSql []string

	var args []interface{}
	for _, column := range tableDef.Columns {
		columnName := column.Name
		pkData, ok := primaryKeyData[columnName]
		if !ok {
			continue
		}

		whereSql = append(whereSql, fmt.Sprintf("`%s` = ?", columnName))
		args = append(args, adjustArgs(pkData, tableDef.MustColumn(columnName)))
	}
	if len(whereSql) == 0 {
		return "", nil, errors.Errorf("where sql is empty, probably missing pk")
	}

	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s", tableDef.Schema, tableDef.Name, strings.Join(whereSql, " AND "))
	return stmt, args, nil
}

func GenerateReplaceSQLWithMultipleValues(msgBatch []*core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {
	// Generate place holders and args
	batchPlaceHolders, args, err := PlaceHoldersAndArgsFromEncodedData(msgBatch, tableDef)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	finalPlaceHolders := strings.Join(batchPlaceHolders, ",")
	s := []string{tableDef.ReplaceSqlPrefix(), finalPlaceHolders}
	return strings.Join(s, " "), args, nil
}

func PlaceHoldersAndArgsFromEncodedData(msgBatch []*core.Msg, tableDef *schema_store.Table) ([]string, []interface{}, error) {
	var batchPlaceHolders []string
	var batchArgs []interface{}

	for _, msg := range msgBatch {
		if err := ValidateSchema(msg, tableDef); err != nil {
			return nil, nil, errors.Trace(err)
		}

		if msg.DmlMsg.Data == nil {
			return nil, nil, errors.Errorf("Data and MysqlRawBytes are null")
		}
		// Use map[string]interface{} from "Data" field
		singleSqlPlaceHolders, singleSqlArgs, err := GetSingleSqlPlaceHolderAndArgWithEncodedData(msg, tableDef)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		batchPlaceHolders = append(batchPlaceHolders, singleSqlPlaceHolders)
		batchArgs = append(batchArgs, singleSqlArgs...)
	}
	return batchPlaceHolders, batchArgs, nil
}

func GetSingleSqlPlaceHolderAndArgWithEncodedData(msg *core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {
	if err := ValidateSchema(msg, tableDef); err != nil {
		return "", nil, errors.Trace(err)
	}

	if msg.DmlMsg.Operation != core.Insert && msg.DmlMsg.Operation != core.Update {
		return "", nil, errors.Errorf("unsupported msg type: %v", msg.DmlMsg.Operation)
	}

	data := msg.DmlMsg.Data

	var placeHolders []string
	var args []interface{}

	for _, column := range tableDef.Columns {
		if column.IsGenerated {
			placeHolders = append(placeHolders, "DEFAULT")
		} else {
			columnName := column.Name
			columnData, ok := data[columnName]
			if !ok {
				placeHolders = append(placeHolders, "DEFAULT")
			} else {
				args = append(args, adjustArgs(columnData, &column))
				placeHolders = append(placeHolders, "?")
			}
		}
	}
	singleSqlPlaceHolder := fmt.Sprintf("(%s)", strings.Join(placeHolders, ","))
	return singleSqlPlaceHolder, args, nil
}

func GenerateInsertIgnoreSQL(msgBatch []*core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {
	columnNames := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		//columnIdx := column.Idx
		columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
	}

	sqlPrefix := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES", tableDef.Schema, tableDef.Name, strings.Join(columnNames, ","))
	// Generate place holders and args
	batchPlaceHolders, args, err := PlaceHoldersAndArgsFromEncodedData(msgBatch, tableDef)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	finalPlaceHolders := strings.Join(batchPlaceHolders, ",")
	s := []string{sqlPrefix, finalPlaceHolders}
	return strings.Join(s, " "), args, nil
}

func GenerateInsertOnDuplicateKeyUpdate(msgBatch []*core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {

	msg := msgBatch[0]

	if err := ValidateSchema(msg, tableDef); err != nil {
		return "", nil, errors.Trace(err)
	}

	// we only allow operations that:
	// 1. has primary key, and dont have primary key change
	// 2. insert/update. (DELETE is not supported)
	if msg.DmlMsg.Operation != core.Insert && msg.DmlMsg.Operation != core.Update {
		return "", nil, errors.Errorf("only support insert/update: operation: %v", msg.DmlMsg.Operation)
	}

	if len(msg.DmlMsg.Pks) <= 0 {
		return "", nil, errors.Errorf("only support data has primary key")
	}

	if len(msg.OutputDepHashes) > 1 {
		return "", nil, errors.Errorf("do not support unique key change")
	}

	allColumnNamesInSQL := make([]string, 0, len(tableDef.Columns))
	allColumnPlaceHolder := make([]string, len(tableDef.Columns))
	args := make([]interface{}, 0, len(tableDef.Columns))
	for i := range allColumnPlaceHolder {
		allColumnPlaceHolder[i] = "?"
	}
	// update columns
	updateColumnsIdx := 0
	columnNamesAssignWithoutPks := make([]string, len(tableDef.Columns)-len(tableDef.PrimaryKeyColumns))
	argsWithoutPks := make([]interface{}, len(tableDef.Columns)-len(tableDef.PrimaryKeyColumns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnNameInSQL := fmt.Sprintf("`%s`", columnName)
		allColumnNamesInSQL = append(allColumnNamesInSQL, columnNameInSQL)
		columnData := msg.DmlMsg.Data[columnName]
		args = append(args, adjustArgs(columnData, tableDef.MustColumn(columnName)))
		_, ok := msg.DmlMsg.Pks[columnName]
		if !ok {
			columnNamesAssignWithoutPks[updateColumnsIdx] = fmt.Sprintf("%s = ?", columnNameInSQL)
			columnData := msg.DmlMsg.Data[columnName]
			argsWithoutPks[updateColumnsIdx] = adjustArgs(columnData, tableDef.MustColumn(columnName))
			updateColumnsIdx++
		}
	}

	sqlInsert := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		tableDef.Schema,
		tableDef.Name,
		strings.Join(allColumnNamesInSQL, ","),
		strings.Join(allColumnPlaceHolder, ","))
	sqlUpdate := fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(columnNamesAssignWithoutPks, ","))

	return fmt.Sprintf("%s %s", sqlInsert, sqlUpdate), append(args, argsWithoutPks...), nil
}

func ValidateSchema(msg *core.Msg, tableDef *schema_store.Table) error {
	columnLenInMsg := len(msg.DmlMsg.Data)
	columnLenInTarget := len(tableDef.Columns)

	if columnLenInMsg != columnLenInTarget {
		return errors.Errorf("%s.%s: columnLenInMsg %d columnLenInTarget %d not equal", tableDef.Schema, tableDef.Name, columnLenInMsg, columnLenInTarget)
	}

	return nil
}

func DebugDmlMsg(msgBatch []*core.Msg) (interface{}, interface{}) {
	var debugData []map[string]interface{}
	var debugPks []map[string]interface{}

	for _, msg := range msgBatch {
		debugData = append(debugData, msg.DmlMsg.Data)
		debugPks = append(debugPks, msg.DmlMsg.Pks)
	}

	return debugData, debugPks
}

func SQLWithAnnotation(sql string, annotationContent string) string {
	return fmt.Sprintf("/*%s*/%s", annotationContent, sql)
}

func adjustArgs(arg interface{}, column *schema_store.Column) interface{} {
	if arg == nil {
		return arg
	}
	if column.Type == schema_store.TypeDatetime { // datetime is in utc and should ignore location
		// zero value will be string
		t, ok := arg.(time.Time)
		if ok && !t.IsZero() {
			return t.Format("2006-01-02 15:04:05.999999999")
		}
	}
	// mysql driver doesn't handle object id correctly, it reports
	// Error 1366: Incorrect string value: '\\x99\\x8B\\x1E\\x0B\\x16\\xA8...' for column '_id' at row 1
	if bid, ok := arg.(bson.ObjectId); ok {
		return bid.Hex()
	}
	return arg
}

func execSql(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return errors.Trace(err)
}

func NewEngineExecutor(pipelineName string, engineName string, db *sql.DB, data map[string]interface{}) EngineExecutor {
	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, engineName)
	if err != nil {
		panic("failed to get replace engine")
	}

	err = p.Configure(pipelineName, data)
	if err != nil {
		logrus.Fatalf("[mysqlReplaceEngine] failed to config")
	}

	i, ok := p.(EngineInitializer)
	if !ok {
		logrus.Fatalf("[mysqlReplaceEngine] not a EngineInitializer")
	}

	if err := i.Init(db); err != nil {
		logrus.Fatalf("[mysqlReplaceEngine] init failed: %v", errors.ErrorStack(err))
	}

	executor, ok := p.(EngineExecutor)
	if !ok {
		logrus.Fatalf("[mysqlReplaceEngine] not a EngineExecutor")
	}

	return executor
}
