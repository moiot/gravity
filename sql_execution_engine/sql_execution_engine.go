package sql_execution_engine

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"time"

	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"
)

const (
	MySQLReplaceEngine       = "mysql_replace"
	BiDirectionShadingEngine = "bidirection_shading"
	BiDirectionEngine        = "bidirection"
	ConflictEngine           = "conflict"
	ManualEngine             = "manual"
)

type MySQLExecutionEngineConfig struct {
	EngineType       string `mapstructure:"type" json:"type"`
	EnableDDL        bool   `mapstructure:"enable-ddl" json:"enable-ddl"`
	UseBidirection   bool   `mapstructure:"use-bidirection" json:"use-bidirection"`
	UseShadingProxy  bool   `mapstructure:"use-shading-proxy"  json:"use-shading-proxy"`
	DetectConflict   bool   `mapstructure:"detect-conflict"json:"detect-conflict"`
	MaxConflictRetry int    `mapstructure:"max-conflict-retry"json:"max-conflict-retry"`
	OverrideConflict bool   `mapstructure:"override-conflict"json:"override-conflict"`
	SQLTemplate        string   `mapstructure:"sql-template" json:"sql-template"`
	SQLArgExpr         []string `mapstructure:"sql-arg-expr" json:"sql-arg-expr"`
}

type SQlExecutionEngine interface {
	Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error
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

func GetBatchSize(defaultBatchSize int, DetectConflict bool, UseShadingProxy bool) (int, error) {
	// normalize batch size for these two special mode.
	if DetectConflict || UseShadingProxy {
		if defaultBatchSize <= 0 {
			defaultBatchSize = 1
		} else if defaultBatchSize > 1 {
			return 0, errors.Errorf("batch size per table must be 1 when detect conflict or shading proxy enabled")
		}
	}

	if defaultBatchSize <= 0 {
		return 10, nil
	} else {
		return defaultBatchSize, nil
	}
}

func SelectEngine(DetectConflict bool, UserBidirection bool, UseShadingProxy bool) (string, error) {
	var engine string
	if !DetectConflict && !UserBidirection && !UseShadingProxy {
		engine = MySQLReplaceEngine
	} else if UserBidirection && !UseShadingProxy {
		engine = BiDirectionEngine
	} else if UserBidirection && UseShadingProxy {
		engine = BiDirectionShadingEngine
	} else if DetectConflict && !UserBidirection && !UseShadingProxy {
		engine = ConflictEngine
	} else {
		return "", errors.BadRequestf("No match sql execution engine found for detect conflict[%t], bidirection[%t], shading proxy[%t]", DetectConflict, UserBidirection, UseShadingProxy)
	}
	log.Infof("SelectEngine: %v", engine)
	return engine, nil
}

func NewSQLExecutionEngine(db *sql.DB, engineConfig MySQLExecutionEngineConfig) SQlExecutionEngine {
	var engine SQlExecutionEngine
	name := engineConfig.EngineType
	switch name {
	case MySQLReplaceEngine:
		engine = NewMySQLReplaceEngine(db)
	case BiDirectionEngine:
		engine = NewBidirectionEngine(db, utils.Stmt)
	case BiDirectionShadingEngine:
		engine = NewBidirectionEngine(db, utils.Annotation)
	case ConflictEngine:
		engine = NewConflictEngine(db, engineConfig.OverrideConflict, engineConfig.MaxConflictRetry, 1*time.Second, false)
	case ManualEngine:
		engine = NewManualSQLEngine(db, engineConfig)
	default:
		log.Fatal("unknown sql execution engine ", name)
		return nil
	}

	return engine
}

func GenerateSingleReplaceSQL(msg *core.Msg, tableDef *schema_store.Table) (string, []interface{}, error) {
	if err := ValidateSchema(msg, tableDef); err != nil {
		return "", nil, errors.Trace(err)
	}
	data := msg.DmlMsg.Data
	columnNames := make([]string, len(data))
	var placeHolders []string
	var args = make([]interface{}, len(data))

	// When generating sql, we use the data as a guide.
	// See the test case for details.
	idx := 0
	for dataColumnName, columnData := range data {
		columnNames[idx] = fmt.Sprintf("`%s`", dataColumnName)
		args[idx] = adjustArgs(columnData, tableDef.MustColumn(dataColumnName))
		placeHolders = append(placeHolders, "?")
		idx++
	}

	stmt := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s)", tableDef.Schema, tableDef.Name, strings.Join(columnNames, ","), strings.Join(placeHolders, ","))
	return stmt, args, nil

}
func adjustArgs(arg interface{}, column *schema_store.Column) interface{} {
	if arg == nil {
		return arg
	}
	if column.IsDatetime() { // datetime is in utc and should ignore location
		return arg.(time.Time).Format("2006-01-02 15:04:05.999999999")
	}
	return arg
}

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
	columnNames := make([]string, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnIdx := column.Idx
		columnNames[columnIdx] = fmt.Sprintf("`%s`", columnName)
	}

	sqlPrefix := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES", tableDef.Schema, tableDef.Name, strings.Join(columnNames, ","))

	// Generate place holders and args
	batchPlaceHolders, args, err := PlaceHoldersAndArgsFromEncodedData(msgBatch, tableDef)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	finalPlaceHolders := strings.Join(batchPlaceHolders, ",")
	s := []string{sqlPrefix, finalPlaceHolders}
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
	args := make([]interface{}, len(tableDef.Columns))

	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnIdx := column.Idx
		columnData, ok := data[columnName]
		if !ok {
			return "", nil, errors.Errorf("columnName: %v data is nil, data: %v", columnName, data)
		}
		args[columnIdx] = adjustArgs(columnData, tableDef.MustColumn(columnName))
		placeHolders = append(placeHolders, "?")
	}
	singleSqlPlaceHolder := fmt.Sprintf("(%s)", strings.Join(placeHolders, ","))
	return singleSqlPlaceHolder, args, nil
}

func ValidateSchema(msg *core.Msg, tableDef *schema_store.Table) error {
	columnLenInMsg := len(msg.DmlMsg.Data)
	columnLenInTarget := len(tableDef.Columns)

	if columnLenInMsg != columnLenInTarget {
		return errors.Errorf("columnLenInMsg %d columnLenInTarget %d not equal", columnLenInMsg, columnLenInTarget)
	}

	return nil
}

type execResult int

const (
	execSuccess execResult = iota
	execFailConflict
	execFailOther
)

func exec(db *sql.DB, stmt string, args ...interface{}) (execResult, error) {
	ret, err := db.Exec(stmt, args...)
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if ok && me.Number == 1062 { //refer to https://dev.mysql.com/doc/refman/5.7/en/error-messages-server.html
			return execFailConflict, nil
		}
		return execFailOther, err
	}

	affected, err := ret.RowsAffected()
	if err != nil {
		return execFailOther, err
	}

	if affected == 0 {
		return execFailConflict, nil
	}

	return execSuccess, nil
}
