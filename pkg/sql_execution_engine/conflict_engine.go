package sql_execution_engine

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/registry"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"time"

	"github.com/moiot/gravity/pkg/schema_store"
)

const (
	ConflictDetectEngine = "conflict-detect-engine"

	ConflictFileName = "conflict.log"
)

type conflictEngineConfig struct {
	MaxConflictRetry         int           `mapstructure:"max-conflict-retry" json:"max-conflict-retry"`
	OverrideConflict         bool          `mapstructure:"override-conflict" json:"override-conflict"`
	EnableDelete             bool          `mapstructure:"enable-delete" json:"enable-delete"`
	RetrySleepDurationString string        `mapstructure:"retry-sleep-duration" json:"retry-sleep-duration"`
	RetrySleepDuration       time.Duration `mapstructure:"-" json:"-"`
}

type conflictEngine struct {
	pipelineName string

	cfg *conflictEngineConfig

	db *sql.DB

	conflictLog *log.Logger
}

func init() {
	registry.RegisterPlugin(registry.SQLExecutionEnginePlugin, ConflictDetectEngine, &conflictEngine{}, false)
}

func (e *conflictEngine) Configure(pipelineName string, data map[string]interface{}) error {
	e.pipelineName = pipelineName

	cfg := conflictEngineConfig{}
	if err := mapstructure.Decode(data, &cfg); err != nil {
		return errors.Trace(err)
	}

	if cfg.RetrySleepDurationString == "" {
		cfg.RetrySleepDurationString = "1s"
	}

	d, err := time.ParseDuration(cfg.RetrySleepDurationString)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RetrySleepDuration = d

	e.cfg = &cfg

	// setup conflict log
	conflictLog := log.New()
	file, err := os.OpenFile(ConflictFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		conflictLog.Out = file
	} else {
		log.Fatal("Failed to create conflict.log. ", err)
	}
	conflictLog.Out = file

	e.conflictLog = conflictLog

	return nil
}

func (e *conflictEngine) Init(db *sql.DB) error {
	e.db = db
	return nil
}

// func NewConflictEngine(db *sql.DB, override bool, maxRetry int, retrySleepDuration time.Duration, enableDelete bool) EngineExecutor {
//
// 	conflictLog := log.New()
// 	file, err := os.OpenFile(ConflictFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// 	if err == nil {
// 		conflictLog.Out = file
// 	} else {
// 		log.Fatal("Failed to create conflict.log. ", err)
// 	}
// 	e := &conflictEngine{
// 		db:                 db,
// 		override:           override,
// 		enableDelete:       enableDelete,
// 		conflictLog:        conflictLog,
// 		maxRetry:           maxRetry,
// 		retrySleepDuration: retrySleepDuration,
// 	}
// 	return e
// }

func (e *conflictEngine) Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error {

	msg := msgBatch[0]

	if len(msgBatch) > 1 {
		return errors.Errorf("conflict engine should have batch 1")
	}

	if err := ValidateSchema(msg, tableDef); err != nil {
		return errors.Trace(err)
	}

	switch msg.DmlMsg.Operation {
	case core.Insert:
		dataColumnNames := tableDef.ColumnNames()
		insertSql := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) ", tableDef.Schema, tableDef.Name, strings.Join(dataColumnNames, ","))

		values := make([]interface{}, 0, len(msg.DmlMsg.Data))
		for _, name := range dataColumnNames {
			targetData := msg.DmlMsg.Data[name]
			values = append(values, targetData)
		}

		valuesSql := "VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(values)), ",") + ")"
		completeSql := insertSql + valuesSql
		ret, err := e.execWithRetry(e.cfg.MaxConflictRetry, e.db, completeSql, values...)
		switch ret {
		case execSuccess:
			return nil

		case execFailOther:
			return errors.Trace(err)

		case execFailConflict:
			e.logConflict(msg, tableDef)
			if e.cfg.OverrideConflict {
				completeSql = strings.Replace(insertSql, "INSERT", "REPLACE", 1) + valuesSql
				_, err = e.execWithRetry(e.cfg.MaxConflictRetry, e.db, completeSql, values...)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

	case core.Update:
		setSql, newValues := extractSqlParam(msg.DmlMsg.Data, false)

		whereSql, oldValues := extractSqlParam(msg.DmlMsg.Old, true)

		completeSql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s", tableDef.Schema, tableDef.Name, strings.Join(setSql, ", "), strings.Join(whereSql, " and "))
		ret, err := e.execWithRetry(e.cfg.MaxConflictRetry, e.db, completeSql, append(newValues, oldValues...)...)
		switch ret {
		case execSuccess:
			return nil

		case execFailOther:
			return errors.Trace(err)

		case execFailConflict:
			e.logConflict(msg, tableDef)
			if e.cfg.OverrideConflict {
				values := make([]interface{}, 0, len(tableDef.ColumnNames()))
				for _, k := range tableDef.Columns {
					cd := msg.DmlMsg.Data[k.Name]
					values = append(values, cd)
				}

				completeSql := fmt.Sprintf("REPLACE INTO `%s`.`%s`(%s) VALUES (%s)", tableDef.Schema, tableDef.Name,
					strings.Join(tableDef.ColumnNames(), ","),
					strings.TrimSuffix(strings.Repeat("?,", len(values)), ","))
				_, err = e.execWithRetry(e.cfg.MaxConflictRetry, e.db, completeSql, values...)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

	case core.Delete:
		if !e.cfg.EnableDelete {
			log.Debugf("conflictEngine: skip DELETE type. msg: %v", msg)
			return nil
		}
		sql, arg, err := GenerateSingleDeleteSQL(msg, tableDef)
		if err != nil {
			return errors.Trace(err)
		}
		ret, err := e.execWithRetry(e.cfg.MaxConflictRetry, e.db, sql, arg...)
		switch ret {
		case execSuccess:
			return nil

		case execFailOther:
			return errors.Trace(err)

		case execFailConflict:
			e.logConflict(msg, tableDef)
		}

	default:
		log.Warnf("conflictEngine: skip unsupported msg type %s, msg %+v", msg.DmlMsg.Operation, msg)
	}

	return nil
}

func extractSqlParam(data map[string]interface{}, isNull bool) ([]string, []interface{}) {
	placeholder := make([]string, len(data))
	values := make([]interface{}, len(data))

	idx := 0
	for name, columnValue := range data {
		if isNull && data[name] == nil {
			placeholder[idx] = name + " is ?"
		} else {
			placeholder[idx] = name + " = ?"
		}
		values[idx] = columnValue
		idx++
	}

	return placeholder, values
}

func (e *conflictEngine) logConflict(msg *core.Msg, tableDef *schema_store.Table) {
	pkCol := make([]schema_store.Column, 0, len(msg.DmlMsg.Pks))
	for k := range msg.DmlMsg.Pks {
		pkCol = append(pkCol, schema_store.Column{Name: k, IsPrimaryKey: true})
	}
	subSql, pkValues := extractSqlParam(msg.DmlMsg.Pks, false)
	completeSql := fmt.Sprintf("select %s from `%s`.`%s` where %s", strings.Join(tableDef.ColumnNames(), ","), tableDef.Schema, tableDef.Name,
		strings.Join(subSql, " and "))
	log.Info("conflictEngine exec: ", completeSql, "; params ", pkValues)
	row := e.db.QueryRow(completeSql, pkValues...)

	count := len(tableDef.ColumnNames())
	values := make([]string, count)
	scanArgs := make([]interface{}, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	row.Scan(scanArgs...)
	targetValues := bytes.NewBufferString("[")
	dataValues := bytes.NewBufferString("[")
	oldValues := bytes.NewBufferString("[")
	for i, v := range tableDef.ColumnNames() {
		targetValues.WriteString(v)
		targetValues.WriteString("=")
		targetValues.WriteString(*scanArgs[i].(*string))
		targetValues.WriteString(" ")

		dataValues.WriteString(v)
		dataValues.WriteString("=")
		dataValues.WriteString(fmt.Sprint(msg.DmlMsg.Data[v]))
		dataValues.WriteString(" ")

		oldValues.WriteString(v)
		oldValues.WriteString("=")
		oldValues.WriteString(fmt.Sprint(msg.DmlMsg.Old[v]))
		oldValues.WriteString(" ")
	}
	targetValues.Truncate(targetValues.Len() - 1)
	targetValues.WriteString("]")

	dataValues.Truncate(dataValues.Len() - 1)
	dataValues.WriteString("]")

	oldValues.Truncate(oldValues.Len() - 1)
	oldValues.WriteString("]")

	e.conflictLog.Warnf("target values %s, msgType: %s, data: %s, old: %s", targetValues.String(), msg.DmlMsg.Operation, dataValues.String(), oldValues.String())
	log.Warnf("target values %s, msgType: %s, data: %s, old: %s", targetValues.String(), msg.DmlMsg.Operation, dataValues.String(), oldValues.String())
}

func (e conflictEngine) close() {
	e.conflictLog.Out.(*os.File).Close()
}

func (e conflictEngine) execWithRetry(times int, db *sql.DB, stmt string, args ...interface{}) (ret execResult, err error) {
	for i := 0; i < times; i++ {
		ret, err = exec(db, stmt, args...)
		if ret == execSuccess || ret == execFailConflict {
			log.Info("conflictEngine ret: ", ret, ", stmt: ", stmt, ", params: ", args)
			return ret, nil
		}
	}
	log.Errorf("[exec][sql][rerun] %s - %v[error]%v", stmt, args, err)
	return execFailOther, errors.Trace(err)
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
