package sql_execution_engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils/retry"
)

type conflictPreviewEngine struct {
	db            *sql.DB
	enableDelete  bool
	maxRetry      int
	sleepDuration time.Duration
}

var ErrRowConflict = errors.New("sql_execution_engine: this row conflicts")
var ErrDeleteRowSkip = errors.New("sql_execution_engine: skip this delete event")

type queryResult int

const (
	QueryExist queryResult = iota
	QueryNotExist
	QueryUnknown
)

func NewConflictPreviewEngine(db *sql.DB, maxRetryCount int, sleepDuration time.Duration, enableDelete bool) EngineExecutor {
	e := &conflictPreviewEngine{
		db:            db,
		maxRetry:      maxRetryCount,
		sleepDuration: sleepDuration,
		enableDelete:  enableDelete,
	}
	return e
}

func buildGeneralConditions(data map[string]interface{}) (string, []interface{}) {
	placeholders, values := extractSqlParam(data, true)
	return strings.Join(placeholders, " and "), values
}

func buildUniqueKeyConditions(uniqueKeyColumnMap map[string][]string, data map[string]interface{}) (string, []interface{}) {
	var placeHolders []string
	var values []interface{}
	for _, columnNames := range uniqueKeyColumnMap {
		var uniqKeyConStrs []string
		for _, columnName := range columnNames {
			var placeHolder string
			if data[columnName] == nil {
				placeHolder = columnName + " is ?"
			} else {
				placeHolder = columnName + " = ?"
			}
			uniqKeyConStrs = append(uniqKeyConStrs, placeHolder)
			values = append(values, data[columnName])
		}
		placeHolders = append(placeHolders, strings.Join(uniqKeyConStrs, " and "))
	}
	return strings.Join(placeHolders, " or "), values
}

func (e *conflictPreviewEngine) Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error {
	if len(msgBatch) > 1 {
		return errors.BadRequestf("preview engine should have batch 1")
	}

	msg := msgBatch[0]

	if err := ValidateSchema(msg, tableDef); err != nil {
		return errors.Trace(err)
	}

	queryTemplate := "SELECT count(*) from `%s`.`%s` where %s "
	switch msg.DmlMsg.Operation {
	case core.Insert:
		condition, values := buildUniqueKeyConditions(tableDef.UniqueKeyColumnMap, msg.DmlMsg.Data)
		queryStatement := fmt.Sprintf(queryTemplate, tableDef.Schema, tableDef.Name, condition)
		result, err := e.query(e.db, queryStatement, values...)
		if result == QueryUnknown {
			return errors.Trace(err)
		} else if result == QueryNotExist {
			return nil
		} else {
			return ErrRowConflict
		}
	case core.Update:
		condition, values := buildGeneralConditions(msg.DmlMsg.Old)
		queryStatement := fmt.Sprintf(queryTemplate, tableDef.Schema, tableDef.Name, condition)
		result, err := e.query(e.db, queryStatement, values...)
		if result == QueryUnknown {
			return errors.Trace(err)
		} else if result == QueryNotExist {
			return ErrRowConflict
		} else {
			condition, values := buildGeneralConditions(msg.DmlMsg.Data)
			queryStatement = fmt.Sprintf(queryTemplate, tableDef.Schema, tableDef.Name, condition)
			result, err = e.query(e.db, queryStatement, values...)
			if result == QueryUnknown {
				return errors.Trace(err)
			} else if result == QueryNotExist {
				return nil
			} else {
				return ErrRowConflict
			}
		}
	case core.Delete:
		if !e.enableDelete {
			log.Debugf("conflictPreviewEngine: skip DELETE type. msg: %v", msg)
			return ErrDeleteRowSkip
		}
		condition, values := buildGeneralConditions(msg.DmlMsg.Data)
		queryStatement := fmt.Sprintf(queryTemplate, tableDef.Schema, tableDef.Name, condition)
		result, err := e.query(e.db, queryStatement, values...)
		if result == QueryUnknown {
			return errors.Trace(err)
		} else if result == QueryNotExist {
			return ErrRowConflict
		} else {
			return nil
		}
	default:
		log.Warnf("conflictPreviewEngine: skip unsupported msg type %s, msg %+v", msg.DmlMsg.Operation, msg)
	}
	return nil
}

func (e conflictPreviewEngine) close() {
}

func (e *conflictPreviewEngine) query(db *sql.DB, stmt string, args ...interface{}) (queryResult, error) {
	var count int
	err := retry.Do(func() error {
		return db.QueryRow(stmt, args...).Scan(&count)
	}, e.maxRetry, e.sleepDuration)

	if err != nil {
		return QueryUnknown, errors.Trace(err)
	} else {
		if count > 0 {
			return QueryExist, nil
		} else {
			return QueryNotExist, nil
		}
	}
}
