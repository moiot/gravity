package sql_execution_engine

import (
	"database/sql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/schema_store"
)

type bidirectionEngine struct {
	db   *sql.DB
	mode utils.BidirectionMode
}

func NewBidirectionEngine(db *sql.DB, mode utils.BidirectionMode) *bidirectionEngine {
	if err := utils.InitBidirection(mode, db); err != nil {
		log.Fatal(err)
	}

	return &bidirectionEngine{db: db, mode: mode}
}

func (e *bidirectionEngine) Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error {

	var stmts []string
	var args [][]interface{}

	currentBatchSize := len(msgBatch)
	if currentBatchSize == 0 {
		return nil
	}

	if e.mode == utils.Annotation && len(msgBatch) > 1 {
		return errors.Errorf("annotation type bidirection engine do not support batch size > 1")
	}

	if currentBatchSize > 1 && msgBatch[0].DmlMsg.Operation == core.Delete {
		return errors.Errorf("[bidirectionEngine] only support single delete")
	}

	if msgBatch[0].DmlMsg.Operation == core.Delete {
		stmt, arg, err := GenerateSingleDeleteSQL(msgBatch[0], tableDef)
		if err != nil {
			return errors.Trace(err)
		}
		if e.mode == utils.Annotation {
			stmt = utils.BuildBidirection(e.mode) + stmt
		}

		stmts = append(stmts, stmt)
		args = append(args, arg)
	} else {
		for _, msg := range msgBatch {
			if err := ValidateSchema(msg, tableDef); err != nil {
				return errors.Trace(err)
			}

			sqlStatement, arg, err := GenerateSingleReplaceSQL(msg, tableDef)
			if err != nil {
				return errors.Trace(err)
			}
			if e.mode == utils.Annotation {
				sqlStatement = utils.BuildBidirection(e.mode) + sqlStatement
			}
			stmts = append(stmts, sqlStatement)
			args = append(args, arg)
		}
	}

	txn, err := e.db.Begin()
	if err != nil {
		log.Warnf("[bidirectionEngine] err: %v", err.Error())
		return errors.Trace(err)
	}

	if e.mode == utils.Stmt {
		additionalStmt := utils.BuildBidirection(e.mode)
		log.Debugf("[bidirectionEngine] sql: %s", additionalStmt)
		_, err := txn.Exec(additionalStmt)
		if err != nil {
			log.Warnf("[bidirectionEngine] err: %v", err.Error())
			txn.Rollback()
			return errors.Annotatef(err, "sql: %v", additionalStmt)
		}
	}

	for idx, stmt := range stmts {
		log.Debugf("[bidirectionEngine] sql: %v, arg: %v", stmt, args[idx])
		_, err := txn.Exec(stmt, args[idx]...)
		if err != nil {
			txn.Rollback()
			log.Warnf("[bidirectionEngine] err: %v", err)
			return errors.Annotatef(err, "sql: %v, arg: %v", stmt, args[idx])
		}
	}
	if err := txn.Commit(); err != nil {
		return errors.Annotatef(err, "[bidirectionEngine] commit error %s", err.Error())
	}

	return nil
}
