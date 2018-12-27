package utils

import (
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/juju/errors"
)

const dbName = "drc"
const tableName = "_drc_bidirection"

const tableDDL = `CREATE TABLE IF NOT EXISTS drc._drc_bidirection (
  id INT(11) UNSIGNED NOT NULL,
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  v BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`

func IsBidirectional(db string, tbl string) bool {
	return db == dbName && tbl == tableName
}

func InitBidirection(mode BidirectionMode, db *sql.DB) error {
	switch mode {
	case Stmt:
		_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = db.Exec(tableDDL)
		if err != nil {
			return errors.Trace(err)
		}

	case Annotation:
	}
	return nil
}

type BidirectionMode int

const (
	Stmt BidirectionMode = iota
	Annotation
)

func BuildBidirection(mode BidirectionMode) string {
	switch mode {
	case Stmt:
		id := rand.Int31n(999) + 1
		return fmt.Sprintf("insert into %s.%s(id) values (%d) on duplicate key update v = v + 1;", dbName, tableName, id)
	case Annotation:
		return "/*drc:bidirectional*/"
	default:
		panic("impossible")
	}
}
