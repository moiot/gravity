package utils

import (
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/siddontang/go-mysql/replication"

	"github.com/moiot/gravity/pkg/consts"

	"github.com/juju/errors"
)

//
// /*drc:bidirectional*/

const (
	dbNameV1    = "drc"
	tableNameV1 = "_drc_bidirection"

	dbNameV2    = consts.GravityDBName
	tableNameV2 = "_gravity_txn_tags"
)

var tableDDLV2 = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
  id INT(11) UNSIGNED NOT NULL,
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  pipeline_name VARCHAR(255) NOT NULL,
  v BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`, dbNameV2, tableNameV2)

// Only for test purpose
var TxnTagSQLFormat = fmt.Sprintf("insert into `%s`.`%s`", dbNameV2, tableNameV2)

func IsInternalTraffic(db string, tbl string) bool {
	return (db == dbNameV1 && tbl == tableNameV1) || (db == dbNameV2 && tbl == tableNameV2)
}

func IsInternalTrafficV2(db string, tbl string) bool {
	return db == dbNameV2 && tbl == tableNameV2
}

func MatchTxnTagPipelineName(db string, tbl string, patterns []string, ev *replication.RowsEvent) (string, bool) {
	if IsInternalTrafficV2(db, tbl) {
		for rowIndex := 0; rowIndex < len(ev.Rows); rowIndex++ {
			pipelineName := ev.Rows[rowIndex][2].(string)
			for _, pattern := range patterns {
				if Glob(pattern, pipelineName) {
					return pipelineName, true
				}
			}
		}
		return "", false
	} else {
		return "", false
	}
}

func InitInternalTxnTags(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbNameV2))
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(tableDDLV2)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func GenerateTxnTagSQL(pipelineName string) string {
	id := rand.Int31n(999) + 1
	return fmt.Sprintf("insert into `%s`.`%s` (id,pipeline_name) values (%d,'%s') on duplicate key update v = v + 1, pipeline_name = '%s'", dbNameV2, tableNameV2, id, pipelineName, pipelineName)
}
