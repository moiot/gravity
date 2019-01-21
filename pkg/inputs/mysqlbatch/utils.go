package mysqlbatch

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/schema_store"
)

// GetTables returns a list of table definition based on the schema, table name patterns
// We only support single sourceDB for now.
func GetTables(db *sql.DB, schemaStore schema_store.SchemaStore, tableConfigs []TableConfig) ([]*schema_store.Table, []TableConfig) {
	var tableDefs []*schema_store.Table
	var retTableConfigs []TableConfig

	for i := range tableConfigs {
		schemaName := tableConfigs[i].Schema
		startTime := time.Now()
		schema, err := schemaStore.GetSchema(schemaName)
		if err != nil {
			log.Fatalf("failed to get schema, err: %v", err)
		}
		log.Infof("[scanner_server] GetSchema duration: %v", time.Since(startTime).Seconds())

		queryStartTime := time.Now()
		statement := fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'")
		rows, err := db.Query(statement, schemaName)
		if err != nil {
			log.Fatalf("failed to get table names, err %v", err)
		}

		for rows.Next() {
			var tableName string
			err := rows.Scan(&tableName)
			if err != nil {
				log.Fatalf("failed to scan, err: %v", err)
			}

			for _, tableExp := range tableConfigs[i].tableExps {
				if tableExp.Match([]byte(tableName)) {
					tableDef, ok := schema[tableName]
					if !ok {
						log.Fatalf("table def not found, schema: %v, table: %v", schemaName, tableName)
					}
					tableDefs = append(tableDefs, tableDef)
					retTableConfigs = append(retTableConfigs, tableConfigs[i])
				}
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[scanner_server] query error: %s", err.Error())
		}
		rows.Close()
		log.Infof("[scanner_server] select tables duration: %v", time.Since(queryStartTime).Seconds())
	}

	return tableDefs, retTableConfigs
}
