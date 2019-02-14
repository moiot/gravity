package mysqlbatch

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/utils"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/schema_store"
)

// GetTables returns a list of table definition based on the schema, table name patterns
// We only support single sourceDB for now.
func GetTables(db *sql.DB, schemaStore schema_store.SchemaStore, tableConfigs []TableConfig, router core.Router) ([]*schema_store.Table, []TableConfig) {
	var tableDefs []*schema_store.Table
	var retTableConfigs []TableConfig

	rows, err := db.Query("SELECT distinct TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA not in ('mysql', 'information_schema', 'performance_schema', 'sys', '_gravity') AND TABLE_TYPE = 'BASE TABLE';")
	if err != nil {
		log.Fatalf("failed to get schema and table names, err %v", errors.Trace(err))
	}

	allSchema := make(map[string][]string)
	for rows.Next() {
		var schemaName, tableName string
		err := rows.Scan(&schemaName, &tableName)
		if err != nil {
			log.Fatalf("failed to scan, err: %v", errors.Trace(err))
		}
		allSchema[schemaName] = append(allSchema[schemaName], tableName)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("[scanner_server] query error: %s", errors.Trace(err))
	}
	if err := rows.Close(); err != nil {
		log.Fatalf("[scanner_server] query error: %s", errors.Trace(err))
	}

	added := make(map[string]bool)
	for i := range tableConfigs {
		schemaName := tableConfigs[i].Schema
		allTables, ok := allSchema[schemaName]
		if !ok {
			continue
		}

		schema, err := schemaStore.GetSchema(schemaName)
		if err != nil {
			log.Fatalf("failed to get schema, err: %v", err)
		}

		for _, tableName := range allTables {
			for _, tablePattern := range tableConfigs[i].Table {
				if utils.Glob(tablePattern, tableName) {
					tableDef, ok := schema[tableName]
					if !ok {
						log.Fatalf("table def not found, schema: %v, table: %v", schemaName, tableName)
					}
					log.Infof("added for batch: %s.%s", schemaName, tableName)
					tableDefs = append(tableDefs, tableDef)
					retTableConfigs = append(retTableConfigs, tableConfigs[i])
					added[fmt.Sprintf("%s.%s", schemaName, tableName)] = true
				}
			}
		}
	}

	if router != nil {
		for schemaName, tables := range allSchema {
			for _, tableName := range tables {
				if added[fmt.Sprintf("%s.%s", schemaName, tableName)] {
					continue
				}
				msg := core.Msg{
					Database: schemaName,
					Table:    tableName,
				}
				if router.Exists(&msg) {
					schema, err := schemaStore.GetSchema(schemaName)
					if err != nil {
						log.Fatalf("failed to get schema, err: %v", errors.Trace(err))
					}
					tableDef, ok := schema[tableName]
					if !ok {
						log.Fatalf("table def not found, schema: %v, table: %v", schemaName, tableName)
					}
					log.Infof("added for batch: %s.%s", schemaName, tableName)
					tableDefs = append(tableDefs, tableDef)
					retTableConfigs = append(retTableConfigs, TableConfig{
						Schema: schemaName,
						Table:  []string{tableName},
					})
				}
			}
		}
	}

	return tableDefs, retTableConfigs
}

func DeleteEmptyTables(db *sql.DB, tables []*schema_store.Table, tableConfigs []TableConfig) ([]*schema_store.Table, []TableConfig) {
	var retTables []*schema_store.Table
	var retTableConfigs []TableConfig

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, t := range tables {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if !utils.IsTableEmpty(db, tables[idx].Schema, tables[idx].Name) {
				mu.Lock()
				defer mu.Unlock()
				retTables = append(retTables, t)
				retTableConfigs = append(retTableConfigs, tableConfigs[idx])
			}
		}(i)

	}
	wg.Wait()
	return retTables, retTableConfigs
}
