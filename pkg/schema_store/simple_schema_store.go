package schema_store

import (
	"database/sql"
	"sync"

	"github.com/juju/errors"
)

type SimpleSchemaStore struct {
	sync.RWMutex
	db      *sql.DB
	schemas map[string]Schema
}

func (store *SimpleSchemaStore) IsInCache(dbName string) bool {
	if _, ok := store.schemas[dbName]; ok {
		return true
	} else {
		return false
	}
}

func (store *SimpleSchemaStore) GetSchema(dbName string) (Schema, error) {
	store.Lock()
	defer store.Unlock()

	if dbName == "" {
		return nil, nil
	}

	// Return the cached Schema
	cachedSchema, ok := store.schemas[dbName]
	if ok {
		return cachedSchema, nil
	}

	schema, err := GetSchemaFromDB(store.db, dbName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store.schemas[dbName] = schema
	return schema, nil
}

func (store *SimpleSchemaStore) InvalidateSchemaCache(schema string) {
	store.Lock()
	defer store.Unlock()

	delete(store.schemas, schema)
}

func (store *SimpleSchemaStore) InvalidateCache() {
	// Invalidate Schema cache
	store.Lock()
	defer store.Unlock()

	// make a new map here
	store.schemas = make(map[string]Schema)
}

func (store *SimpleSchemaStore) Close() {
}

func NewSimpleSchemaStoreFromDBConn(db *sql.DB) (SchemaStore, error) {
	return &SimpleSchemaStore{db: db, schemas: make(map[string]Schema)}, nil
}
