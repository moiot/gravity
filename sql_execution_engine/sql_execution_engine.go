package sql_execution_engine

import (
	"database/sql"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/schema_store"
)

type EngineExecutor interface {
	Execute(msgBatch []*core.Msg, tableDef *schema_store.Table) error
}

type EngineInitializer interface {
	Init(db *sql.DB) error
}

// func SelectEngine(DetectConflict bool, UserBidirection bool, UseShadingProxy bool) (string, error) {
// 	var engine string
// 	if !DetectConflict && !UserBidirection && !UseShadingProxy {
// 		engine = MySQLReplaceEngine
// 	} else if UserBidirection && !UseShadingProxy {
// 		engine = BiDirectionEngine
// 	} else if UserBidirection && UseShadingProxy {
// 		engine = BiDirectionShadingEngine
// 	} else if DetectConflict && !UserBidirection && !UseShadingProxy {
// 		engine = ConflictEngine
// 	} else {
// 		return "", errors.BadRequestf("No match sql execution engine found for detect conflict[%t], bidirection[%t], shading proxy[%t]", DetectConflict, UserBidirection, UseShadingProxy)
// 	}
// 	log.Infof("SelectEngine: %v", engine)
// 	return engine, nil
// }
//
// func NewSQLExecutionEngine(db *sql.DB, engineConfig MySQLExecutionEngineConfig) EngineExecutor {
// 	var engine EngineExecutor
// 	name := engineConfig.EngineType
// 	switch name {
// 	case MySQLReplaceEngine:
// 		engine = NewMySQLReplaceEngine(db)
// 	case BiDirectionEngine:
// 		engine = NewBidirectionEngine(db, utils.Stmt)
// 	case BiDirectionShadingEngine:
// 		engine = NewBidirectionEngine(db, utils.Annotation)
// 	case ConflictEngine:
// 		engine = NewConflictEngine(db, engineConfig.OverrideConflict, engineConfig.MaxConflictRetry, 1*time.Second, false)
// 	case ManualEngine:
// 		engine = NewManualSQLEngine(db, engineConfig)
// 	default:
// 		log.Fatal("unknown sql execution engine ", name)
// 		return nil
// 	}
//
// 	return engine
// }
