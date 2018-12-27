package job_processor

//
// import (
// 	"sync"
//
// 	"github.com/juju/errors"
// 	log "github.com/sirupsen/logrus"
//
// 	"github.com/moiot/gravity/pkg/core"
//
// 	"github.com/moiot/gravity/pkg/worker_pool"
// 	"github.com/moiot/gravity/schema_store"
// 	"github.com/moiot/gravity/sql_execution_engine"
// )
//
// type MySQLPreviewWorker struct {
// 	targetSchemaStore  schema_store.SchemaStore
// 	sqlExecutionEngine sql_execution_engine.SQlExecutionEngine
// 	conflictCount      int
// 	unknownCount       int
// 	writeEventsCount   int
// 	wg                 sync.WaitGroup
// }
//
// func NewMySQLPreviewWorker(
// 	jobBatchC chan []worker_pool.Job,
// 	schemaStore schema_store.SchemaStore,
// 	sqlEngine sql_execution_engine.SQlExecutionEngine,
// ) *MySQLPreviewWorker {
// 	w := MySQLPreviewWorker{
// 		sqlExecutionEngine: sqlEngine,
// 		targetSchemaStore:  schemaStore,
// 	}
//
// 	w.wg.Add(1)
// 	go func() {
// 		defer w.wg.Done()
// 		for jobBatch := range jobBatchC {
// 			err := w.Execute(jobBatch)
//
// 			cause := errors.Cause(err)
//
// 			if errors.IsBadRequest(err) {
// 				log.Fatalf("")
// 			} else if cause == sql_execution_engine.ErrRowConflict {
// 				w.conflictCount += 1
// 			} else if err != nil {
// 				w.unknownCount += 1
// 			}
// 			w.writeEventsCount += 1
// 			log.Errorf("total: %v, conflict: %v, unknown: %v", w.writeEventsCount, w.conflictCount, w.unknownCount)
// 		}
// 	}()
//
// 	return &w
// }
//
// func (w *MySQLPreviewWorker) Wait() {
// 	w.wg.Wait()
// }
//
// func (w *MySQLPreviewWorker) Execute(jobBatch []worker_pool.Job) error {
// 	var pbMsgBatch []*core.Msg
// 	var tableDef *schema_store.Table
// 	for i := range jobBatch {
// 		job := jobBatch[i]
// 		mysqlJob, ok := job.(Job)
// 		if !ok {
// 			return errors.Errorf("[padder_mysql_preview_worker] failed type conversion")
// 		}
//
// 		if tableDef == nil {
// 			if schema, err := w.targetSchemaStore.GetSchema(mysqlJob.JobMsg.Database); err != nil {
// 				return errors.Trace(err)
// 			} else {
// 				tableDef = schema[mysqlJob.JobMsg.Table]
// 			}
//
// 		}
// 		pbMsg := job.Msg()
// 		pbMsgBatch = append(pbMsgBatch, &pbMsg)
// 	}
//
// 	e := w.sqlExecutionEngine.Execute(pbMsgBatch, tableDef)
// 	if e != nil {
// 		return errors.Trace(e)
// 	}
//
// 	return nil
// }
//
// func (w *MySQLPreviewWorker) GetStatistic() (conflictCount int, unknownCount int, writeEventCount int) {
// 	conflictCount = w.conflictCount
// 	unknownCount = w.unknownCount
// 	writeEventCount = w.writeEventsCount
// 	return
// }
