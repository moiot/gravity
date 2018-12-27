package job_processor

//
// import (
// 	"github.com/juju/errors"
//
// 	"github.com/moiot/gravity/pkg/core"
//
// 	"sync"
// 	"time"
//
// 	"github.com/moiot/gravity/pkg/utils/retry"
// 	"github.com/moiot/gravity/pkg/worker_pool"
// 	"github.com/moiot/gravity/schema_store"
// 	"github.com/moiot/gravity/sql_execution_engine"
// )
//
// type MySQLWorker struct {
// 	targetSchemaStore  schema_store.SchemaStore
// 	sqlExecutionEngine sql_execution_engine.SQlExecutionEngine
// 	wg                 sync.WaitGroup
// }
//
// func NewMySQLWorker(
// 	jobBatchC chan []worker_pool.Job,
// 	schemaStore schema_store.SchemaStore,
// 	sqlEngine sql_execution_engine.SQlExecutionEngine,
// 	maxRetryCount int,
// 	sleepDuration time.Duration,
// ) *MySQLWorker {
// 	w := MySQLWorker{
// 		sqlExecutionEngine: sqlEngine,
// 		targetSchemaStore:  schemaStore,
// 	}
//
// 	w.wg.Add(1)
// 	go func() {
// 		defer w.wg.Done()
// 		for jobBatch := range jobBatchC {
// 			retry.Do(func() error {
// 				return w.Execute(jobBatch)
// 			}, maxRetryCount, sleepDuration)
// 		}
// 	}()
//
// 	return &w
// }
//
// func (w *MySQLWorker) Wait() {
// 	w.wg.Wait()
// }
//
// func (w *MySQLWorker) Execute(jobBatch []worker_pool.Job) error {
// 	var pbMsgBatch []*core.Msg
// 	var tableDef *schema_store.Table
// 	for i := range jobBatch {
// 		job := jobBatch[i]
// 		mysqlJob, ok := job.(Job)
// 		if !ok {
// 			return errors.Errorf("[padder_mysql_worker] failed type conversion")
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
// 		msg := job.Msg()
// 		pbMsgBatch = append(pbMsgBatch, &msg)
// 	}
//
// 	e := w.sqlExecutionEngine.Execute(pbMsgBatch, tableDef)
// 	if e != nil {
// 		return errors.Trace(e)
// 	}
//
// 	return nil
// }
