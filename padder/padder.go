package padder

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"

	"time"

	"github.com/siddontang/go-mysql/mysql"

	"database/sql"

	"github.com/moiot/gravity/gravity/schedulers/batch_table_scheduler"
	"github.com/moiot/gravity/metrics"
	"github.com/moiot/gravity/padder/config"
	"github.com/moiot/gravity/padder/job_processor"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/worker_pool"
	"github.com/moiot/gravity/schema_store"
	"github.com/moiot/gravity/sql_execution_engine"
)

const (
	WorkerQueueSize = 1024
	MaxRetryCount   = 2
	SleepDuration   = 200 * time.Millisecond
)

type PreviewStatistic struct {
	WriteEventCount uint64
	ConflictRatio   float64
	UnknownRatio    float64
}

func Pad(padderConfig config.PadderConfig) error {
	var scheduler core.Scheduler
	var worker *job_processor.MySQLWorker

	mysqlConfig := padderConfig.MySQLConfig
	store, db, err := getDB(mysqlConfig)
	if err != nil {
		return errors.Trace(err)
	}

	sqlEngine := sql_execution_engine.NewConflictEngine(db, false, MaxRetryCount, SleepDuration, padderConfig.EnableDelete)

	targetTablePattern := mysqlConfig.Target.Schema + ".*"

	scheduler, workerQueues, err := batch_table_scheduler.NewBatchScheduler(
		"padder",
		metrics.PadderTag,
		worker_pool.WorkerPoolConfig{
			SlidingWindowSize: 0,
			NrWorker:          1,
			QueueSize:         WorkerQueueSize,
			MaxBatchPerWorker: 1,
		},
	)

	if err != nil {
		return errors.Trace(err)
	}

	worker = job_processor.NewMySQLWorker(
		workerQueues[0],
		store,
		sqlEngine,
		MaxRetryCount,
		SleepDuration,
	)

	if err := execute(scheduler, mysqlConfig, targetTablePattern, store, padderConfig.BinLogList); err != nil {
		return errors.Trace(err)
	}

	scheduler.Close()
	worker.Wait()

	log.Info("pad bin logs finished")

	return nil
}

func execute(
	scheduler core.Scheduler,
	mysqlConfig *config.MySQLConfig,
	srcId string,
	store schema_store.SchemaStore,
	binlogList []string,
) error {

	startPos := mysqlConfig.StartPosition
	processor := job_processor.NewJobProcessor(scheduler,
		mysql.Position{
			Name: startPos.BinLogFileName,
			Pos:  startPos.BinLogFilePos,
		},
		mysqlConfig.Target.Schema, srcId, store)
	onEvent := processor.Process
	binLogList := binlogList
	p := replication.NewBinlogParser()
	p.SetParseTime(true)
	processBinLog := p.ParseFile
	err := processBinLog(binLogList[0], int64(startPos.BinLogFilePos), onEvent)
	if err != nil {
		return errors.Trace(err)
	}
	if len(binLogList) > 1 {
		for _, binLogFile := range binLogList[1:] {
			err := processBinLog(binLogFile, 0, onEvent)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func getDB(mysqlConfig *config.MySQLConfig) (*schema_store.SimpleSchemaStore, *sql.DB, error) {
	store, err := schema_store.NewSimpleSchemaStore(mysqlConfig.Target)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	targetDB, err := utils.CreateDBConnection(mysqlConfig.Target)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return store, targetDB, nil
}

func Preview(padderConfig config.PadderConfig) (PreviewStatistic, error) {

	mysqlConfig := padderConfig.MySQLConfig
	store, db, err := getDB(mysqlConfig)
	if err != nil {
		return PreviewStatistic{}, errors.Trace(err)
	}

	targetTablePattern := mysqlConfig.Target.Schema + ".*"

	sqlEngine := sql_execution_engine.NewConflictPreviewEngine(db, MaxRetryCount, SleepDuration, padderConfig.EnableDelete)

	scheduler, workerQueues, err := batch_table_scheduler.NewBatchScheduler(
		"padder",
		metrics.PadderTag,
		worker_pool.WorkerPoolConfig{
			SlidingWindowSize: 0,
			NrWorker:          1,
			QueueSize:         WorkerQueueSize,
			MaxBatchPerWorker: 1,
		},
	)

	if err != nil {
		return PreviewStatistic{}, errors.Trace(err)
	}

	worker := job_processor.NewMySQLPreviewWorker(workerQueues[0], store, sqlEngine)

	if err := execute(scheduler, mysqlConfig, targetTablePattern, store, padderConfig.BinLogList); err != nil {
		return PreviewStatistic{}, errors.Trace(err)

	}

	scheduler.Close()
	worker.Wait()

	log.Info("pad-preview bin logs finished")

	stats := PreviewStatistic{}
	conflictCount, unknownCount, writeEventsCount := worker.GetStatistic()
	if writeEventsCount > 0 {
		stats.WriteEventCount = uint64(writeEventsCount)
		stats.ConflictRatio = float64(conflictCount) / float64(writeEventsCount)
		stats.UnknownRatio = float64(unknownCount) / float64(writeEventsCount)
	}
	return stats, nil
}
