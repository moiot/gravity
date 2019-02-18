package binlog_checker

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/metrics"
	pb "github.com/moiot/gravity/pkg/protocol/tidb"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/moiot/gravity/pkg/utils/retry"
)

var binlogCheckerTableNameV2 = "gravity_heartbeat_v2"

var binlogCheckerFullTableNameV2 = fmt.Sprintf("`%s`.`%s`", consts.GravityDBName, binlogCheckerTableNameV2)

var createV2TableStatement = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s(
		name VARCHAR(255) NOT NULL,
		offset BIGINT,
		update_time_at_gravity DATETIME(6),
		update_time_at_source DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		PRIMARY KEY(name)		
	)
`, binlogCheckerFullTableNameV2)

type Row struct {
	GravityName         string
	Offset              int64
	UpdateTimeAtGravity time.Time
	UpdateTimeAtSource  time.Time
}

// type CheckerConfig struct {
// 	PipelineName   string
// 	// DBConfig       *utils.DBConfig
// 	// ProbeConfig    *gravityConfig.SourceProbeCfg
//
// 	DisableChecker bool
// 	Interval       time.Duration
// }

type BinlogChecker interface {
	Start() error
	Stop()
	MarkActive(row Row)
	// IsEventBelongsToMySelf(event *replication.RowsEvent) bool
	IsEventBelongsToMySelf(row Row) bool
}

// mockBinlogChecker is only used for debug/test,
// so that the heartbeat message don't mess up the binlog event stream
type mockBinlogChecker struct{}

func (checker mockBinlogChecker) Start() error {
	return nil
}

func (checker mockBinlogChecker) Stop() {
}

func (checker mockBinlogChecker) IsBinlogCheckerMsg(db string, table string) bool {
	return false
}

func (checker mockBinlogChecker) IsEventBelongsToMySelf(row Row) bool {
	return false
}

func (checker mockBinlogChecker) MarkActive(row Row) {

}

type binlogChecker struct {
	// cfg                     CheckerConfig
	pipelineName            string
	sourceDB                *sql.DB
	checkInterval           time.Duration
	lastProbeSentOffset     int64
	lastProbeReceivedOffset sync2.AtomicInt64
	stop                    chan struct{}
	wg                      sync.WaitGroup
	annotation              string
}

func (checker *binlogChecker) probe() {
	retryCnt := 3
	err := retry.Do(func() error {
		// add the sentOffset first, so that we can have alert when the lag goes too high
		sentOffset := checker.lastProbeSentOffset + 1
		checker.lastProbeSentOffset = sentOffset

		queryWithPlaceHolder := fmt.Sprintf(
			"%sUPDATE %s SET offset = ?, update_time_at_gravity = ? WHERE name = ?",
			checker.annotation,
			binlogCheckerFullTableNameV2)
		_, err := checker.sourceDB.Exec(queryWithPlaceHolder, sentOffset, time.Now(), checker.pipelineName)
		if err != nil {
			log.Warnf("[binlog_checker] failed probe: %v", err)
		}
		return err
	}, retryCnt, 1*time.Second)
	if err != nil {
		log.Fatalf("[binlog_checker] failed probe for %d times", retryCnt)
	}

}

func (checker *binlogChecker) run() {
	ticker := time.NewTicker(checker.checkInterval)
	checker.wg.Add(1)
	go func() {
		defer func() {
			ticker.Stop()
			checker.wg.Done()
		}()

		for {
			select {
			case <-ticker.C:
				checker.probe()
			case <-checker.stop:
				return
			}
		}
	}()
}

func IsBinlogCheckerMsg(db string, table string) bool {
	return db == consts.GravityDBName && (table == binlogCheckerTableNameV2)
}

func (checker *binlogChecker) IsEventBelongsToMySelf(row Row) bool {
	return row.GravityName == checker.pipelineName
}

// MarkActive will parse the timestamp in RowsEvent and update lastProbeReceivedAt
// CREATE TABLE IF NOT EXISTS binlog_heartbeat (
// id INT AUTO_INCREMENT PRIMARY KEY,
// gravity_id INT,
// Offset BIGINT,
// update_time_at_gravity DATETIME(6),
// update_time_at_source DATETIME(6) NOT NULL ON UPDATE CURRENT_TIMESTAMP
// )
func (checker *binlogChecker) MarkActive(row Row) {
	checker.lastProbeReceivedOffset.Set(row.Offset)
	metrics.ProbeHistogram.WithLabelValues(checker.pipelineName).Observe(time.Since(row.UpdateTimeAtGravity).Seconds())
}

func ParseMySQLRowEvent(event *replication.RowsEvent) (Row, error) {
	checkerRow := Row{}
	rows := event.Rows
	if len(rows) != 2 {
		return checkerRow, errors.Errorf("BinlogChecker: seems not an update: %v", rows)
	}
	data := rows[1]
	if len(data) != 4 {
		log.Warnf("BinlogChecker: data don't have 4 field")
		return checkerRow, errors.Errorf("BinlogChecker: column not matched: %v", rows)
	}

	name, ok := data[0].(string)
	if !ok {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: GravityName, origin: %v", data[0])
	}
	checkerRow.GravityName = name

	offset, ok := data[1].(int64)
	if !ok {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: Offset, value: %v", data[1])
	}
	checkerRow.Offset = offset

	updatedAtGravity, ok := data[2].(time.Time)
	if !ok {
		log.Errorf("BinlogChecker type conversion error: updatedAtGravity: %v", data[2])
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: updatedAtGravity, value: %v", data[2])
	}
	checkerRow.UpdateTimeAtGravity = updatedAtGravity

	updatedAtSource, ok := data[2].(time.Time)
	if !ok {
		log.Errorf("BinlogChecker type conversion error: updatedAtGravity: %v", data[2])
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: updatedAtSource, value: %v", data[3])
	}
	checkerRow.UpdateTimeAtSource = updatedAtSource
	return checkerRow, nil
}

const DRC_TIME_LAYOUT_VERSION_01 = "2006-01-02 15:04:05.999999999"

func ParseTiDBRow(row pb.Row) (Row, error) {
	checkerRow := Row{}
	data := row.Columns
	if len(data) != 4 {
		return checkerRow, errors.Errorf("BinlogChecker: column not matched. colunns: %v", data)
	}

	name := data[0].StringValue
	if name == nil {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: GravityName, origin: %v", data[0])
	}
	checkerRow.GravityName = string(*name)

	offset := data[1].Int64Value
	if offset == nil {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: Offset, origin: %v", data[1])
	}
	checkerRow.Offset = *offset

	updatedAtGravityStr := data[2].StringValue
	if updatedAtGravityStr == nil {
		return checkerRow, errors.Errorf("BinlogChecker updatedAtGravity is nil, data: %v", data)
	}
	updatedAtGravity, err := time.Parse(DRC_TIME_LAYOUT_VERSION_01, *updatedAtGravityStr)
	if err != nil {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: UpdatedAtGravity, origin: %v", data[2])

	}
	checkerRow.UpdateTimeAtGravity = updatedAtGravity

	updatedAtSourceStr := data[3].StringValue
	if updatedAtSourceStr == nil {
		return checkerRow, errors.Errorf("BinlogChecker updatedAtSource is nil, data: %v", data)
	}
	updatedAtSource, err := time.Parse(DRC_TIME_LAYOUT_VERSION_01, *updatedAtSourceStr)
	if err != nil {
		return checkerRow, errors.Errorf("BinlogChecker type conversion error: UpdatupdatedAtSourceedAtGravity, origin: %v", data[3])

	}
	checkerRow.UpdateTimeAtSource = updatedAtSource
	return checkerRow, nil
}

func (checker *binlogChecker) Start() error {
	log.Infof("[binlog_checker] start")
	if err := checker.initRepo(); err != nil {
		return errors.Trace(err)
	}

	checker.lastProbeSentOffset = 0
	checker.lastProbeReceivedOffset.Set(0)

	checker.run()

	return nil
}

func (checker *binlogChecker) Stop() {
	close(checker.stop)
	checker.wg.Wait()

	checker.sourceDB.Close()
}

func (checker *binlogChecker) initRepo() error {
	if _, err := checker.sourceDB.Exec(fmt.Sprintf("%sCREATE DATABASE IF NOT EXISTS %s", checker.annotation, consts.GravityDBName)); err != nil {
		return errors.Trace(err)
	}

	if _, err := checker.sourceDB.Exec(fmt.Sprintf("%s%s", checker.annotation, createV2TableStatement)); err != nil {
		return errors.Trace(err)
	}

	var nrGravity int
	row := checker.sourceDB.QueryRow(fmt.Sprintf("%sSELECT COUNT(1) FROM %s", checker.annotation, binlogCheckerFullTableNameV2))
	if err := row.Scan(&nrGravity); err != nil {
		log.Fatalf("[binlog_checker] failed to get heartbeat count, err: %v", err.Error())
	}

	if nrGravity > config.MaxNrGravity {
		log.Fatalf("[binlog_checker] too many gravity here: %v", nrGravity)
	}

	// init the record
	statement := fmt.Sprintf("%sINSERT INTO %s (name, offset, update_time_at_gravity) VALUES (?, 0, ?) ON DUPLICATE KEY UPDATE name=name", checker.annotation, binlogCheckerFullTableNameV2)
	if _, err := checker.sourceDB.Exec(statement, checker.pipelineName, time.Now()); err != nil {
		return errors.Annotate(err, fmt.Sprintf("failed to init heartbeat"))
	}
	return nil
}

func NewBinlogChecker(
	pipelineName string,
	dbConfig *utils.DBConfig,
	annotation string,
	interval time.Duration,
	disableChecker bool) (BinlogChecker, error) {

	if disableChecker {
		return mockBinlogChecker{}, nil
	}

	db, err := utils.CreateDBConnection(dbConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &binlogChecker{
		pipelineName:  pipelineName,
		sourceDB:      db,
		stop:          make(chan struct{}),
		checkInterval: interval,
		annotation:    annotation,
	}, nil
}
