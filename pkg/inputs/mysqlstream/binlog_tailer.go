package mysqlstream

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/moiot/gravity/pkg/inputs/helper"

	"github.com/juju/errors"
	"github.com/pingcap/parser"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/utils"
)

var (
	GravityTableInsertCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "table_insert_rows_counter",
		Help:      "table insert rows counter",
	}, []string{metrics.PipelineTag, "db"})

	GravityTableUpdateCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "table_update_rows_counter",
		Help:      "table update rows counter",
	}, []string{metrics.PipelineTag, "db"})

	GravityTableDeleteCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "table_delete_rows_counter",
		Help:      "table delete rows counter",
	}, []string{metrics.PipelineTag, "db"})

	GravityTableRowsEventSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "drc_v2",
		Subsystem: "gravity",
		Name:      "rows_event_size",
		Help:      "rows event size per txn",
	}, []string{metrics.PipelineTag})
)

func init() {
	prometheus.MustRegister(GravityTableInsertCounter)
	prometheus.MustRegister(GravityTableUpdateCounter)
	prometheus.MustRegister(GravityTableDeleteCounter)
	prometheus.MustRegister(GravityTableRowsEventSize)
}

type BinlogEventSchemaFilterFunc func(schemaName string) bool

type BinlogTailer struct {
	ctx             context.Context
	cancel          context.CancelFunc
	pipelineName    string
	cfg             *MySQLBinlogInputPluginConfig
	gravityServerID uint32
	sourceDB        *sql.DB
	binlogSyncer    *replication.BinlogSyncer
	parser          *parser.Parser

	msgTxnBuffer []*core.Msg

	sourceTimeZone string

	positionCache     position_store.PositionCacheInterface
	sourceSchemaStore schema_store.SchemaStore
	binlogChecker     binlog_checker.BinlogChecker

	emitter core.Emitter

	wg sync.WaitGroup

	done                    chan struct{}
	binlogEventSchemaFilter BinlogEventSchemaFilterFunc
	closed                  bool
}

// NewBinlogTailer creats a new binlog tailer
func NewBinlogTailer(
	pipelineName string,
	cfg *MySQLBinlogInputPluginConfig,
	gravityServerID uint32,
	positionCache position_store.PositionCacheInterface,
	sourceSchemaStore schema_store.SchemaStore,
	sourceDB *sql.DB,
	emitter core.Emitter,
	binlogChecker binlog_checker.BinlogChecker,
	binlogEventSchemaFilter BinlogEventSchemaFilterFunc,
) (*BinlogTailer, error) {

	if pipelineName == "" {
		return nil, errors.Errorf("[binlogTailer] pipeline name is empty")
	}

	c, cancel := context.WithCancel(context.Background())

	tailer := &BinlogTailer{
		cfg:                     cfg,
		pipelineName:            pipelineName,
		gravityServerID:         gravityServerID,
		sourceDB:                sourceDB,
		parser:                  parser.New(),
		ctx:                     c,
		cancel:                  cancel,
		binlogSyncer:            utils.NewBinlogSyncer(gravityServerID, cfg.Source),
		emitter:                 emitter,
		positionCache:           positionCache,
		sourceSchemaStore:       sourceSchemaStore,
		binlogChecker:           binlogChecker,
		done:                    make(chan struct{}),
		sourceTimeZone:          cfg.Source.Location,
		binlogEventSchemaFilter: binlogEventSchemaFilter,
	}
	return tailer, nil
}

func (tailer *BinlogTailer) Start() error {

	log.Infof("[binlogTailer] start")

	if err := utils.CheckBinlogFormat(tailer.sourceDB); err != nil {
		return errors.Trace(err)
	}

	// streamer needs positionCache to load the GTID set
	position := tailer.positionCache.Get()
	binlogPositions, err := helper.DeserializeBinlogPositions(position.Value)
	if err != nil {
		return errors.Trace(err)
	}

	streamer, err := tailer.getBinlogStreamer(binlogPositions.CurrentPosition.BinlogGTID)

	if err != nil {
		return errors.Trace(err)
	}

	var (
		tryReSync = true
	)

	currentPosition, err := GetCurrentPosition(tailer.positionCache)
	if err != nil {
		return errors.Trace(err)
	}

	tailer.wg.Add(1)
	go func() {
		defer func() {
			tailer.wg.Done()
			close(tailer.done)
		}()

		// binlogSyncerTimeout := BinlogProbeInterval * 5

		for {

			// ctx, cancel := context.WithTimeout(tailer.ctx, binlogSyncerTimeout)
			e, err := streamer.GetEvent(tailer.ctx)
			// cancel()

			if err == context.Canceled {
				log.Infof("[binlogTailer] quit for context canceled, currentBinlogGTIDSet: %v, currentBinlogFileName: %v, currentBinlogFilePosition: %v",
					currentPosition.BinlogGTID,
					currentPosition.BinLogFileName,
					currentPosition.BinLogFilePos)
				return
			}

			// We are using binlog checker to send heartbeat to source db, and
			// the timeout for streamer event is longer than binlog checker interval.
			// If the timeout happens, then we need to try to reopen the binlog syncer.
			if err == context.DeadlineExceeded {
				log.Info("[binlogTailer] BinlogSyncerTimeout try to reopen")
				gtid, err := GetCurrentGTID(tailer.positionCache)
				if err != nil {
					log.Fatalf("[binlogTailer] failed to deserialize position, err: %v", errors.ErrorStack(err))
				}

				streamer, err = tailer.reopenBinlogSyncer(gtid)
				if err != nil {
					log.Fatalf("[binlogTailer] failed reopenBinlogSyncer: %v", errors.ErrorStack(err))
				}
				continue
			}

			if err != nil {
				log.Errorf("get binlog error %v", err)
				if tryReSync && utils.IsBinlogPurgedError(err) {
					time.Sleep(RetryTimeout)

					db := utils.NewMySQLDB(tailer.sourceDB)
					currentPosition, err := GetCurrentPosition(tailer.positionCache)
					if err != nil {
						log.Fatalf("[binlogTailer] failed getCurrentPosition, err: %v", errors.ErrorStack(err))
					}
					p, err := fixGTID(db, *currentPosition)
					if err != nil {
						log.Fatalf("[binlogTailer] failed retrySyncGTIDs: %v", errors.ErrorStack(err))
					}

					log.Infof("retrySyncGTIDs done")
					barrierMsg := NewBarrierMsg(int64(e.Header.Timestamp), tailer.AfterMsgCommit)
					if err := tailer.emitter.Emit(barrierMsg); err != nil {
						log.Fatalf("failed to emit barrier msg: %v", errors.ErrorStack(err))
					}
					<-barrierMsg.Done
					if err := tailer.positionCache.Flush(); err != nil {
						log.Fatalf("[binlogTailer] failed to flush position cache, err: %v", errors.ErrorStack(err))
					}

					tryReSync = false

					// reopen streamer with new position
					streamer, err = tailer.reopenBinlogSyncer(p.BinlogGTID)
					if err != nil {
						log.Fatalf("[binlogTailer] failed reopenBinlogSyncer")
					}

					currentPosition, err = GetCurrentPosition(tailer.positionCache)
					if err != nil {
						log.Fatalf("[binlogTailer] failed to getCurrentPosition, err: %v", errors.ErrorStack(err))
					}
					continue
				}

				if tailer.closed {
					log.Infof("[binlogTailer] tailer closed")
					return
				}

				log.Fatalf("[binlogTailer] unexpected err: %v", errors.ErrorStack(err))
			}

			tryReSync = true

			if tailer.cfg.DebugBinlog && e.Header.EventType != replication.HEARTBEAT_EVENT {
				e.Dump(os.Stdout)
			}

			switch ev := e.Event.(type) {
			case *replication.RotateEvent:
				sourcePosition := gomysql.Position{Name: string(ev.NextLogName), Pos: uint32(ev.Position)}

				// If the currentPos returned from source db is less than the position
				// we have in position store, we skip it.
				currentPos := gomysql.Position{Name: currentPosition.BinLogFileName, Pos: currentPosition.BinLogFilePos}
				if utils.CompareBinlogPos(sourcePosition, currentPos, 0) <= 0 {
					log.Infof(
						"[binlogTailer] skip rotate event: source binlog Name %v, source binlog Pos: %v; store Name: %v, store Pos: %v",
						sourcePosition.Name,
						sourcePosition.Pos,
						currentPos.Name,
						currentPos.Pos,
					)
					continue
				}
				currentPos = sourcePosition
				log.Infof("[binlogTailer] rotate binlog to %v", sourcePosition)
			case *replication.RowsEvent:

				schemaName, tableName := string(ev.Table.Schema), string(ev.Table.Table)
				GravityTableRowsEventSize.
					WithLabelValues(tailer.pipelineName).
					Add(float64(len(ev.Rows)))
				// dead signal is received from special internal table.
				// it is only used for test purpose right now.
				isDeadSignal := mysql_test.IsDeadSignal(schemaName, tableName)
				if isDeadSignal && IsEventBelongsToMyself(ev, tailer.gravityServerID) {
					log.Infof("[binlogTailer] dead signal for myself, exit")
					return
				}

				// skip dead signal for others
				if isDeadSignal {
					log.Infof("[binlogTailer] dead signal for others, continue")
					continue
				}

				// heartbeat message
				isBinlogChecker := binlog_checker.IsBinlogCheckerMsg(schemaName, tableName)
				if isBinlogChecker {
					eventType := e.Header.EventType
					if eventType == replication.UPDATE_ROWS_EVENTv0 || eventType == replication.UPDATE_ROWS_EVENTv1 || eventType == replication.UPDATE_ROWS_EVENTv2 {
						checkerRow, err := binlog_checker.ParseMySQLRowEvent(ev)
						if err != nil {
							log.Fatalf("[binlogTailer] failed to parse mysql row event: %v", errors.ErrorStack(err))
						}
						if !tailer.binlogChecker.IsEventBelongsToMySelf(checkerRow) {
							log.Debugf("[binlogTailer] heartbeat for others")
							continue
						}
						tailer.binlogChecker.MarkActive(checkerRow)
					}
				}

				// skip binlog position event
				if position_store.IsPositionStoreEvent(schemaName, tableName) {
					log.Debugf("[binlogTailer] skip position event")
					continue
				}

				// TODO refactor the above functions: dead signal, heartbeat, event store
				// to use a unified filter pipelines.
				if tailer.binlogEventSchemaFilter != nil {
					if !tailer.binlogEventSchemaFilter(schemaName) {
						continue
					}
				}

				schema, err := tailer.sourceSchemaStore.GetSchema(schemaName)
				if err != nil {
					log.Fatalf("[binlogTailer] failed GetSchema %v. err: %v.", schemaName, errors.ErrorStack(err))
				}

				tableDef := schema[tableName]
				if tableDef == nil {
					log.Fatalf("[binlogTailer] failed to get table def, schemaName: %v, tableName: %v", schemaName, tableName)
				}

				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					GravityTableInsertCounter.WithLabelValues(tailer.pipelineName, schemaName).Add(1)

					log.Debugf("[binlogTailer] Insert rows %s.%s.", schemaName, tableName)

					msgs, err := NewInsertMsgs(
						tailer.cfg.Source.Host,
						schemaName,
						tableName,
						int64(e.Header.Timestamp),
						ev,
						tableDef,
					)
					if err != nil {
						log.Fatalf("[binlogTailer] insert m failed %v", errors.ErrorStack(err))
					}

					for _, m := range msgs {
						tailer.AppendMsgTxnBuffer(m)
					}
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					GravityTableUpdateCounter.WithLabelValues(tailer.pipelineName, schemaName).Add(1)

					if isBinlogChecker {
						log.Debug(".")
					} else {
						log.Debugf("[binlogTailer] Update rows with: schemaName: %v, tableName: %v", schemaName, tableName)
					}

					msgs, err := NewUpdateMsgs(
						tailer.cfg.Source.Host,
						schemaName,
						tableName,
						int64(e.Header.Timestamp),
						ev,
						tableDef)
					if err != nil {
						log.Fatalf("[binlogTailer] failed to genUpdateJobs %v", errors.ErrorStack(err))
					}
					for _, m := range msgs {
						tailer.AppendMsgTxnBuffer(m)
					}
				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					GravityTableDeleteCounter.WithLabelValues(tailer.pipelineName, schemaName).Add(1)

					msgs, err := NewDeleteMsgs(
						tailer.cfg.Source.Host,
						schemaName,
						tableName,
						int64(e.Header.Timestamp),
						ev,
						tableDef)
					if err != nil {
						log.Fatalf("[binlogTailer] failed to generate delete m: %v", errors.ErrorStack(err))
					}
					for _, m := range msgs {
						tailer.AppendMsgTxnBuffer(m)
					}
					//log.Info("Delete rows.")
				default:
					log.Fatalf("unknown rows event: %v", e.Header.EventType)
				}
			case *replication.QueryEvent:

				ddlSQL := strings.TrimSpace(string(ev.Query))

				// Begin comes after every CUD event so ignore
				if ddlSQL == "BEGIN" {
					continue
				}

				if strings.Contains(ddlSQL, consts.DDLTag) {
					log.Infof("ignore internal ddl: %s", ddlSQL)
					continue
				}

				dbName, table, ast := extractSchemaNameFromDDLQueryEvent(tailer.parser, ev)

				if dbName == consts.GravityDBName || dbName == "mysql" || dbName == "drc" {
					continue
				}

				tailer.sourceSchemaStore.InvalidateSchemaCache(dbName)

				log.Infof("QueryEvent: database: %s, sql: %s", dbName, ddlSQL)

				if tailer.binlogEventSchemaFilter != nil {
					if !tailer.binlogEventSchemaFilter(dbName) {
						continue
					}
				}

				// emit barrier msg
				barrierMsg := NewBarrierMsg(int64(e.Header.Timestamp), tailer.AfterMsgCommit)
				if err := tailer.emitter.Emit(barrierMsg); err != nil {
					log.Fatalf("failed to emit barrier msg: %v", errors.ErrorStack(err))
				}
				<-barrierMsg.Done
				if err := tailer.positionCache.Flush(); err != nil {
					log.Fatalf("[binlogTailer] failed to flush position cache, err: %v", errors.ErrorStack(err))
				}

				// emit ddl msg
				ddlMsg := NewDDLMsg(
					tailer.AfterMsgCommit,
					dbName,
					table,
					ast,
					ddlSQL,
					int64(e.Header.Timestamp),
					*currentPosition)
				if err := tailer.emitter.Emit(ddlMsg); err != nil {
					log.Fatalf("failed to emit ddl msg: %v", errors.ErrorStack(err))
				}
				<-ddlMsg.Done
				if err := tailer.positionCache.Flush(); err != nil {
					log.Fatalf("[binlogTailer] failed to flush position cache, err: %v", errors.ErrorStack(err))
				}

				log.Infof("[binlogTailer] ddl done with gtid: %v", ev.GSet.String())
			case *replication.GTIDEvent:
				// GTID stands for Global Transaction IDentifier
				// It is composed of two parts:
				//   - SID for Source Identifier, and
				//   - GNO for Group Number.
				// The basic idea is to
				//    -  Associate an identifier, the Global Transaction IDentifier or GTID,
				// 	  to every transaction.
				//    -  When a transaction is copied to a slave, re-executed on the slave,
				// 	  and written to the slave's binary log, the GTID is preserved.
				//    -  When a  slave connects to a master, the slave uses GTIDs instead of
				// 	  (file, offset)

				// This is the start of a txn.
				// It contains GTID_NEXT, for example:
				//
				// Log position: 259
				// Event size: 65
				// Commit flag: 1
				// GTID_NEXT: 58ff439a-c2e2-11e6-bdc7-125c95d674c1:2225062
				// LAST_COMMITTED: 0
				// SEQUENCE_NUMBER: 1
				//

				// This is the start of a txn.
				currentPosition.BinLogFilePos = e.Header.LogPos

				u, err := uuid.FromBytes(ev.SID)
				if err != nil {
					log.Fatalf("[binlogTailer] failed at GTIDEvent %v", errors.ErrorStack(err))
				}
				eventGTIDString := fmt.Sprintf("%s:1-%d", u.String(), ev.GNO)
				eventUUIDSet, err := gomysql.ParseUUIDSet(eventGTIDString)
				if err != nil {
					log.Fatalf("[binlogTailer] failed at ParseUUIDSet %v", errors.ErrorStack(err))
				}

				currentGTIDSet, err := gomysql.ParseMysqlGTIDSet(currentPosition.BinlogGTID)
				if err != nil {
					log.Fatalf("[binlogTailer] failed at ParseMysqlGTIDSet %v", errors.ErrorStack(err))
				}
				s := currentGTIDSet.(*gomysql.MysqlGTIDSet)
				s.AddSet(eventUUIDSet)

				currentPosition.BinlogGTID = s.String()

				metrics.GravityBinlogGTIDGaugeVec.WithLabelValues(tailer.pipelineName, "gravity").Set(float64(ev.GNO))
				GravityTableRowsEventSize.
					WithLabelValues(tailer.pipelineName).
					Set(0.0)

				tailer.ClearMsgTxnBuffer()
			case *replication.XIDEvent:
				// An XID event is generated for a commit of a transaction that modifies one or
				// more tables of an XA-capable storage engine.

				// It contains GTIDSet that is executed, for example:
				//
				// 	Log position: 525
				// 	Event size: 31
				//  XID: 243
				//  GTIDSet: 58ff439a-c2e2-11e6-bdc7-125c95d674c1:1-2225062
				//
				m := NewXIDMsg(int64(e.Header.Timestamp), tailer.AfterMsgCommit, *currentPosition)
				if err != nil {
					log.Fatalf("[binlogTailer] failed: %v", errors.ErrorStack(err))
				}

				tailer.AppendMsgTxnBuffer(m)
				tailer.FlushMsgTxnBuffer()
			}
		}
	}()

	return nil
}

func (tailer *BinlogTailer) AfterMsgCommit(msg *core.Msg) error {
	ctx := msg.InputContext.(inputContext)
	if ctx.op == xid || ctx.op == ddl {

		startPosition, currentPosition, err := GetBinlogPositions(tailer.positionCache)
		if err != nil {
			return errors.Trace(err)
		}
		*currentPosition = ctx.position

		v, err := helper.SerializeBinlogPositions(&helper.BinlogPositions{StartPosition: startPosition, CurrentPosition: currentPosition})
		if err != nil {
			return errors.Trace(err)
		}

		position := position_store.Position{
			Name:  tailer.pipelineName,
			Stage: config.Stream,
			Value: v,
		}
		tailer.positionCache.Put(position)
	}

	return nil
}

func (tailer *BinlogTailer) Close() {
	// if tailer.closed {
	// 	return
	// }

	// tailer.closed = true

	log.Infof("closing binlogTailer...")

	tailer.cancel()

	log.Infof("cancel func called")
	tailer.wg.Wait()

	tailer.binlogSyncer.Close()

	log.Infof("binlogTailer closed")
}

func (tailer *BinlogTailer) Wait() {
	tailer.wg.Wait()
}

// AppendMsgTxnBuffer adds basic job information to txn buffer
func (tailer *BinlogTailer) AppendMsgTxnBuffer(msg *core.Msg) {
	tailer.msgTxnBuffer = append(tailer.msgTxnBuffer, msg)

	// the main purpose of txn buffer is to filter out internal data,
	// since we don't have internal txn that updates rows exceed the TxnBufferLimit,
	// it is ok to just flush and clear the txn buffer when the limits comes.
	if len(tailer.msgTxnBuffer) >= config.TxnBufferLimit {
		tailer.FlushMsgTxnBuffer()
		tailer.ClearMsgTxnBuffer()
	}
}

// FlushMsgTxnBuffer will flush job in txn  buffer to queue.
// We will also filter out job that don't need to send out in this stage.
func (tailer *BinlogTailer) FlushMsgTxnBuffer() {

	// ignore internal drc txn data
	isBiDirectionalTxn := false
	for _, msg := range tailer.msgTxnBuffer {
		if utils.IsInternalTraffic(msg.Database, msg.Table) {
			isBiDirectionalTxn = true
			log.Debugf("[binlogTailer] bidirectional transaction will be ignored")
			break
		}
	}
	if isBiDirectionalTxn && tailer.cfg.IgnoreBiDirectionalData {
		// only keep the xid message here.
		txnBufferLen := len(tailer.msgTxnBuffer)
		lastMsg := tailer.msgTxnBuffer[txnBufferLen-1]
		ctx := lastMsg.InputContext.(inputContext)
		if ctx.op != xid {
			return
		}
		tailer.msgTxnBuffer = []*core.Msg{lastMsg}
	}

	for i, m := range tailer.msgTxnBuffer {
		if binlog_checker.IsBinlogCheckerMsg(m.Database, m.Table) || m.Database == consts.GravityDBName {
			m.Type = core.MsgCtl
		}
		ctx := m.InputContext.(inputContext)

		if err := tailer.emitter.Emit(m); err != nil {
			log.Fatalf("failed to emit, idx: %d, schema: %v, table: %v, msgType: %v, op: %v, err: %v",
				i, m.Database, m.Table, m.Type, ctx.op, errors.ErrorStack(err))
		} else {

		}
	}
}

func (tailer *BinlogTailer) ClearMsgTxnBuffer() {
	tailer.msgTxnBuffer = nil
}

func (tailer *BinlogTailer) getBinlogStreamer(gtid string) (*replication.BinlogStreamer, error) {

	log.Infof("[binlogTailer] getBinlogStreamer gtid: %v", gtid)

	gs, err := gomysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	streamer, err := tailer.binlogSyncer.StartSyncGTID(gs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return streamer, nil
}

func (tailer *BinlogTailer) reopenBinlogSyncer(gtidString string) (*replication.BinlogStreamer, error) {
	tailer.binlogSyncer.Close()
	tailer.binlogSyncer = utils.NewBinlogSyncer(tailer.gravityServerID, tailer.cfg.Source)
	return tailer.getBinlogStreamer(gtidString)
}

func fixGTID(db utils.MySQLStatusGetter, binlogPosition utils.MySQLBinlogPosition) (*utils.MySQLBinlogPosition, error) {
	log.Infof("[fixGTID] gtid: %v", binlogPosition.BinlogGTID)

	pos, gs, err := ToGoMySQLPosition(binlogPosition)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("[fixGTID] old gtid set %v", gs.String())

	_, newGS, err := db.GetMasterStatus()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// find master UUID and ignore it
	masterUUID, err := db.GetServerUUID()
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("[fixGTID] master gtid set %v, master uuid %s", newGS, masterUUID)

	// remove useless gtid from
	for uuid := range gs.Sets {
		if _, ok := newGS.Sets[uuid]; !ok {
			delete(gs.Sets, uuid)
		}
	}

	if len(gs.Sets) != len(newGS.Sets) {
		// remove master gtid from currentGTIDs, so that we can start from this master's first txn
		delete(newGS.Sets, masterUUID)

		// add unknown gtid
		for uuid, uuidSet := range newGS.Sets {
			if _, ok := gs.Sets[uuid]; !ok {
				gs.AddSet(uuidSet)
			}
		}
	}

	positionConfig := utils.MySQLBinlogPosition{
		BinLogFileName: pos.Name,
		BinLogFilePos:  pos.Pos,
		BinlogGTID:     gs.String(),
	}
	log.Infof("[fixGTID] cleaned positionConfig: %v, gs: %v, gsString: %v", positionConfig, gs.Sets, gs.String())
	return &positionConfig, nil

}
