package job_processor

//
// import (
// 	"fmt"
// 	"time"
//
// 	"github.com/juju/errors"
// 	"github.com/satori/go.uuid"
// 	gomysql "github.com/siddontang/go-mysql/mysql"
// 	"github.com/siddontang/go-mysql/replication"
// 	log "github.com/sirupsen/logrus"
//
// 	"github.com/moiot/gravity/pkg/core"
//
// 	"github.com/moiot/gravity/pkg/config"
// 	"github.com/moiot/gravity/pkg/mysql/job_msg"
// 	mysqlUtils "github.com/moiot/gravity/pkg/mysql/utils"
// 	"github.com/moiot/gravity/pkg/utils"
// 	"github.com/moiot/gravity/schema_store"
// )
//
// type processor interface {
// 	Process(binlogEvent *replication.BinlogEvent) error
// }
//
// const SequenceNumberBase = int64(0)
//
// var sequenceNumber = SequenceNumberBase
//
// type JobProcessor struct {
// 	emitter         core.Emitter
// 	msgBuffer       []*core.Msg
// 	canSend         bool
// 	currentPos      gomysql.Position
// 	currentUUIDSet  *gomysql.UUIDSet
// 	currentGTIDSet  gomysql.MysqlGTIDSet
// 	schemaName      string
// 	srcId           string
// 	schemaStore     schema_store.SchemaStore
// 	tableConfigList []config.TableConfig
// }
//
// func getLatestSequenceNumber() int64 {
// 	sequenceNumber += 1
// 	return sequenceNumber
// }
//
// func NewJobProcessor(emitter core.Emitter, startPos gomysql.Position, schemaName string, srcId string, schemaStore schema_store.SchemaStore) JobProcessor {
// 	return JobProcessor{
// 		emitter:        emitter,
// 		msgBuffer:      nil,
// 		canSend:        true,
// 		currentPos:     startPos,
// 		currentUUIDSet: nil,
// 		currentGTIDSet: gomysql.MysqlGTIDSet{Sets: make(map[string]*gomysql.UUIDSet)},
// 		schemaName:     schemaName,
// 		srcId:          srcId,
// 		schemaStore:    schemaStore,
// 	}
// }
//
// func (jobProcessor *JobProcessor) Process(e *replication.BinlogEvent) error {
// 	switch ev := e.Event.(type) {
// 	case *replication.RotateEvent:
// 		jobProcessor.currentPos = gomysql.Position{Name: string(ev.NextLogName), Pos: uint32(ev.Position)}
// 		log.Infof("[job_processor] rotate binlog to %v", jobProcessor.currentPos)
//
// 	case *replication.RowsEvent:
// 		// operations which have impact on rows.
// 		if string(ev.Table.Schema) == jobProcessor.schemaName {
// 			tableName := string(ev.Table.Table)
// 			schema, err := jobProcessor.schemaStore.GetSchema(jobProcessor.schemaName)
// 			if err != nil {
// 				return errors.Trace(errors.Annotatef(err, "[job_processor] failed to GetSchema"))
// 			}
// 			table := schema[tableName]
// 			var tableConfig *config.TableConfig = nil
// 			for i, tc := range jobProcessor.tableConfigList {
// 				if utils.Glob(tc.Schema, jobProcessor.schemaName) && utils.Glob(tc.Table, tableName) {
// 					tableConfig = &jobProcessor.tableConfigList[i]
// 					break
// 				}
// 			}
// 			jobList, err := processRowsEvent(e.Header, ev, jobProcessor.srcId, table, jobProcessor.currentPos, jobProcessor.currentGTIDSet, tableConfig)
// 			if err != nil {
// 				return errors.Trace(errors.Annotatef(err, "[job_processor] failed to process RowsEvent: %v", e.Header.EventType))
// 			}
// 			jobProcessor.jobBuffer = append(jobProcessor.jobBuffer, jobList...)
// 		}
//
// 	case *replication.GTIDEvent:
// 		// This is the start of a txn.
// 		jobProcessor.currentPos.Pos = e.Header.LogPos
// 		u, err := uuid.FromBytes(ev.SID)
// 		if err != nil {
// 			return errors.Trace(errors.Annotate(err, "[job_processor] failed to parse GTIDEvent"))
// 		}
// 		gtidString := fmt.Sprintf("%s:1-%d", u.String(), ev.GNO)
// 		jobProcessor.currentUUIDSet, err = gomysql.ParseUUIDSet(gtidString)
// 		if err != nil {
// 			return errors.Annotate(err, "[job_processor] failed to ParseUUIDSet")
// 		}
// 		jobProcessor.canSend = false
//
// 	case *replication.XIDEvent:
// 		// This is the end of a txn.
// 		jobProcessor.currentPos.Pos = e.Header.LogPos
// 		jobProcessor.currentGTIDSet.AddSet(jobProcessor.currentUUIDSet)
// 		jobProcessor.canSend = true
//
// 	case *replication.QueryEvent:
// 		jobProcessor.currentPos.Pos = e.Header.LogPos
// 		// TODO: Do not handle DDL right now
// 		log.Debugf("[job_processor] skip query event: %v", e)
//
// 	default:
// 		log.Debugf("[job_processor] un-supported event type: %v", e)
//
// 	}
//
// 	if len(jobProcessor.jobBuffer) > 0 && jobProcessor.canSend {
// 		jobProcessor.flushJobBuffer()
// 	}
// 	return nil
// }
//
// func CreateInsertJobs(header *replication.EventHeader, ev *replication.RowsEvent, srcId string, table *schema_store.Table, pos gomysql.Position, gs gomysql.MysqlGTIDSet, tableConfig *config.TableConfig) ([]Job, error) {
// 	jobList := make([]Job, len(ev.Rows))
// 	columns := table.Columns
// 	pkColumns := job_msg.BuildPkColumns(table, tableConfig)
// 	pcs := make([]string, len(pkColumns))
// 	for i, c := range pkColumns {
// 		pcs[i] = c.Name
// 	}
// 	for rowIndex, row := range ev.Rows {
// 		if len(row) != len(columns) {
// 			return nil, errors.Trace(errors.Errorf("pos: %s, gs: %s, and data mismatch in length: %d vs %d, table %v, row: %v",
// 				pos.String(), gs.String(), len(columns), len(row), table, row))
// 		}
// 		dmlMsg := &core.DMLMsg{}
// 		dmlMsg.Operation = core.Insert
// 		data := make(map[string]interface{})
// 		for i := 0; i < len(columns); i++ {
// 			data[columns[i].Name] = schema_store.Deserialize(row[i], columns[i])
// 		}
// 		dmlMsg.Data = data
// 		pks, err := job_msg.GenPrimaryKeys(pkColumns, data)
// 		if err != nil {
// 			return nil, errors.Trace(err)
// 		}
// 		dmlMsg.Pks = pks
// 		dmlMsg.PkColumns = pcs
//
// 		jobMsg := core.Msg{
// 			Database:  table.Schema,
// 			Table:     table.Name,
// 			Timestamp: time.Unix(int64(header.Timestamp), 0),
// 		}
// 		jobMsg.DmlMsg = dmlMsg
//
// 		jobList[rowIndex] = CreateJob(getLatestSequenceNumber(), srcId, mysqlUtils.Insert, jobMsg, pos, gs)
// 	}
// 	return jobList, nil
// }
//
// func CreateUpdateJobs(header *replication.EventHeader, ev *replication.RowsEvent, srcId string, table *schema_store.Table, pos gomysql.Position, gs gomysql.MysqlGTIDSet, tableConfig *config.TableConfig) ([]Job, error) {
// 	jobList := make([]Job, len(ev.Rows)/2)
// 	columns := table.Columns
// 	pkColumns := job_msg.BuildPkColumns(table, tableConfig)
// 	pcs := make([]string, len(pkColumns))
// 	for i, c := range pkColumns {
// 		pcs[i] = c.Name
// 	}
// 	for rowIndex := 0; rowIndex < len(ev.Rows); rowIndex += 2 {
// 		oldData := ev.Rows[rowIndex]
// 		changedData := ev.Rows[rowIndex+1]
//
// 		if len(oldData) != len(changedData) {
// 			return nil, errors.Errorf("pos: %s, gs: %s, update %s.%s data mismatch in length: %d vs %d",
// 				pos.String(), gs.String(), table.Schema, table.Name, len(oldData), len(changedData))
// 		}
//
// 		if len(oldData) != len(columns) {
// 			return nil, errors.Errorf("pos: %s, gs: %s, update %s.%s columns and data mismatch in length: %d vs %d",
// 				pos.String(), gs.String(), table.Schema, table.Name, len(columns), len(oldData))
// 		}
// 		dmlMsg := &core.DMLMsg{
// 			Operation: core.Update,
// 		}
//
// 		data := make(map[string]interface{})
// 		old := make(map[string]interface{})
// 		for i := 0; i < len(oldData); i++ {
// 			data[columns[i].Name] = schema_store.Deserialize(changedData[i], columns[i])
// 			old[columns[i].Name] = schema_store.Deserialize(oldData[i], columns[i])
// 		}
// 		dmlMsg.Data = data
// 		dmlMsg.Old = old
// 		pks, err := job_msg.GenPrimaryKeys(pkColumns, data)
// 		if err != nil {
// 			return nil, errors.Trace(err)
// 		}
// 		dmlMsg.Pks = pks
// 		dmlMsg.PkColumns = pcs
// 		idx := rowIndex / 2
//
// 		jobMsg := core.Msg{
// 			Database:  table.Schema,
// 			Table:     table.Name,
// 			Timestamp: time.Unix(int64(header.Timestamp), 0),
// 		}
// 		jobMsg.DmlMsg = dmlMsg
// 		jobList[idx] = CreateJob(getLatestSequenceNumber(), srcId, mysqlUtils.Update, jobMsg, pos, gs)
// 	}
// 	return jobList, nil
// }
//
// func CreateDeleteJobs(header *replication.EventHeader, ev *replication.RowsEvent, srcId string, table *schema_store.Table, pos gomysql.Position, gs gomysql.MysqlGTIDSet,
// 	tableConfig *config.TableConfig) ([]Job, error) {
// 	jobList := make([]Job, len(ev.Rows))
// 	columns := table.Columns
// 	pkColumns := job_msg.BuildPkColumns(table, tableConfig)
// 	pcs := make([]string, len(pkColumns))
// 	for i, c := range pkColumns {
// 		pcs[i] = c.Name
// 	}
// 	for rowIndex, row := range ev.Rows {
// 		if len(row) != len(columns) { // TODO get col name from table map events
// 			return nil, errors.Errorf("pos: %s, gs: %s, delete %s.%s columns and data mismatch in length: %d vs %d",
// 				pos.String(), gs.String(), table.Schema, table.Name, len(columns), len(row))
// 		}
// 		dmlMsg := &core.DMLMsg{
// 			Operation: core.Delete,
// 		}
// 		data := make(map[string]interface{})
// 		for i := 0; i < len(columns); i++ {
// 			data[columns[i].Name] = schema_store.Deserialize(row[i], columns[i])
// 		}
// 		dmlMsg.Data = data
// 		pks, err := job_msg.GenPrimaryKeys(pkColumns, data)
// 		if err != nil {
// 			return nil, errors.Trace(err)
// 		}
// 		dmlMsg.Pks = pks
// 		dmlMsg.PkColumns = pcs
//
// 		jobMsg := core.Msg{
// 			Database:  table.Schema,
// 			Table:     table.Name,
// 			TableDef:  table,
// 			Timestamp: time.Unix(int64(header.Timestamp), 0),
// 			Position:  gs.String(),
// 		}
// 		jobMsg.DmlMsg = dmlMsg
// 		jobList[rowIndex] = CreateJob(getLatestSequenceNumber(), srcId, mysqlUtils.Del, jobMsg, pos, gs)
// 	}
//
// 	return jobList, nil
// }
//
// func processRowsEvent(header *replication.EventHeader, ev *replication.RowsEvent, srcId string, table *schema_store.Table,
// 	pos gomysql.Position, gs gomysql.MysqlGTIDSet, tableConfig *config.TableConfig) ([]Job, error) {
// 	eventType := header.EventType
// 	log.Debugf("[job_processor] process rows event started. event type: %v", eventType)
// 	var jobList []Job
// 	var err error
// 	switch eventType {
// 	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
// 		jobList, err = CreateInsertJobs(header, ev, srcId, table, pos, gs, tableConfig)
//
// 	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
// 		jobList, err = CreateUpdateJobs(header, ev, srcId, table, pos, gs, tableConfig)
//
// 	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
// 		jobList, err = CreateDeleteJobs(header, ev, srcId, table, pos, gs, tableConfig)
//
// 	default:
// 		err = errors.NotValidf(fmt.Sprintf("[job_processor] un-expected rows event type: %v", eventType))
// 	}
// 	if err != nil {
// 		return nil, err
// 	}
// 	log.Debugf("[job_processor] process rows event finished. event type: %v", eventType)
// 	return jobList, nil
// }
//
// func (jobProcessor *JobProcessor) flushJobBuffer() {
// 	for _, job := range jobProcessor.jobBuffer {
// 		if err := jobProcessor.scheduler.SubmitJob(job); err != nil {
// 			log.Fatalf("%v", errors.ErrorStack(err))
// 		}
// 	}
// 	jobProcessor.jobBuffer = nil
// }
