package job_processor

//
// import (
// 	"math/rand"
//
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// 	gomysql "github.com/siddontang/go-mysql/mysql"
// 	r "github.com/siddontang/go-mysql/replication"
//
// 	"github.com/moiot/gravity/pkg/core"
//
// 	"fmt"
//
// 	"github.com/juju/errors"
//
// 	"time"
//
// 	"github.com/BurntSushi/toml"
//
// 	mysqlUtils "github.com/moiot/gravity/pkg/mysql/utils"
// 	"github.com/moiot/gravity/pkg/mysql_test"
// 	"github.com/moiot/gravity/pkg/utils"
// 	"github.com/moiot/gravity/pkg/worker_pool"
// 	s "github.com/moiot/gravity/schema_store"
// )
//
// var (
// 	schemaStoreTestDB      = "padderTestDB"
// 	sourceIdentity         = schemaStoreTestDB + ".*"
// 	schemaStore            *s.SimpleSchemaStore
// 	table                  *s.Table
// 	schema                 *s.Schema
// 	originRow              = []interface{}{rand.Int(), "old", "old", time.Now()}
// 	validRowsEventTypeList = [9]r.EventType{
// 		r.WRITE_ROWS_EVENTv0, r.WRITE_ROWS_EVENTv1, r.WRITE_ROWS_EVENTv2,
// 		r.UPDATE_ROWS_EVENTv0, r.UPDATE_ROWS_EVENTv1, r.UPDATE_ROWS_EVENTv2,
// 		r.DELETE_ROWS_EVENTv0, r.DELETE_ROWS_EVENTv1, r.DELETE_ROWS_EVENTv2,
// 	}
// )
//
// type MockScheduler struct {
// 	QueueSize int
// }
//
// func (ms *MockScheduler) Healthy() bool {
// 	return true
// }
//
// func (ms *MockScheduler) SubmitJob(job worker_pool.Job) error {
// 	ms.QueueSize += 1
// 	return nil
// }
//
// func (ms *MockScheduler) AckJob(job worker_pool.Job) error {
// 	return nil
// }
//
// func (ms *MockScheduler) Start() error {
// 	return nil
// }
//
// func (ms *MockScheduler) Close() {
//
// }
//
// var _ = Describe("padder job processor test", func() {
// 	schemaStore, err := createSimpleSchemaStore(schemaStoreTestDB)
// 	if err != nil {
// 		Fail(fmt.Sprintf("failed to createSimpleSchemaStore failed. %v", err))
// 	}
// 	BeforeEach(func() {
// 		schema, err := schemaStore.GetSchema(schemaStoreTestDB)
// 		if err != nil {
// 			Fail(fmt.Sprintf("failed to GetSchema. %v", err))
// 		}
// 		table = schema[mysql_test.TestTableName]
// 	})
//
// 	Describe("processRowsEvent test", func() {
// 		It("process insert event normally", func() {
// 			for _, eventType := range []r.EventType{r.WRITE_ROWS_EVENTv0, r.WRITE_ROWS_EVENTv1, r.WRITE_ROWS_EVENTv2} {
// 				eventHeader := &r.EventHeader{EventType: eventType}
// 				columnCount := uint64(len(table.Columns))
// 				rows := [][]interface{}{originRow}
// 				rowEvent := &r.RowsEvent{
// 					ColumnCount: columnCount,
// 					Rows:        rows}
// 				expectPos := gomysql.Position{Name: "test", Pos: 1}
// 				expectGTIDSet := gomysql.MysqlGTIDSet{}
// 				jobList, err := processRowsEvent(eventHeader, rowEvent, sourceIdentity, table, expectPos, gomysql.MysqlGTIDSet{}, nil)
// 				Expect(err).To(BeNil())
// 				processRowsEventHelper(rowEvent, jobList)
// 				Expect(jobList[0].opType).To(BeEquivalentTo(mysqlUtils.Insert))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["id"]).To(BeEquivalentTo(rows[0][0]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["name"]).To(BeEquivalentTo(rows[0][1]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["email"]).To(BeEquivalentTo(rows[0][2]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["ts"]).To(BeEquivalentTo(rows[0][3]))
// 				Expect(jobList[0].pos).To(BeEquivalentTo(expectPos))
// 				Expect(jobList[0].gtidSet).To(BeEquivalentTo(expectGTIDSet))
// 			}
// 		})
// 		It("process update event normally", func() {
// 			for _, eventType := range []r.EventType{r.UPDATE_ROWS_EVENTv0, r.UPDATE_ROWS_EVENTv1, r.UPDATE_ROWS_EVENTv2} {
// 				eventHeader := &r.EventHeader{EventType: eventType}
// 				columnCount := uint64(len(table.Columns))
// 				rows := [][]interface{}{originRow}
// 				nRow := []interface{}{rows[0][0], "new", "new", time.Now()}
// 				rows = append(rows, nRow)
// 				rowEvent := &r.RowsEvent{
// 					ColumnCount: columnCount,
// 					Rows:        rows}
// 				expectPos := gomysql.Position{Name: "test", Pos: 1}
// 				expectGTIDSet := gomysql.MysqlGTIDSet{}
// 				jobList, err := processRowsEvent(eventHeader, rowEvent, sourceIdentity, table, expectPos, gomysql.MysqlGTIDSet{}, nil)
// 				Expect(err).To(BeNil())
// 				processRowsEventHelper(rowEvent, jobList)
// 				Expect(jobList[0].opType).To(BeEquivalentTo(mysqlUtils.Update))
// 				Expect(jobList[0].JobMsg.DmlMsg.Operation).To(BeEquivalentTo(core.Update))
// 				Expect(jobList[0].JobMsg.DmlMsg.Old["id"]).To(BeEquivalentTo(rows[0][0]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Old["name"]).To(BeEquivalentTo(rows[0][1]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Old["email"]).To(BeEquivalentTo(rows[0][2]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Old["ts"]).To(BeEquivalentTo(rows[0][3]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["id"]).To(BeEquivalentTo(rows[1][0]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["name"]).To(BeEquivalentTo(rows[1][1]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["email"]).To(BeEquivalentTo(rows[1][2]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["ts"]).To(BeEquivalentTo(rows[1][3]))
// 				Expect(jobList[0].pos).To(BeEquivalentTo(expectPos))
// 				Expect(jobList[0].gtidSet).To(BeEquivalentTo(expectGTIDSet))
// 			}
//
// 		})
// 		It("process delete event normally", func() {
// 			for _, eventType := range []r.EventType{r.DELETE_ROWS_EVENTv0, r.DELETE_ROWS_EVENTv1, r.DELETE_ROWS_EVENTv2} {
// 				eventHeader := &r.EventHeader{EventType: eventType}
// 				columnCount := uint64(len(table.Columns))
// 				rows := [][]interface{}{originRow}
// 				rowEvent := &r.RowsEvent{
// 					ColumnCount: columnCount,
// 					Rows:        rows}
// 				expectPos := gomysql.Position{Name: "test", Pos: 1}
// 				expectGTIDSet := gomysql.MysqlGTIDSet{}
// 				jobList, err := processRowsEvent(eventHeader, rowEvent, sourceIdentity, table, expectPos, gomysql.MysqlGTIDSet{}, nil)
// 				Expect(err).To(BeNil())
// 				processRowsEventHelper(rowEvent, jobList)
// 				Expect(jobList[0].opType).To(BeEquivalentTo(mysqlUtils.Del))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["id"]).To(BeEquivalentTo(rows[0][0]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["name"]).To(BeEquivalentTo(rows[0][1]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["email"]).To(BeEquivalentTo(rows[0][2]))
// 				Expect(jobList[0].JobMsg.DmlMsg.Data["ts"]).To(BeEquivalentTo(rows[0][3]))
// 				Expect(jobList[0].pos).To(BeEquivalentTo(expectPos))
// 				Expect(jobList[0].gtidSet).To(BeEquivalentTo(expectGTIDSet))
// 			}
// 		})
// 		It("report an error when no primary keys found ", func() {
// 			for _, eventType := range validRowsEventTypeList {
// 				eventHeader := &r.EventHeader{EventType: eventType}
// 				columnCount := uint64(len(table.Columns))
// 				rows := [][]interface{}{{nil, "old", "old"}, {nil, "new", "new"}}
// 				rowEvent := &r.RowsEvent{
// 					ColumnCount: columnCount,
// 					Rows:        rows}
// 				expectPos := gomysql.Position{Name: "test", Pos: 1}
// 				_, err := processRowsEvent(eventHeader, rowEvent, sourceIdentity, table, expectPos, gomysql.MysqlGTIDSet{}, nil)
// 				Expect(err).Should(HaveOccurred())
// 			}
// 		})
// 		It("report an error with un-supported event", func() {
// 			support := func(target r.EventType) bool {
// 				for _, element := range validRowsEventTypeList {
// 					if target == element {
// 						return true
// 					}
// 				}
// 				return false
// 			}
// 			unSupportedEvent := func() r.EventType {
// 				// see event set in github.com/siddontang/go-mysql/r/const.go
// 				event := r.EventType(byte(rand.Intn(36)))
// 				for support(event) {
// 					event = r.EventType(byte(rand.Intn(36)))
// 				}
// 				return event
// 			}()
// 			mysqlEventHeader := &r.EventHeader{EventType: unSupportedEvent}
// 			_, err := processRowsEvent(mysqlEventHeader, nil, sourceIdentity, nil, gomysql.Position{}, gomysql.MysqlGTIDSet{}, nil)
// 			Expect(errors.IsNotValid(err)).To(BeEquivalentTo(true))
//
// 		})
// 		It("report an error when row does NOT match with columns ", func() {
// 			for _, eventType := range validRowsEventTypeList {
// 				eventHeader := &r.EventHeader{EventType: eventType}
// 				columnCount := uint64(len(table.Columns))
// 				rows := [][]interface{}{{rand.Int(), "old"}, {rand.Int(), "old"}}
// 				rowEvent := &r.RowsEvent{
// 					ColumnCount: columnCount,
// 					Rows:        rows}
// 				expectPos := gomysql.Position{Name: "test", Pos: 1}
// 				_, err := processRowsEvent(eventHeader, rowEvent, sourceIdentity, table, expectPos, gomysql.MysqlGTIDSet{}, nil)
// 				Expect(true).To(BeEquivalentTo(err != nil))
// 			}
// 		})
//
// 	})
//
// 	Describe("processor test", func() {
// 		It("process one rotate event normally", func() {
// 			rotateEvent := &r.RotateEvent{
// 				Position:    1,
// 				NextLogName: []byte("nextLogName"),
// 			}
// 			expectPos := gomysql.Position{Name: string(rotateEvent.NextLogName), Pos: uint32(rotateEvent.Position)}
// 			binLogEvent := &r.BinlogEvent{RawData: nil, Header: nil, Event: rotateEvent}
// 			jobProducer := NewJobProcessor(nil, gomysql.Position{Name: "test", Pos: 0}, "", sourceIdentity, nil)
// 			err := jobProducer.Process(binLogEvent)
// 			Expect(err).To(BeNil())
// 			Expect(jobProducer.currentPos).To(BeEquivalentTo(expectPos))
// 		})
// 		It("process rows event normally", func() {
// 			eventHeader := &r.EventHeader{EventType: r.WRITE_ROWS_EVENTv0}
// 			columnCount := uint64(len(table.Columns))
// 			rows := [][]interface{}{originRow}
// 			rowsEvent := &r.RowsEvent{
// 				ColumnCount: columnCount,
// 				Rows:        rows,
// 				Table:       &r.TableMapEvent{Schema: []byte(table.Schema), Table: []byte(table.Name)},
// 			}
// 			expectPos := gomysql.Position{Name: "test", Pos: 1}
// 			binLogEvent := &r.BinlogEvent{RawData: nil, Header: eventHeader, Event: rowsEvent}
// 			mockScheduler := &MockScheduler{}
// 			jobProducer := NewJobProcessor(mockScheduler, expectPos, table.Schema, sourceIdentity, schemaStore)
// 			err := jobProducer.Process(binLogEvent)
// 			Expect(err).To(BeNil())
// 			Expect(jobProducer.currentPos).To(BeEquivalentTo(expectPos))
// 			Expect(jobProducer.canSend).To(BeEquivalentTo(true))
// 			Expect(jobProducer.jobBuffer).To(BeNil())
// 			Expect(mockScheduler.QueueSize).To(BeEquivalentTo(1))
// 		})
// 		It("process a rows event within txn", func() {
// 			eventHeader := &r.EventHeader{EventType: r.WRITE_ROWS_EVENTv0}
// 			columnCount := uint64(len(table.Columns))
// 			rows := [][]interface{}{originRow}
// 			rowsEvent := &r.RowsEvent{
// 				ColumnCount: columnCount,
// 				Rows:        rows,
// 				Table:       &r.TableMapEvent{Schema: []byte(table.Schema), Table: []byte(table.Name)},
// 			}
// 			expectPos := gomysql.Position{Name: "test", Pos: 1}
// 			binLogEvent := &r.BinlogEvent{RawData: nil, Header: eventHeader, Event: rowsEvent}
// 			mockScheduler := &MockScheduler{}
// 			jobProducer := NewJobProcessor(mockScheduler, expectPos, table.Schema, sourceIdentity, schemaStore)
// 			jobProducer.canSend = false
// 			err := jobProducer.Process(binLogEvent)
// 			Expect(err).To(BeNil())
// 			Expect(jobProducer.currentPos).To(BeEquivalentTo(expectPos))
// 			Expect(jobProducer.canSend).To(BeEquivalentTo(false))
// 			Expect(len(jobProducer.jobBuffer)).To(BeEquivalentTo(1))
// 			Expect(mockScheduler.QueueSize).To(BeEquivalentTo(0))
// 		})
// 		//
// 		It("process gtid event normally", func() {
// 			// TODO: How to construct a valid GTIDEvent.GNO?
// 		})
// 		It("process xid event normally", func() {
// 			expectPos := gomysql.Position{Name: "test", Pos: 1}
// 			mockScheduler := &MockScheduler{}
// 			jobProducer := NewJobProcessor(mockScheduler, expectPos, table.Schema, sourceIdentity, schemaStore)
// 			jobProducer.canSend = false
// 			eventHeader := &r.EventHeader{EventType: r.WRITE_ROWS_EVENTv0}
// 			columnCount := uint64(len(table.Columns))
// 			rows := [][]interface{}{originRow}
// 			rowsEvent := &r.RowsEvent{
// 				ColumnCount: columnCount,
// 				Rows:        rows,
// 				Table:       &r.TableMapEvent{Schema: []byte(table.Schema), Table: []byte(table.Name)},
// 			}
// 			binLogEvent := &r.BinlogEvent{RawData: nil, Header: eventHeader, Event: rowsEvent}
// 			err := jobProducer.Process(binLogEvent)
// 			Expect(err).To(BeNil())
// 			Expect(len(jobProducer.jobBuffer)).To(BeEquivalentTo(1))
// 			Expect(mockScheduler.QueueSize).To(BeEquivalentTo(0))
//
// 			gtidEvent := &r.XIDEvent{}
// 			secEventHeader := &r.EventHeader{LogPos: 2}
// 			secondBinLogEvent := &r.BinlogEvent{RawData: nil, Header: secEventHeader, Event: gtidEvent}
// 			secErr := jobProducer.Process(secondBinLogEvent)
// 			Expect(secErr).To(BeNil())
//
// 			Expect(jobProducer.canSend).To(BeEquivalentTo(true))
// 			Expect(jobProducer.jobBuffer).To(BeNil())
// 			Expect(mockScheduler.QueueSize).To(BeEquivalentTo(1))
// 			Expect(jobProducer.currentPos.Pos).To(BeEquivalentTo(secEventHeader.LogPos))
// 		})
// 	})
// })
//
// func processRowsEventHelper(rowEvent *r.RowsEvent, jobList []Job) {
// 	Expect(1).To(BeEquivalentTo(len(jobList)))
// 	Expect(jobList[0].JobMsg.Table).To(BeEquivalentTo(table.Name))
// 	Expect(jobList[0].JobMsg.Database).To(BeEquivalentTo(table.Schema))
// 	var expectPKColumns []string
// 	for _, column := range table.PrimaryKeyColumns {
// 		expectPKColumns = append(expectPKColumns, column.Name)
// 	}
// 	Expect(jobList[0].JobMsg.DmlMsg.PkColumns).To(BeEquivalentTo(expectPKColumns))
// 	Expect(jobList[0].JobMsg.DmlMsg.Pks["id"]).To(BeEquivalentTo(rowEvent.Rows[0][0]))
//
// }
//
// func createSimpleSchemaStore(dbName string) (*s.SimpleSchemaStore, error) {
// 	var sourceConfig = `
// host = "source-db"
// username = "root"
// password = ""
// port = 3306
// 	`
// 	mysql_test.MustSetupSourceDB(schemaStoreTestDB)
// 	mysqlConfig := utils.DBConfig{}
// 	_, err := toml.Decode(sourceConfig, &mysqlConfig)
// 	if err != nil {
// 		Fail("failed to get config: " + err.Error())
// 	}
// 	store, err := s.NewSimpleSchemaStore(&mysqlConfig)
// 	if err != nil {
// 		Fail("failed to NewSimpleSchemaStore: " + err.Error())
// 	}
// 	return store, err
// }
