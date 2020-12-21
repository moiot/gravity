package tidb_kafka

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/mitchellh/hashstructure"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/pingcap/parser"
	log "github.com/sirupsen/logrus"

	gCfg "github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/inputs/helper/binlog_checker"
	"github.com/moiot/gravity/pkg/kafka"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/position_cache"
	"github.com/moiot/gravity/pkg/position_repos"
	pb "github.com/moiot/gravity/pkg/protocol/tidb"
	"github.com/moiot/gravity/pkg/sarama_cluster"
	"github.com/moiot/gravity/pkg/utils"
)

type BinlogTailer struct {
	gravityServerID uint32

	name           string
	sourceTimeZone string
	consumer       *sarama_cluster.Consumer
	config         *gCfg.SourceTiDBConfig
	emitter        core.Emitter
	router         core.Router
	binlogChecker  binlog_checker.BinlogChecker
	mapLock        sync.Mutex
	parser         *parser.Parser

	wg sync.WaitGroup
}

func (t *BinlogTailer) Start() error {
	log.Info("Consumer subscribes ", t.consumer.Subscriptions())

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		for msg := range t.consumer.Messages() {
			log.Debugf("[tidb_binlog_tailer]: topic: %v, partition: %v, offset: %v", msg.Topic, msg.Partition, msg.Offset)
			binlog := pb.Binlog{}
			if err := binlog.Unmarshal(msg.Value); err != nil {
				log.Fatalf("[binlog_tailer] failed to parse tidb binlog msg: %v", errors.ErrorStack(err))
			}
			jobs, err := t.createMsgs(binlog, msg)
			if err != nil {
				log.Fatalf("[tidb_binlog_tailer] failed to convert tidb binlog to gravity jobs. offset: %v.%v.%v, err: %v", msg.Topic, msg.Partition, msg.Offset, err)
			}
			t.dispatchMsg(jobs)
		}
	}()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		for err := range t.consumer.Errors() {
			log.Fatalf("[tidb_binlog_tailer] received error: %s", err)
		}
	}()

	return nil
}

func (t *BinlogTailer) Wait() {
	t.wg.Wait()
}

func (t *BinlogTailer) Close() {
	t.consumer.Close()
	t.Wait()
}

func buildPKValueMap(columnInfos []*pb.ColumnInfo, row *pb.Row) map[string]interface{} {
	pkValues := make(map[string]interface{})
	for i, columnInfo := range columnInfos {
		if columnInfo.IsPrimaryKey {
			pkValues[columnInfo.Name] = deserialize(row.Columns[i], columnInfo.MysqlType)
		}
	}
	return pkValues
}

func (t *BinlogTailer) createMsgs(binlog pb.Binlog, kMsg *sarama.ConsumerMessage) ([]*core.Msg, error) {
	var msgList []*core.Msg

	if binlog.Type == pb.BinlogType_DDL {
		return msgList, t.handleDDL(binlog, kMsg)
	}

	processTime := time.Now()
	eventTime := time.Unix(int64(ParseTimeStamp(uint64(binlog.CommitTs))), 0)

	for _, table := range binlog.DmlData.Tables {
		schemaName := *table.SchemaName
		tableName := *table.TableName
		for _, mutation := range table.Mutations {
			msg := core.Msg{
				Phase: core.Phase{
					Start: processTime,
				},
				Type:      core.MsgDML,
				Database:  schemaName,
				Table:     tableName,
				Timestamp: eventTime,
				Done:      make(chan struct{}),
			}

			if binlog_checker.IsBinlogCheckerMsg(schemaName, tableName) {
				msg.Type = core.MsgCtl

				if *mutation.Type == pb.MutationType_Update {
					row := *mutation.ChangeRow
					checkerRow, err := binlog_checker.ParseTiDBRow(row)
					if err != nil {
						return msgList, errors.Trace(err)
					}
					if !t.binlogChecker.IsEventBelongsToMySelf(checkerRow) {
						log.Debugf("skip other binlog checker row. row: %v", row)
						continue
					}
					t.binlogChecker.MarkActive(checkerRow)
				}
			}

			// skip binlog position event
			if position_repos.IsPositionStoreEvent(schemaName, tableName) {
				log.Debugf("[binlogTailer] skip position event")
				continue
			}

			// do not send messages without router to the system
			if !consts.IsInternalDBTraffic(schemaName) &&
				t.router != nil && !t.router.Exists(&core.Msg{
				Database: schemaName,
				Table:    tableName,
			}) {
				continue
			}

			dmlMsg := &core.DMLMsg{}
			data := make(map[string]interface{})
			colInfoList := table.ColumnInfo
			switch *mutation.Type {
			case pb.MutationType_Insert:
				dmlMsg.Operation = core.Insert
				for index, value := range mutation.Row.Columns {
					data[colInfoList[index].Name] = deserialize(value, colInfoList[index].MysqlType)
				}
			case pb.MutationType_Update:
				dmlMsg.Operation = core.Update
				old := make(map[string]interface{})
				for index, value := range mutation.Row.Columns {
					data[colInfoList[index].Name] = deserialize(value, colInfoList[index].MysqlType)
				}
				for index, value := range mutation.ChangeRow.Columns {
					old[colInfoList[index].Name] = deserialize(value, colInfoList[index].MysqlType)
				}
				dmlMsg.Old = old
			case pb.MutationType_Delete:
				dmlMsg.Operation = core.Delete
				for index, value := range mutation.Row.Columns {
					data[colInfoList[index].Name] = deserialize(value, colInfoList[index].MysqlType)
				}
			default:
				log.Fatalf("unexpected MutationType: %v", *mutation.Type)
				continue
			}
			metrics.InputCounter.WithLabelValues(t.name, msg.Database, msg.Table, string(msg.Type), string(dmlMsg.Operation)).Add(1)

			if mysql_test.IsDeadSignal(schemaName, tableName) && data["id"].(string) == t.name {
				t.consumer.Close()
				return msgList, nil
			}

			dmlMsg.Data = data
			dmlMsg.Pks = buildPKValueMap(table.ColumnInfo, mutation.Row)
			msg.DmlMsg = dmlMsg
			msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
			msg.OutputDepHashes = calculateOutputDep(table.UniqueKeys, msg)
			msgList = append(msgList, &msg)
		}
	}

	if len(msgList) > 0 {
		lastMsg := msgList[len(msgList)-1]
		lastMsg.InputContext = kMsg
		lastMsg.AfterCommitCallback = t.AfterMsgCommit
	}
	return msgList, nil
}

func (t *BinlogTailer) handleDDL(binlog pb.Binlog, kMsg *sarama.ConsumerMessage) error {
	metrics.InputCounter.WithLabelValues(t.name, "", "", string(core.MsgDDL), "").Add(1)
	processTime := time.Now()
	eventTime := time.Unix(int64(ParseTimeStamp(uint64(binlog.CommitTs))), 0)
	ddlStmt := string(binlog.DdlData.DdlQuery)

	if t.config.IgnoreBiDirectionalData && strings.Contains(ddlStmt, consts.DDLTag) {
		log.Info("ignore internal ddl: ", ddlStmt)
		return nil
	}

	dbNames, tables, asts := parseDDL(t.parser, binlog)

	// emit barrier msg
	barrierMsg := NewBarrierMsg()
	if err := t.emitter.Emit(barrierMsg); err != nil {
		log.Fatalf("failed to emit barrier msg: %v", errors.ErrorStack(err))
	}
	<-barrierMsg.Done

	sent := 0

	for i := range dbNames {
		dbName := dbNames[i]
		table := tables[i]
		ast := asts[i]

		if dbName == consts.MySQLInternalDBName {
			continue
		}

		if dbName == consts.GravityDBName || dbName == consts.OldDrcDBName {
			continue
		}

		log.Infof("QueryEvent: database: %s, sql: %s", dbName, ddlStmt)

		// emit ddl msg
		ddlMsg := &core.Msg{
			Phase: core.Phase{
				Start: processTime,
			},
			Type:                core.MsgDDL,
			Timestamp:           eventTime,
			Database:            dbName,
			Table:               table,
			DdlMsg:              &core.DDLMsg{Statement: ddlStmt, AST: ast},
			Done:                make(chan struct{}),
			InputStreamKey:      utils.NewStringPtr(inputStreamKey),
			InputContext:        kMsg,
			AfterCommitCallback: t.AfterMsgCommit,
		}

		// do not send messages without router to the system
		if consts.IsInternalDBTraffic(dbName) || (t.router != nil && !t.router.Exists(ddlMsg)) {
			continue
		}

		if err := t.emitter.Emit(ddlMsg); err != nil {
			log.Fatalf("failed to emit ddl msg: %v", errors.ErrorStack(err))
		}
		sent++
	}

	if sent > 0 {
		// emit barrier msg
		barrierMsg = NewBarrierMsg()
		if err := t.emitter.Emit(barrierMsg); err != nil {
			log.Fatalf("failed to emit barrier msg: %v", errors.ErrorStack(err))
		}
		<-barrierMsg.Done
		log.Infof("[binlogTailer] ddl done with commit ts: %d, offset: %d, stmt: %s", binlog.CommitTs, kMsg.Offset, ddlStmt)
	}

	return nil
}

var hasher = xxhash.New64()
var hashOptions = hashstructure.HashOptions{
	Hasher: hasher,
}

func calculateOutputDep(uniqueKeys []*pb.Key, msg core.Msg) (hashes []core.OutputHash) {
	for _, uk := range uniqueKeys {
		var isUKUpdate bool
		isUKUpdate = ukUpdated(uk.ColumnNames, msg.DmlMsg.Data, msg.DmlMsg.Old)

		// add hash based on new data
		keyName, h := dataHash(msg.Database, msg.Table, uk.GetName(), uk.ColumnNames, msg.DmlMsg.Data)
		if keyName != "" {
			hashes = append(hashes, core.OutputHash{Name: keyName, H: h})
		}

		// add hash if unique key changed
		if isUKUpdate {
			keyName, h := dataHash(msg.Database, msg.Table, uk.GetName(), uk.ColumnNames, msg.DmlMsg.Old)
			if keyName != "" {
				hashes = append(hashes, core.OutputHash{Name: keyName, H: h})
			}
		}
	}

	return
}

func dataHash(schema string, table string, idxName string, idxColumns []string, data map[string]interface{}) (string, uint64) {
	key := []interface{}{schema, table, idxName}
	var nonNull bool
	for _, columnName := range idxColumns {
		if data[columnName] != nil {
			key = append(key, columnName, data[columnName])
			nonNull = true
		}
	}
	if !nonNull {
		return "", 0
	}

	h, err := hashstructure.Hash(key, &hashOptions)
	if err != nil {
		log.Fatalf("error hash: %v, uk: %v", err, idxName)
	}
	return fmt.Sprint(key), h
}

func ukUpdated(ukColumns []string, newData map[string]interface{}, oldData map[string]interface{}) bool {
	for _, column := range ukColumns {
		// if oldData[column] == nil, we consider this is a insert
		if oldData[column] != nil && !reflect.DeepEqual(newData[column], oldData[column]) {
			return true
		}
	}
	return false
}

func (t *BinlogTailer) AfterMsgCommit(msg *core.Msg) error {
	kMsg, ok := msg.InputContext.(*sarama.ConsumerMessage)
	if !ok {
		return errors.Errorf("invalid input context")
	}

	t.consumer.MarkPartitionOffset(kMsg.Topic, kMsg.Partition, kMsg.Offset, "")
	return nil
}

func deserialize(raw *pb.Column, colType string) interface{} {
	if raw == nil {
		return nil
	}
	switch colType {
	case "date", "datetime", "time", "year", "timestamp":
		return raw.GetStringValue()
	case "int", "integer", "tinyint", "smallint", "mediumint", "bigint":
		return raw.GetInt64Value()
	case "int unsigned", "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "bigint unsigned":
		return raw.GetUint64Value()
	case "float", "double":
		return raw.GetDoubleValue()
	case "decimal":
		return raw.GetStringValue()
	case "bit":
		return raw.GetBytesValue()
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return raw.GetStringValue()
	case "tinyblob", "blob", "mediumblob", "longblob", "binary":
		return raw.GetBytesValue()
	case "enum", "set":
		return raw.GetUint64Value()
	case "json":
		return raw.GetBytesValue()
	default:
		log.Fatalf("un-recognized mysql type: %v", raw)
		return raw
	}
}

func (t *BinlogTailer) dispatchMsg(msgs []*core.Msg) {
	// ignore internal txn data
	hasInternalTxnTag := false
	for _, msg := range msgs {
		if utils.IsCircularTrafficTag(msg.Database, msg.Table) {
			hasInternalTxnTag = true
			log.Debugf("[binlogTailer] internal traffic found")
			break
		}
	}

	if hasInternalTxnTag && t.config.IgnoreBiDirectionalData {
		last := msgs[len(msgs)-1]
		msgs = []*core.Msg{
			{
				Phase:               last.Phase,
				Type:                core.MsgCtl,
				Timestamp:           last.Timestamp,
				Done:                last.Done,
				InputContext:        last.InputContext,
				InputStreamKey:      last.InputStreamKey,
				AfterCommitCallback: last.AfterAckCallback,
			},
		}
	} else {
		log.Debugf("[binlogTailer] do not ignore traffic: hasInternalTxnTag %v, cfg.Ignore %v, msgTxnBufferLen: %v", hasInternalTxnTag, t.config.IgnoreBiDirectionalData, len(msgs))
	}

	for i, m := range msgs {
		if binlog_checker.IsBinlogCheckerMsg(m.Database, m.Table) || m.Database == consts.GravityDBName {
			m.Type = core.MsgCtl
		}

		// check circular traffic again before emitter emit the message
		if pipelineName, circular := core.MatchTxnTagPipelineName(t.config.FailOnTxnTags, m); circular {
			log.Fatalf("[binlog_tailer] detected internal circular traffic, txn tag: %v", pipelineName)
		}

		if err := t.emitter.Emit(m); err != nil {
			log.Fatalf("failed to emit, idx: %d, schema: %v, table: %v, msgType: %v, err: %v", i, m.Database, m.Table, m.Type, errors.ErrorStack(err))
		}
	}
}

func NewBinlogTailer(
	pipelineName string,
	serverID uint32,
	positionCache position_cache.PositionCacheInterface,
	config *gCfg.SourceTiDBConfig,
	emitter core.Emitter,
	router core.Router,
	binlogChecker binlog_checker.BinlogChecker,
) (*BinlogTailer, error) {

	srcKafkaCfg := config.SourceKafka

	offsetFactory := NewKafkaOffsetStoreFactory(pipelineName, positionCache)
	kafkaConfig := sarama_cluster.NewConfig()
	kafkaConfig.Version = kafka.MsgVersion
	// if no previous offset committed, use the oldest offset
	consumeFrom := srcKafkaCfg.ConsumeFrom
	if consumeFrom == "newest" {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else if consumeFrom == "oldest" {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		return nil, errors.Errorf("invalid kafka consume from: %v", consumeFrom)
	}
	kafkaGlobalConfig := srcKafkaCfg.BrokerConfig

	if kafkaGlobalConfig.Net != nil {
		kafkaConfig.Net.SASL.Enable = kafkaGlobalConfig.Net.SASL.Enable
		kafkaConfig.Net.SASL.User = kafkaGlobalConfig.Net.SASL.User
		kafkaConfig.Net.SASL.Password = kafkaGlobalConfig.Net.SASL.Password
	}

	//
	// common settings
	//
	if srcKafkaCfg.Common.ClientID != "" {
		kafkaConfig.ClientID = srcKafkaCfg.Common.ClientID
	} else {
		kafkaConfig.ClientID = "_gravity"
	}
	if srcKafkaCfg.Common.ChannelBufferSize > 0 {
		kafkaConfig.ChannelBufferSize = srcKafkaCfg.Common.ChannelBufferSize
	}

	//
	// consumer related performance tuning
	//
	if srcKafkaCfg.Consumer != nil {
		d, err := time.ParseDuration(srcKafkaCfg.Consumer.Offsets.CommitInterval)
		if err != nil {
			return nil, errors.Errorf("invalid commit interval: %v", srcKafkaCfg.Consumer.Offsets.CommitInterval)
		}
		kafkaConfig.Consumer.Offsets.CommitInterval = d

		if srcKafkaCfg.Consumer.Fetch.Default != 0 {
			kafkaConfig.Consumer.Fetch.Default = srcKafkaCfg.Consumer.Fetch.Default
		}

		if srcKafkaCfg.Consumer.Fetch.Max != 0 {
			kafkaConfig.Consumer.Fetch.Max = srcKafkaCfg.Consumer.Fetch.Max
		}

		if srcKafkaCfg.Consumer.Fetch.Min != 0 {
			kafkaConfig.Consumer.Fetch.Min = srcKafkaCfg.Consumer.Fetch.Min
		}

		maxWaitDuration, err := time.ParseDuration(srcKafkaCfg.Consumer.MaxWaitTime)
		if err != nil {
			return nil, errors.Errorf("invalid max wait time")
		}

		kafkaConfig.Consumer.MaxWaitTime = maxWaitDuration
	}
	kafkaConfig.Consumer.Return.Errors = true

	if err := kafkaConfig.Validate(); err != nil {
		log.Fatal(err)
	}

	log.Infof("[tidb_binlog_tailer] consumer config: sarama config: %#v", kafkaConfig)

	consumer, err := sarama_cluster.NewConsumer(
		srcKafkaCfg.BrokerConfig.BrokerAddrs,
		srcKafkaCfg.GroupID,
		srcKafkaCfg.Topics,
		kafkaConfig,
		offsetFactory,
	)
	if err != nil {
		log.Error(err)
		return nil, errors.Trace(err)
	}

	tailer := &BinlogTailer{
		name:            pipelineName,
		gravityServerID: serverID,
		consumer:        consumer,
		config:          config,
		emitter:         emitter,
		router:          router,
		binlogChecker:   binlogChecker,
		parser:          parser.New(),
	}
	return tailer, nil
}
