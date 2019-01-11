package tidb_kafka

import (
	"time"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/gravity/binlog_checker"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/schema_store"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"sync"

	gCfg "github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/kafka"
	pb "github.com/moiot/gravity/pkg/protocol/tidb"
	"github.com/moiot/gravity/pkg/sarama_cluster"
)

type BinlogTailer struct {
	gravityServerID uint32

	sourceTimeZone string
	consumer       *sarama_cluster.Consumer
	config         *gCfg.SourceTiDBConfig
	emitter        core.Emitter
	binlogChecker  binlog_checker.BinlogChecker
	mapLock        sync.Mutex

	wg sync.WaitGroup
}

func (t *BinlogTailer) Start() error {
	log.Info("Consumer subscribes ", t.consumer.Subscriptions())
	log.Info("Begin to consume!")

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

	ListenPartitionLoop:
		for {
			select {
			case partitionConsumer, ok := <-t.consumer.Partitions():
				if !ok {
					log.Info("cannot fetch partitionConsumers, the channel may be closed")
					break ListenPartitionLoop
				}

				log.Infof("[mq_consumer] partition consumer topic: %v, partition: %v", partitionConsumer.Topic(), partitionConsumer.Partition())

				t.wg.Add(1)
				go func(partitionConsumer sarama_cluster.PartitionConsumer) {
					defer t.wg.Done()

					for msg := range partitionConsumer.Messages() {
						log.Debugf("[tidb_binlog_tailer]: topic: %v, partition: %v, offset: %v", msg.Topic, msg.Partition, msg.Offset)
						binlog := pb.Binlog{}
						if err := binlog.Unmarshal(msg.Value); err != nil {
							log.Fatalf("[binlog_tailer] failed to parse tidb binlog msg: %v", errors.ErrorStack(err))
						}
						jobs, err := t.createMsgs(binlog, msg)
						if err != nil {
							log.Fatalf("[tidb_binlog_tailer] failed to convert tidb binlog to gravity jobs. offset: %v.%v.%v, err: %v", msg.Topic, msg.Partition, msg.Offset, err)
						}
						for _, job := range jobs {
							if err := t.dispatchMsg(job); err != nil {
								log.Fatalf("[tidb_binlog_tailer] failed to dispatch job. offset: %v.%v.%v. err: %v", msg.Topic, msg.Partition, msg.Offset, err)
							}
						}
					}
				}(partitionConsumer)
			}
		}
		log.Info("Get out of ListenPartitionLoop")
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

func buildPKColumnList(colInfoList []*pb.ColumnInfo) []*pb.ColumnInfo {
	var pkCols []*pb.ColumnInfo
	for _, colInfo := range colInfoList {
		if colInfo.IsPrimaryKey {
			pkCols = append(pkCols, colInfo)
		}
	}
	return pkCols
}

func buildPKNameList(pkColList []*pb.ColumnInfo) []string {
	pkNames := make([]string, len(pkColList))
	for i, colInfo := range pkColList {
		pkNames[i] = colInfo.Name
	}
	return pkNames
}

func buildPKValueMap(pkColList []*pb.ColumnInfo, row *pb.Row) map[string]interface{} {
	pkValues := make(map[string]interface{})
	for i, colInfo := range pkColList {
		pkValues[colInfo.Name] = deserialize(row.Columns[i], colInfo.MysqlType)
	}
	return pkValues
}

func (t *BinlogTailer) createMsgs(
	binlog pb.Binlog,
	kMsg *sarama.ConsumerMessage,
) ([]*core.Msg, error) {
	var msgList []*core.Msg
	if binlog.Type == pb.BinlogType_DDL {
		log.Infof("skip ddl %s", string(binlog.DdlData.DdlQuery))
		return msgList, nil
	}
	for _, table := range binlog.DmlData.Tables {
		schemaName := *table.SchemaName
		tableName := *table.TableName
		pkColumnList := buildPKColumnList(table.ColumnInfo)
		for _, mutation := range table.Mutations {
			msg := core.Msg{
				Type:      core.MsgDML,
				Database:  schemaName,
				Table:     tableName,
				Timestamp: time.Unix(int64(ParseTimeStamp(uint64(binlog.CommitTs))), 0),
				Done:      make(chan struct{}),
			}

			if binlog_checker.IsBinlogCheckerMsg(schemaName, tableName) {
				msg.Type = core.MsgCtl
			}

			if binlog_checker.IsBinlogCheckerMsg(schemaName, tableName) && *mutation.Type == pb.MutationType_Update {
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
				log.Warnf("unexpected MutationType: %v", *mutation.Type)
				continue
			}

			if mysql_test.IsDeadSignal(schemaName, tableName) && data["id"] == t.gravityServerID {
				t.consumer.Close()
				return msgList, nil
			}

			dmlMsg.Data = data
			dmlMsg.Pks = buildPKValueMap(pkColumnList, mutation.Row)
			msg.DmlMsg = dmlMsg
			msgList = append(msgList, &msg)
		}
	}
	return msgList, nil
}

func buildTableDef(table *pb.Table) *schema_store.Table {
	ret := &schema_store.Table{
		Schema: *table.SchemaName,
		Name:   *table.TableName,
	}

	for i, c := range table.ColumnInfo {
		ret.Columns = append(ret.Columns, schema_store.Column{
			Idx:          i,
			Name:         c.Name,
			ColType:      c.MysqlType,
			IsPrimaryKey: c.IsPrimaryKey,
		})

		if c.IsPrimaryKey {
			ret.PrimaryKeyColumns = append(ret.PrimaryKeyColumns, ret.Columns[i])
		}
	}

	return ret
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
		log.Warnf("un-recognized mysql type: %v", raw)
		return raw
	}
}

func (t *BinlogTailer) dispatchMsg(msg *core.Msg) error {
	msg.InputStreamKey = utils.NewStringPtr("tidbbinlog")
	msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())

	return errors.Trace(t.emitter.Emit(msg))
}

func NewBinlogTailer(
	serverID uint32,
	config *gCfg.SourceTiDBConfig,
	emitter core.Emitter,
	binlogChecker binlog_checker.BinlogChecker,
) (*BinlogTailer, error) {

	srcKafkaCfg := config.SourceKafka
	osf := NewKafkaOffsetStoreFactory(config.OffsetStoreConfig)
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

	kafkaConfig.Group.Mode = sarama_cluster.ConsumerModePartitions

	//
	// common settings
	//
	kafkaConfig.ClientID = srcKafkaCfg.Common.ClientID
	kafkaConfig.ChannelBufferSize = srcKafkaCfg.Common.ChannelBufferSize

	//
	// consumer related performance tuning
	//
	if srcKafkaCfg.Consumer == nil {
		return nil, errors.Errorf("empty consumer config")
	}

	d, err := time.ParseDuration(srcKafkaCfg.Consumer.Offsets.CommitInterval)
	if err != nil {
		return nil, errors.Errorf("invalid commit interval")
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

	log.Infof("[tidb_binlog_tailer] consumer config: sarama config: %v, pipeline config: %v", kafkaConfig, srcKafkaCfg)

	consumer, err := sarama_cluster.NewConsumer(
		srcKafkaCfg.BrokerConfig.BrokerAddrs,
		srcKafkaCfg.GroupID,
		srcKafkaCfg.Topics,
		kafkaConfig,
		osf,
	)
	if err != nil {
		log.Error(err)
		return nil, errors.Trace(err)
	}

	tailer := &BinlogTailer{
		gravityServerID: serverID,
		consumer:        consumer,
		config:          config,
		emitter:         emitter,
		binlogChecker:   binlogChecker,
	}
	return tailer, nil
}
