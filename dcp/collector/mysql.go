package collector

import (
	"context"
	"crypto/md5"
	"fmt"

	"github.com/json-iterator/go"
	"github.com/moiot/gravity/dcp/barrier"
	"github.com/moiot/gravity/pkg/protocol/dcp"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Mysql struct {
	//configs
	tagInfo map[string]tagInfo

	//binlog
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	//states
	started  bool
	nextGtid string

	output chan *dcp.Message
}

func (c *Mysql) GetChan() chan *dcp.Message {
	return c.output
}

type tagInfo struct {
	tag           string
	primaryKeyIdx int
}

// NewMysqlCollector creates a new collector.
// There should be ONE collector per mysql instance
func NewMysqlCollector(config *MysqlConfig) Interface {
	cfg := replication.BinlogSyncerConfig{
		ServerID: config.Db.ServerId,
		Flavor:   "mysql",
		Host:     config.Db.Host,
		Port:     uint16(config.Db.Port),
		User:     config.Db.Username,
		Password: config.Db.Password,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	id := ""
	gtid, err := mysql.ParseMysqlGTIDSet(id)
	if err != nil {
		log.Panic("error parse nextGtid ", id, err)
	}
	streamer, err := syncer.StartSyncGTID(gtid)
	if err != nil {
		log.Panic("error start streamer ", err)
	}

	schemaTables := make(map[string]tagInfo)
	for _, tagConfig := range config.TagConfigs {
		for _, v := range tagConfig.Tables {
			schemaTables[schemaTbl2Key(v.Schema, v.Table)] = tagInfo{
				tag:           tagConfig.Tag,
				primaryKeyIdx: v.PrimaryKeyIdx,
			}
		}
	}

	return &Mysql{
		tagInfo:  schemaTables,
		syncer:   syncer,
		streamer: streamer,
		started:  false,
		output:   make(chan *dcp.Message, 100),
	}
}

func schemaTbl2Key(schema, table string) string {
	return schema + "." + table
}

func (c *Mysql) Start() {
	go c.mainLoop()
}

func (c *Mysql) Stop() {
	c.syncer.Close()
	close(c.output)
	log.Info("mysql collector stopped")
}

func (c *Mysql) mainLoop() {
	for {
		rawEvent, err := c.streamer.GetEvent(context.Background())
		if err != nil {
			if err == replication.ErrSyncClosed {
				log.Info("exit collector loop")
				return
			} else {
				log.Fatal("collector error", err)
			}
		}

		switch evt := rawEvent.Event.(type) {
		case *replication.RowsEvent:
			schemaName, tableName := string(evt.Table.Schema), string(evt.Table.Table)
			isBarrier := false
			if schemaName == barrier.DB_NAME && tableName == barrier.TABLE_NAME {
				isBarrier = true
				if !c.started {
					log.Info("receive first barrier, starting")
					c.started = true
				}
			}
			if !c.started {
				log.Debug("ignore event since the first barrier hasn't been received, gtid=", c.getNextGtid())
			}

			tagInfo, ok := c.tagInfo[schemaTbl2Key(schemaName, tableName)]
			if !isBarrier && !ok {
				log.Debug("ignore event schema=", schemaName, ", table=", tableName)
				continue
			}

			switch rawEvent.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				for _, row := range evt.Rows {
					if isBarrier {
						for _, v := range c.tagInfo {
							msg := &dcp.Message{
								Tag:       v.tag,
								Id:        c.getNextGtid(),
								Timestamp: uint64(rawEvent.Header.Timestamp),
							}
							msg.Body = &dcp.Message_Barrier{Barrier: uint64(row[barrier.OFFSET_COLUMN_SEQ].(int64))}
							c.output <- msg
						}
					} else {
						msg := &dcp.Message{
							Tag:       tagInfo.tag,
							Id:        c.getNextGtid(),
							Timestamp: uint64(rawEvent.Header.Timestamp),
						}
						s, err := json.MarshalToString(row)
						if err != nil {
							log.Error("error serialize row to json. row=", row, ", err=", err)
							continue
						}
						s = "INSERT " + s
						msg.Body = &dcp.Message_Payload{Payload: &dcp.Payload{
							Id:      fmt.Sprint(row[tagInfo.primaryKeyIdx]),
							Content: s,
						}}
						msg.Checksum = fmt.Sprintf("%x", md5.Sum([]byte(s)))
						c.output <- msg
					}
				}

			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				for rowIndex := 0; rowIndex < len(evt.Rows); rowIndex += 2 {
					changedData := evt.Rows[rowIndex+1]
					if isBarrier {
						for _, v := range c.tagInfo {
							msg := &dcp.Message{
								Tag:       v.tag,
								Id:        c.getNextGtid(),
								Timestamp: uint64(rawEvent.Header.Timestamp),
							}
							msg.Body = &dcp.Message_Barrier{Barrier: uint64(changedData[barrier.OFFSET_COLUMN_SEQ].(int64))}
							c.output <- msg
						}
					} else {
						msg := &dcp.Message{
							Tag:       tagInfo.tag,
							Id:        c.getNextGtid(),
							Timestamp: uint64(rawEvent.Header.Timestamp),
						}
						s, err := json.MarshalToString(changedData)
						if err != nil {
							log.Error("error serialize row to json. row=", changedData, ", err=", err)
							continue
						}
						s = "UPDATE " + s
						msg.Body = &dcp.Message_Payload{Payload: &dcp.Payload{
							Id:      fmt.Sprint(changedData[tagInfo.primaryKeyIdx]), //actually id shouldn't change
							Content: s,
						}}
						msg.Checksum = fmt.Sprintf("%x", md5.Sum([]byte(s)))
						c.output <- msg
					}
				}

			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				for _, row := range evt.Rows {
					msg := &dcp.Message{
						Tag:       tagInfo.tag,
						Id:        c.getNextGtid(),
						Timestamp: uint64(rawEvent.Header.Timestamp),
					}

					s, err := json.MarshalToString(row)
					if err != nil {
						log.Error("error serialize row to json. row=", row, ", err=", err)
						continue
					}
					s = "DELETE " + s
					msg.Body = &dcp.Message_Payload{Payload: &dcp.Payload{
						Id:      fmt.Sprint(row[tagInfo.primaryKeyIdx]), //actually id shouldn't change
						Content: s,
					}}
					msg.Checksum = fmt.Sprintf("%x", md5.Sum([]byte(s)))

					c.output <- msg
				}

			}

		case *replication.XIDEvent:
			//TODO save gtid
		case *replication.GTIDEvent:
			// generate GTID set
			// only support mysql nextGtid, not for mariadb
			u, err := uuid.FromBytes(evt.SID)
			if err != nil {
				log.Error(err)
				continue
			}
			gtid := fmt.Sprintf("%s:%d", u.String(), evt.GNO)
			c.setNextGtid(gtid)
		default:
			log.Debug("ignore event not CURD & GTID. ", replication.EventType(rawEvent.Header.EventType))
		}
	}
}

func (c *Mysql) setNextGtid(gtid string) {
	c.nextGtid = gtid
}

func (c *Mysql) getNextGtid() string {
	return c.nextGtid
}
