package offsets

import (
	"fmt"
	"time"

	"database/sql"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func SaveOffsetToDB(db *sql.DB, fullTableName string, request *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	ts := time.Now()
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	for topic, pbs := range request.blocks {
		for partition, b := range pbs {
			stmt := fmt.Sprintf("REPLACE INTO %s (consumer_group, topic, kafka_partition, offset, ts, metadata) VALUES(?,?,?,?,?,?)", fullTableName)
			tx.Exec(stmt, request.ConsumerGroup, topic, partition, b.Offset, ts, b.Metadata)
		}
	}
	tx.Commit()

	resp := &OffsetCommitResponse{}
	for topic, pbs := range request.blocks {
		for partition := range pbs {
			resp.AddError(topic, partition, nil)
		}
	}
	return resp, nil
}

func FetchOffsetFromDB(db *sql.DB, request *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	godb, err := gorm.Open("mysql", db)
	if err != nil {
		log.Fatal(err)
	}
	offsets := make([]ConsumerOffset, 0)
	godb = godb.Where("consumer_group = ?", request.ConsumerGroup).Find(&offsets)
	if godb.Error != nil {
		return nil, errors.Trace(godb.Error)
	}

	resp := &OffsetFetchResponse{}
	for _, o := range offsets {
		resp.AddBlock(o.Topic, o.KafkaPartition, o.Offset, o.Metadata)
	}
	return resp, nil
}

type ConsumerOffset struct {
	ConsumerGroup  string    `gorm:"column:consumer_group;primary_key"`
	Topic          string    `gorm:"column:topic;primary_key"`
	KafkaPartition int32     `gorm:"column:kafka_partition;primary_key"`
	Offset         int64     `gorm:"column:offset"`
	Ts             time.Time `gorm:"column:ts"`
	Metadata       string    `gorm:"column:metadata"`
}

const OffsetTableName = "consumer_offset"

func (ConsumerOffset) TableName() string {
	return OffsetTableName
}
