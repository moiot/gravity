package tidb_kafka

import (
	"database/sql"
	"github.com/moiot/gravity/pkg/consts"
	"strings"
	"time"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/sarama_cluster"

	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/offsets"
	"github.com/moiot/gravity/pkg/utils"
)

var offsetStoreTable = "kafka_offsets"

var createDBStatement = fmt.Sprintf(`
	CREATE DATABASE IF NOT EXISTS %s
`, consts.GravityDBName)

var createOffsetStoreTableStatement = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
	consumer_group VARCHAR(50) NOT NULL DEFAULT '',
	topic VARCHAR(50) NOT NULL DEFAULT '',
	kafka_partition INT NOT NULL DEFAULT 0,
    offset BIGINT(20) DEFAULT NULL,
	ts TIMESTAMP,
	metadata VARCHAR(50) DEFAULT NULL,
	PRIMARY KEY(consumer_group, topic, kafka_partition)
)
`, consts.GravityDBName, offsetStoreTable)

type KafkaOffsetStoreFactory struct {
	config *config.SourceProbeCfg
}

func (f *KafkaOffsetStoreFactory) GenOffsetStore(c *sarama_cluster.Consumer) sarama_cluster.OffsetStore {
	// create db if needed
	db, err := utils.CreateDBConnection(f.config.SourceMySQL)
	if err != nil {
		log.Fatalf("failed to create db connection: %v", err)
	}
	_, err = db.Exec(createDBStatement)
	if err != nil {
		log.Fatalf("failed to create db: %v", err)
	}

	_, err = db.Exec("use " + consts.GravityDBName)
	if err != nil {
		log.Fatalf("failed to use %s: %v", consts.GravityDBName, err)
	}

	_, err = db.Exec(createOffsetStoreTableStatement)
	if err != nil {
		log.Fatalf("failed to create offset table: %v", err)
	}

	return &DBOffsetStore{db}
}

func NewKafkaOffsetStoreFactory(config *config.SourceProbeCfg) *KafkaOffsetStoreFactory {
	return &KafkaOffsetStoreFactory{
		config: config,
	}
}

type DBOffsetStore struct {
	db *sql.DB
}

func (s *DBOffsetStore) CommitOffset(req *offsets.OffsetCommitRequest) (*offsets.OffsetCommitResponse, error) {
	log.Debugf("Commit Offset: %#v", req)
	return offsets.SaveOffsetToDB(s.db, fmt.Sprintf("%s.%s", consts.GravityDBName, offsetStoreTable), req)

}

func (s *DBOffsetStore) FetchOffset(req *offsets.OffsetFetchRequest) (*offsets.OffsetFetchResponse, error) {
	qryTpl := "SELECT topic, kafka_partition, offset, metadata, ts from `%s`.`%s` where consumer_group=?"
	stmt := fmt.Sprintf(qryTpl, consts.GravityDBName, offsetStoreTable)

	rows, err := s.db.Query(stmt, req.ConsumerGroup)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	resp := &offsets.OffsetFetchResponse{}
	var lastUpdate time.Time
	for rows.Next() {
		var topic, metadata string
		var partition int32
		var offset int64
		var ts time.Time
		err := rows.Scan(&topic, &partition, &offset, &metadata, &ts)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.AddBlock(topic, partition, offset, metadata)
		if ts.After(lastUpdate) {
			lastUpdate = ts
		}
	}
	resp.LastUpdate = lastUpdate
	return resp, errors.Trace(rows.Err())
}

func (s *DBOffsetStore) Clear(group string, topic []string) {
	stmt := fmt.Sprintf("DELETE FROM %s.%s WHERE consumer_group = ? and topic in ('%s')", consts.GravityDBName, offsetStoreTable, strings.Join(topic, "', '"))
	_, err := s.db.Exec(stmt, group)
	if err != nil {
		log.Fatalf("[DBOffsetStore.Clear] group = %s, sql = %s, err: %s", group, stmt, errors.Trace(err))
	}
}

func (s *DBOffsetStore) Close() error {
	return nil // should not close with kafka consumer. output needs to commit offset
}
