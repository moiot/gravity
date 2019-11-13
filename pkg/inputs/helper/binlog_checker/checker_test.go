package binlog_checker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/replication"
	"github.com/stretchr/testify/assert"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/utils"
)

func TestBinlogChecker(t *testing.T) {
	assert := assert.New(t)

	dbConfig := mysql_test.SourceDBConfig()
	dbConfig.Location = "Local"
	db, err := utils.CreateDBConnection(dbConfig)
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

	pipelineConfig := config.PipelineConfig{
		PipelineName: "test_binlog_checker",
		MySQLConfig:  &config.MySQLConfig{Source: dbConfig},
	}

	binlogChecker, err := NewBinlogChecker(
		pipelineConfig.PipelineName,
		dbConfig,
		"",
		100*time.Millisecond,
		false,
	)
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

	if err := binlogChecker.Start(); err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

	syncer := utils.NewBinlogSyncer(1234, dbConfig, 0)

	dbUtil := utils.NewMySQLDB(db)
	_, gtid, err := dbUtil.GetMasterStatus()
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}
	streamer, err := syncer.StartSyncGTID(&gtid)
	if err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}

	var nrHeartbeat int
	var nrMyHeratbeat int

	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			e, err := streamer.GetEvent(context.Background())
			if err != nil {
				assert.FailNow(err.Error())
			}

			switch ev := e.Event.(type) {
			case *replication.RowsEvent:
				schemaName, tableName := string(ev.Table.Schema), string(ev.Table.Table)

				if IsBinlogCheckerMsg(schemaName, tableName) {

					nrHeartbeat++

					row, err := ParseMySQLRowEvent(ev)
					if err == nil {
						if binlogChecker.IsEventBelongsToMySelf(row) {
							nrMyHeratbeat++
							binlogChecker.MarkActive(row)
						}
					}
				}

			}
		}
	}()

	// we wait for 15 seconds
	<-time.After(15 * time.Second)
	close(stop)

	// we should at least receive some heartbeat
	assert.True(nrHeartbeat > 10)
	assert.True(nrMyHeratbeat > 10)

	var nrHeartbeatRec int
	r1 := db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE name = ?", binlogCheckerFullTableNameV2), pipelineConfig.PipelineName)
	if err := r1.Scan(&nrHeartbeatRec); err != nil {
		assert.FailNow(err.Error())
	}

	assert.Equal(1, nrHeartbeatRec)

	r2 := db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE name = ?", binlogCheckerFullTableNameV2), pipelineConfig.PipelineName)
	var name string
	var offset int
	var t1 time.Time
	var t2 time.Time
	if err := r2.Scan(&name, &offset, &t1, &t2); err != nil {
		assert.FailNow(err.Error())
	}
	assert.True(offset > 5)

	// start it multiple times should not add records to db
	if err := binlogChecker.Start(); err != nil {
		assert.FailNow(errors.ErrorStack(err))
	}
	r3 := db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE name = ?", binlogCheckerFullTableNameV2), pipelineConfig.PipelineName)
	if err := r3.Scan(&nrHeartbeatRec); err != nil {
		assert.FailNow(err.Error())
	}
	assert.Equal(1, nrHeartbeatRec)

	binlogChecker.Stop()
}
