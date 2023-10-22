package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/inputs"
	"github.com/moiot/gravity/pkg/outputs/mysql"
	"github.com/moiot/gravity/pkg/sql_execution_engine"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mysql_test"
)

func TestBidirection(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDBConfig := config.DBConfig{
		Host:     "tidb",
		Username: "root",
		Port:     4000,
	}
	targetDBConfig := config.DBConfig{
		Host:     "tidb",
		Username: "root",
		Port:     4000,
	}

	sourceDB, err := utils.CreateDBConnection(&sourceDBConfig)
	r.NoError(err)
	defer sourceDB.Close()

	r.NoError(mysql_test.SetupTestDB(sourceDB, sourceDBName))

	err = utils.InitInternalTxnTags(sourceDB)
	r.NoError(err)

	targetDB, err := utils.CreateDBConnection(&targetDBConfig)
	r.NoError(err)
	defer targetDB.Close()

	r.NoError(mysql_test.SetupTestDB(targetDB, targetDBName))

	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.TiDB,
			Mode: config.Stream,
			Config: utils.MustAny2Map(config.SourceTiDBConfig{
				SourceDB: &sourceDBConfig,
				SourceKafka: &config.SourceKafkaConfig{
					BrokerConfig: config.KafkaGlobalConfig{
						BrokerAddrs: []string{"kafka:9092"},
					},
					Topics:      []string{"obinlog"},
					ConsumeFrom: "oldest",
					GroupID:     t.Name(),
				},
				PositionRepo: &config.GenericPluginConfig{
					Type: "mysql-repo",
					Config: map[string]interface{}{
						"source": utils.MustAny2Map(sourceDBConfig),
					},
				},
				IgnoreBiDirectionalData: true,
			}),
		},
		OutputPlugin: config.GenericPluginConfig{
			Type: "mysql",
			Config: utils.MustAny2Map(mysql.MySQLPluginConfig{
				DBConfig:  &targetDBConfig,
				EnableDDL: true,
				Routes: []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
				EngineConfig: &config.GenericPluginConfig{
					Type: sql_execution_engine.MySQLReplaceEngine,
					Config: map[string]interface{}{
						"tag-internal-txn": true,
					},
				},
			}),
		},
	}
	// start the server
	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	_, err = sourceDB.Exec(fmt.Sprintf("create table `%s`.t(id int(11), primary key(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ", sourceDBName))
	r.NoError(err)

	tx, err := sourceDB.Begin()
	r.NoError(err)
	_, err = tx.Exec(utils.GenerateTxnTagSQL(sourceDBName))
	r.NoError(err)
	_, err = tx.Exec(fmt.Sprintf("insert into `%s`.t(id) values (1)", sourceDBName))
	r.NoError(err)
	err = tx.Commit()
	r.NoError(err)

	_, err = sourceDB.Exec(fmt.Sprintf("insert into `%s`.t(id) values (2)", sourceDBName))
	r.NoError(err)

	err = mysql_test.SendDeadSignal(sourceDB, pipelineConfig.PipelineName)
	r.NoError(err)

	server.Input.Wait()
	server.Close()

	success := false

	for retry := 0; retry < 30; retry++ {
		rows, err := targetDB.Query(fmt.Sprintf("select id from `%s`.t", targetDBName))
		r.NoError(err)

		ids := make([]int, 0, 1)
		for rows.Next() {
			var id int
			err = rows.Scan(&id)
			r.NoError(err)
			ids = append(ids, id)
		}
		r.NoError(rows.Err())
		_ = rows.Close()

		if len(ids) == 1 && ids[0] == 2 {
			success = true
			break
		} else {
			fmt.Printf("wait for syncing %d time(s)...\n", retry)
			time.Sleep(time.Second)
		}
	}

	r.True(success)
}
