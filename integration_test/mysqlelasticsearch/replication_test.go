package mysqlelasticsearch_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/olivere/elastic"

	"github.com/moiot/gravity/pkg/inputs/mysqlbatch"

	"github.com/moiot/gravity/pkg/elasticsearch_test"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/outputs"
	"github.com/moiot/gravity/pkg/outputs/elasticsearch"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestMySQL2ElasticsearchReplication(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	// 127.0.0.1       source-db in /etc/hosts
	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()

	es, err := elasticsearch_test.CreateTestClient()
	r.NoError(err)
	defer es.Stop()

	sourceDBConfig := mysql_test.SourceDBConfig()

	batchOpCnt := 100

	for i := 0; i < batchOpCnt; i++ {
		r.NoError(createDataToSource(sourceDB, sourceDBName, mysql_test.TestTableName, i))
	}

	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.Mysql,
			Mode: config.Replication,
			Config: utils.Struct2Map(
				mysqlbatch.PluginConfig{
					Source: sourceDBConfig,
				},
			),
		},
		OutputPlugin: config.GenericConfig{
			Type: outputs.Elasticsearch,
			Config: utils.Struct2Map(elasticsearch.ElasticsearchPluginConfig{
				ServerConfig: &elasticsearch.ElasticsearchServerConfig{
					URLs:  elasticsearch_test.TestURLs(),
					Sniff: false,
				},
				Routes: []map[string]interface{}{
					{
						"match-schema": sourceDBName,
						"match-table":  mysql_test.TestTableName,
						"target-index": mysql_test.TestTableName,
					},
				},
			}),
		},
	}

	// start the server
	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())

	waitUtilBatchFinished(server)

	cnt, err := getDocCount(es, mysql_test.TestTableName)
	r.NoError(err)

	r.Equal(cnt, batchOpCnt)

	streamOpCnt := 200
	for i := batchOpCnt; i < batchOpCnt+streamOpCnt; i++ {
		r.NoError(createDataToSource(sourceDB, sourceDBName, mysql_test.TestTableName, i))
	}
	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	cnt, err = getDocCount(es, mysql_test.TestTableName)
	r.NoError(err)
	r.Equal(cnt, batchOpCnt+streamOpCnt)
}

func waitUtilBatchFinished(server *app.Server) {
	for {
		time.Sleep(100 * time.Millisecond)

		if server.Input.Stage() == config.Stream {
			time.Sleep(500 * time.Millisecond)
			break
		}
	}
}

func createDataToSource(db *sql.DB, sourceDBName, tableName string, id int) error {
	_, err := db.Exec(fmt.Sprintf("insert into `%s`.`%s` (id, name) values(%d, 'gravity')", sourceDBName, tableName, id))
	return err
}

func getDocCount(es *elastic.Client, index string) (int, error) {
	_, err := es.Refresh(index).Do(context.Background())
	if err != nil {
		return 0, err
	}
	cnt, err := es.Count(index).Do(context.Background())
	if err != nil {
		return 0, err
	}
	return int(cnt), nil
}
