package mongokafka_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/outputs/mysql"

	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/inputs"
	"github.com/moiot/gravity/pkg/inputs/mongobatch"
	"github.com/moiot/gravity/pkg/mongo_test"
	"github.com/moiot/gravity/pkg/outputs"
	"github.com/moiot/gravity/pkg/utils"
)

func TestMongo2MysqlReplication(t *testing.T) {
	r := require.New(t)

	collectionName := strings.ToLower(t.Name())
	targetDBC := mysql_test.TargetDBConfig()
	targetDB := mysql_test.MustSetupTargetDB(db.Name)
	defer targetDB.Close()

	_, err := targetDB.Exec(fmt.Sprintf("CREATE TABLE `%s`.`%s` (`_id` varchar(36) NOT NULL, `name` varchar(10) NOT NULL, PRIMARY KEY (`_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", db.Name, collectionName))
	r.NoError(err)

	mongoCfg := mongo_test.TestConfig()
	pipelineConfig := config.PipelineConfigV3{
		PipelineName: t.Name(),
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: inputs.Mongo,
			Mode: config.Replication,
			Config: utils.Struct2Map(
				mongobatch.Config{
					Source: &mongoCfg,
				},
			),
		},
		OutputPlugin: config.GenericConfig{
			Type: outputs.Mysql,
			Config: utils.Struct2Map(mysql.MySQLPluginConfig{
				DBConfig: targetDBC,
				Routes: []map[string]interface{}{
					{
						"match-schema": db.Name,
						"match-table":  collectionName,
					},
				},
			}),
		},
	}

	operations := 100
	collection := db.C(collectionName)
	for i := 0; i < operations; i++ {
		r.NoError(collection.Insert(bson.M{"name": fmt.Sprint(i)}))
	}

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())
	for {
		time.Sleep(100 * time.Millisecond)

		if server.Input.Stage() == config.Stream {
			time.Sleep(500 * time.Millisecond)
			break
		}
	}

	operations2 := 10
	for i := operations; i < operations+operations2; i++ {
		doc := bson.M{"name": fmt.Sprint(i)}
		r.NoError(collection.Insert(doc))
		r.NoError(collection.Remove(doc))
	}

	r.NoError(server.Input.SendDeadSignal())
	server.Input.Wait()
	server.Close()

	row := targetDB.QueryRow(fmt.Sprintf("select count(1) from `%s`.`%s`", db.Name, collectionName))
	var cnt int
	r.NoError(row.Scan(&cnt))
	r.Equal(operations, cnt)
}
