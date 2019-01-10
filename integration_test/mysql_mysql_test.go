package integration_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/sliding_window"

	"github.com/moiot/gravity/pkg/consts"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/gravity/inputs/stages"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/gravity"
	gravityConfig "github.com/moiot/gravity/gravity/config"
	"github.com/moiot/gravity/pkg/mysql_test"
)

func init() {
	db := mysql_test.MustCreateSourceDBConn()
	_, err := db.Exec("drop database if exists " + consts.GravityDBName)
	if err != nil {
		panic(err)
	}
	_ = db.Close()
	go func() {
		err = http.ListenAndServe(":8080", nil)
		if err != nil {
			log.Println("http error", err)
		}
	}()
}

func TestMySQLToMySQLStream(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)

	generator := mysql_test.Generator{
		SourceDB:     sourceDB,
		SourceSchema: sourceDBName,
		TargetDB:     targetDB,
		TargetSchema: targetDBName,
		GeneratorConfig: mysql_test.GeneratorConfig{
			NrTables:    10,
			NrSeedRows:  50,
			DeleteRatio: 0.2,
			InsertRatio: 0.1,
			Concurrency: 5,
		},
	}
	generator.SetupTestTables(true)

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	pipelineConfig := gravityConfig.PipelineConfigV2{
		PipelineName: sourceDBName,
		InputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
				"mode": "stream",
			},
		},
		OutputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},
				"enable-ddl": true,
				"routes": []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"target-schema": targetDBName,
					},
				},
			},
		},
	}
	// start the server
	server, err := gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	// generate updates and close the server
	generator.SeedRows()
	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

	server.Close()

	// start the server again without insert/update.
	server, err = gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	cancel()
	done.Wait()

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	server.Input.Wait()
	server.Close()

	r.NoError(generator.TestChecksum())
}

func TestMySQLBatch(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)

	generator := mysql_test.Generator{
		SourceDB:     sourceDB,
		SourceSchema: sourceDBName,
		TargetDB:     targetDB,
		TargetSchema: targetDBName,
		GeneratorConfig: mysql_test.GeneratorConfig{
			NrTables:    10,
			NrSeedRows:  50,
			DeleteRatio: 0.2,
			InsertRatio: 0.1,
			Concurrency: 5,
		},
	}
	tables := generator.SetupTestTables(false)
	generator.SeedRows()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	tableConfigs := []map[string]interface{}{
		{
			"schema": sourceDBName,
			"table":  tables,
		},
	}

	pipelineConfig := gravityConfig.PipelineConfigV2{
		PipelineName: sourceDBName,
		InputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
				"table-configs": tableConfigs,
				"mode":          "batch",
			},
		},
		OutputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},

				"routes": []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
				"enable-ddl": true,
			},
		},
	}

	server, err := gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	<-server.Input.Done()

	// wait for some time to see if server is healthy
	sliding_window.DefaultHealthyThreshold = 4
	time.Sleep(5)

	r.True(server.Scheduler.Healthy())

	server.Close()

	r.NoError(generator.TestChecksum())
}

func TestMySQLToMySQLReplication(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)

	generator := mysql_test.Generator{
		SourceDB:     sourceDB,
		SourceSchema: sourceDBName,
		TargetDB:     targetDB,
		TargetSchema: targetDBName,
		GeneratorConfig: mysql_test.GeneratorConfig{
			NrTables:    10,
			NrSeedRows:  50,
			DeleteRatio: 0.2,
			InsertRatio: 0.1,
			Concurrency: 5,
		},
	}
	tables := generator.SetupTestTables(false)
	generator.SeedRows()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	tableConfigs := []map[string]interface{}{
		{
			"schema": sourceDBName,
			"table":  tables,
		},
	}

	pipelineConfig := gravityConfig.PipelineConfigV2{
		PipelineName: sourceDBName,
		InputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
				"table-configs": tableConfigs,
				"mode":          "replication",
			},
		},
		OutputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},

				"routes": []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
				"enable-ddl": true,
			},
		},
	}

	server, err := gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	waitFullComplete(server.Input)

	// update after scan
	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

	server.Close()

	// restart server
	server, err = gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	cancel()
	done.Wait()

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	server.Input.Wait()
	server.Close()

	r.NoError(generator.TestChecksum())
}

func waitFullComplete(i core.Input) {
	for {
		if i.Stage() == stages.InputStageIncremental {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func TestMySQLToMySQLPositionReset(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)

	generator := mysql_test.Generator{
		SourceDB:     sourceDB,
		SourceSchema: sourceDBName,
		TargetDB:     targetDB,
		TargetSchema: targetDBName, GeneratorConfig: mysql_test.GeneratorConfig{
			NrTables:    10,
			NrSeedRows:  50,
			DeleteRatio: 0.2,
			InsertRatio: 0.1,
			Concurrency: 5,
		},
	}
	tables := generator.SetupTestTables(false)
	generator.SeedRows()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	tableConfigs := []map[string]interface{}{
		{
			"schema": sourceDBName,
			"table":  tables,
		},
	}
	pipelineConfig := gravityConfig.PipelineConfigV2{
		PipelineName: sourceDBName,
		InputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
				"table-configs": tableConfigs,
				"mode":          "replication",
			},
		},
		OutputPlugins: map[string]interface{}{
			"mysql": map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},

				"routes": []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"target-schema": targetDBName,
					},
				},
				"enable-ddl": true,
			},
		},
	}

	// start full, incremental, close server
	server, err := gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	waitFullComplete(server.Input)

	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	server.Close()

	// clear position store, truncate table,
	// and start over again.
	server, err = gravity.NewServer(&pipelineConfig)
	r.NoError(err)

	server.PositionStore.Clear()
	for _, t := range tables {
		_, err := targetDB.Exec(fmt.Sprintf("truncate table %s.%s", targetDBName, t))
		r.NoError(err)
	}

	server, err = gravity.NewServer(&pipelineConfig)
	r.NoError(err)
	r.NoError(server.Start())

	waitFullComplete(server.Input)

	cancel()
	done.Wait()

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	server.Input.Wait()
	server.Close()
	r.NoError(generator.TestChecksum())
}
