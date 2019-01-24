package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"strings"
	"testing"
	"time"

	"github.com/moiot/gravity/pkg/inputs/mysqlstream"
	"github.com/moiot/gravity/pkg/outputs/mysql"
	"github.com/moiot/gravity/pkg/sql_execution_engine"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/stretchr/testify/require"

	"github.com/moiot/gravity/pkg/app"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/sliding_window"
)

func init() {
	db := mysql_test.MustCreateSourceDBConn()
	_, err := db.Exec("drop database if exists " + consts.GravityDBName)
	if err != nil {
		panic(err)
	}
	_ = db.Close()
}

func TestMySQLToMySQLStream(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

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

	pipelineConfig := config.PipelineConfigV2{
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
	server, err := app.NewServer(pipelineConfig.ToV3())
	r.NoError(err)

	r.NoError(server.Start())

	// generate updates and close the server
	generator.SeedRows()
	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

	server.Close()

	// start the server again without insert/update.
	server, err = app.NewServer(pipelineConfig.ToV3())
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
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

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

	pipelineConfig := config.PipelineConfigV2{
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

	server, err := app.NewServer(pipelineConfig.ToV3())
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

func TestMySQLBatchWithInsertIgnore(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

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
	pipelineConfig := config.PipelineConfigV3{
		PipelineName: sourceDBName,
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: "mysql",
			Mode: "batch",
			Config: map[string]interface{}{
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
		OutputPlugin: config.GenericConfig{
			Type: "mysql",
			Config: map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},
				"enable-ddl": true,
				"sql-engine-config": &config.GenericConfig{
					Type: "mysql-insert-ignore",
				},
				"routes": []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
			},
		},
	}

	server, err := app.NewServer(pipelineConfig)
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
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

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

	pipelineConfig := config.PipelineConfigV2{
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

	server, err := app.NewServer(pipelineConfig.ToV3())
	r.NoError(err)

	r.NoError(server.Start())

	waitFullComplete(server.Input)

	// update after scan
	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

	server.Close()

	// restart server
	server, err = app.NewServer(pipelineConfig.ToV3())
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
		if i.Stage() == config.Stream {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func TestMySQLToMySQLPositionReset(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

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
	pipelineConfig := config.PipelineConfigV2{
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
	server, err := app.NewServer(pipelineConfig.ToV3())
	r.NoError(err)

	r.NoError(server.Start())

	waitFullComplete(server.Input)

	ctx, cancel := context.WithCancel(context.Background())
	done := generator.ParallelUpdate(ctx)

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	server.Close()

	// clear position store, truncate table,
	// and start over again.
	server, err = app.NewServer(pipelineConfig.ToV3())
	r.NoError(err)

	server.PositionStore.Clear()
	for _, t := range tables {
		_, err := targetDB.Exec(fmt.Sprintf("truncate table %s.%s", targetDBName, t))
		r.NoError(err)
	}

	server, err = app.NewServer(pipelineConfig.ToV3())
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

func TestMySQLToMyBidirection(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	serverCfg := config.PipelineConfigV3{
		PipelineName: sourceDBName,
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: "mysql",
			Mode: config.Stream,
			Config: struct2Map(mysqlstream.MySQLBinlogInputPluginConfig{
				IgnoreBiDirectionalData: true,
				Source:                  sourceDBConfig,
			}),
		},
		OutputPlugin: config.GenericConfig{
			Type: "mysql",
			Config: struct2Map(mysql.MySQLPluginConfig{
				DBConfig:  targetDBConfig,
				EnableDDL: true,
				Routes: []map[string]interface{}{
					{
						"match-schema":  sourceDBName,
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
				EngineConfig: &config.GenericConfig{
					Type: sql_execution_engine.MySQLReplaceEngine,
					Config: struct2Map(sql_execution_engine.MysqlReplaceEngineConfig{
						TagInternalTxn: true,
					}),
				},
			}),
		},
	}
	// start the server
	server, err := app.NewServer(serverCfg)
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

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	server.Input.Wait()
	server.Close()

	rows, err := targetDB.Query(fmt.Sprintf("select id from `%s`.t", targetDBName))
	r.NoError(err)
	defer rows.Close()

	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		r.NoError(err)
		ids = append(ids, id)
	}
	r.NoError(rows.Err())

	r.Equal(1, len(ids))
	r.Equal(2, ids[0])
}

func TestTagDDL(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	pipelineConfig := config.PipelineConfigV3{
		PipelineName: sourceDBName,
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: "mysql",
			Mode: config.Stream,
			Config: map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
			},
		},
		OutputPlugin: config.GenericConfig{
			Type: "mysql",
			Config: map[string]interface{}{
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
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
			},
		},
	}

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	tbl := "abc"
	_, err = sourceDB.Exec(fmt.Sprintf("%screate table `%s`.`%s`(`id` int(11),  PRIMARY KEY (`id`)) ENGINE=InnoDB", consts.DDLTag, sourceDBName, tbl))
	r.NoError(err)

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	<-server.Input.Done()

	server.Close()

	row := targetDB.QueryRow(fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE  TABLE_SCHEMA = '%s' and table_name = '%s'", targetDBName, tbl))
	var tblName string
	err = row.Scan(&tblName)
	r.Equal(sql.ErrNoRows, err)
}

func TestDDL(t *testing.T) {
	t.Parallel()

	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name()) + "_source"
	targetDBName := strings.ToLower(t.Name()) + "_target"

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	pipelineConfig := config.PipelineConfigV3{
		PipelineName: sourceDBName,
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: "mysql",
			Mode: config.Stream,
			Config: map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
			},
		},
		OutputPlugin: config.GenericConfig{
			Type: "mysql",
			Config: map[string]interface{}{
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
						"match-table":   "*",
						"target-schema": targetDBName,
					},
				},
			},
		},
	}

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	ddls := []string{
		`CREATE TABLE tn3 (
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  thirdparty tinyint(2) NOT NULL DEFAULT '0' COMMENT '第三方编号',
  PRIMARY KEY (id),
  KEY thirdparty (thirdparty)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='第三方调用记录';`,

		`alter table tn3 add column ii int(11)`,

		"CREATE TABLE tn4 like tn3",

		"CREATE TABLE IF NOT EXISTS tn4 like tn3",

		"create table `abc`(`id` int(11),  PRIMARY KEY (`id`)) ENGINE=InnoDB",
	}

	for _, ddl := range ddls {
		tx, err := sourceDB.Begin()
		r.NoError(err)

		_, err = tx.Exec("use " + sourceDBName)
		r.NoError(err)

		_, err = tx.Exec(ddl)
		r.NoError(err)

		r.NoError(tx.Commit())
	}

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	<-server.Input.Done()

	server.Close()
}

func TestDDLNoRoute(t *testing.T) {
	r := require.New(t)

	sourceDBName := strings.ToLower(t.Name())
	targetDBName := strings.ToLower(t.Name())

	sourceDB := mysql_test.MustSetupSourceDB(sourceDBName)
	defer sourceDB.Close()
	targetDB := mysql_test.MustSetupTargetDB(targetDBName)
	defer targetDB.Close()

	sourceDBConfig := mysql_test.SourceDBConfig()
	targetDBConfig := mysql_test.TargetDBConfig()

	pipelineConfig := config.PipelineConfigV3{
		PipelineName: sourceDBName,
		Version:      config.PipelineConfigV3Version,
		InputPlugin: config.InputConfig{
			Type: "mysql",
			Mode: config.Stream,
			Config: map[string]interface{}{
				"source": map[string]interface{}{
					"host":     sourceDBConfig.Host,
					"username": sourceDBConfig.Username,
					"password": sourceDBConfig.Password,
					"port":     sourceDBConfig.Port,
				},
			},
		},
		OutputPlugin: config.GenericConfig{
			Type: "mysql",
			Config: map[string]interface{}{
				"target": map[string]interface{}{
					"host":     targetDBConfig.Host,
					"username": targetDBConfig.Username,
					"password": targetDBConfig.Password,
					"port":     targetDBConfig.Port,
				},
				"enable-ddl": true,
			},
		},
	}

	server, err := app.NewServer(pipelineConfig)
	r.NoError(err)

	r.NoError(server.Start())

	tbl := "abc"
	ddls := []string{
		fmt.Sprintf("create table `%s`(`id` int(11),  PRIMARY KEY (`id`)) ENGINE=InnoDB", tbl),

		fmt.Sprintf("alter table `%s` add column v int(11)", tbl),
	}

	for _, ddl := range ddls {
		tx, err := sourceDB.Begin()
		r.NoError(err)

		_, err = tx.Exec("use " + sourceDBName)
		r.NoError(err)

		_, err = tx.Exec(ddl)
		r.NoError(err)

		r.NoError(tx.Commit())
	}

	err = mysql_test.SendDeadSignal(sourceDB, server.Input.Identity())
	r.NoError(err)

	<-server.Input.Done()

	server.Close()
}
