package mysql

import (
	"database/sql"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/gravity/outputs/routers"
	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/schema_store"
	"github.com/moiot/gravity/sql_execution_engine"
)

type MySQLPluginConfig struct {
	DBConfig     *utils.DBConfig                                 `mapstructure:"target"  json:"target"`
	Routes       []map[string]interface{}                        `mapstructure:"routes"  json:"routes"`
	EngineConfig sql_execution_engine.MySQLExecutionEngineConfig `mapstructure:"sql-engine-config"  json:"sql-engine-config"`
}

type MySQLOutput struct {
	pipelineName       string
	cfg                *MySQLPluginConfig
	routes             []*routers.MySQLRoute
	db                 *sql.DB
	targetSchemaStore  schema_store.SchemaStore
	sqlExecutionEngine sql_execution_engine.SQlExecutionEngine
	tableConfigs       []config.TableConfig
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, "mysql", &MySQLOutput{}, false)
}

func (output *MySQLOutput) Configure(pipelineName string, data map[string]interface{}) error {
	output.pipelineName = pipelineName

	// setup plugin config
	pluginConfig := MySQLPluginConfig{}
	err := mapstructure.Decode(data, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	if pluginConfig.DBConfig == nil {
		return errors.Errorf("empty db config")
	}

	engineConfig := pluginConfig.EngineConfig
	if pluginConfig.EngineConfig.EngineType == "" {
		engine, err := sql_execution_engine.SelectEngine(
			engineConfig.DetectConflict,
			engineConfig.UseBidirection,
			engineConfig.UseShadingProxy)
		if err != nil {
			return errors.Trace(err)
		}
		pluginConfig.EngineConfig.EngineType = engine
	}


	output.cfg = &pluginConfig

	// init routes
	output.routes, err = routers.NewMySQLRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (output *MySQLOutput) Start() error {

	targetSchemaStore, err := schema_store.NewSimpleSchemaStore(output.cfg.DBConfig)
	if err != nil {
		return errors.Trace(err)
	}

	output.targetSchemaStore = targetSchemaStore

	db, err := utils.CreateDBConnection(output.cfg.DBConfig)
	if err != nil {
		return errors.Trace(err)
	}
	output.db = db

	sqlEngine := sql_execution_engine.NewSQLExecutionEngine(
		db,
		output.cfg.EngineConfig,
	)
	output.sqlExecutionEngine = sqlEngine
	return nil
}

func (output *MySQLOutput) Close() {
	output.db.Close()
	output.targetSchemaStore.Close()
}

// msgs in the same batch should have the same table name
func (output *MySQLOutput) Execute(msgs []*core.Msg) error {
	var targetTableDef *schema_store.Table
	var targetMsgs []*core.Msg

	for _, msg := range msgs {
		// ddl msg filter
		if !output.cfg.EngineConfig.EnableDDL && msg.Type == core.MsgDDL {
			continue
		}

		matched := false
		var targetSchema string
		var targetTable string
		for _, route := range output.routes {
			if route.Match(msg) {
				matched = true
				targetSchema, targetTable = route.GetTarget(msg.Database, msg.Table)
				break
			}
		}

		// none of the routes matched, skip this msg
		if !matched {
			continue
		}

		if targetTableDef == nil {
			schema, err := output.targetSchemaStore.GetSchema(targetSchema)
			if err != nil {
				return errors.Trace(err)
			}

			targetTableDef = schema[targetTable]
		}

		// go through a serial of filters inside this output plugin
		// right now, we only support AddMissingColumn
		if _, err := AddMissingColumn(msg, targetTableDef); err != nil {
			return errors.Trace(err)
		}

		targetMsgs = append(targetMsgs, msg)
	}

	batches := splitMsgBatchWithDelete(targetMsgs)

	for _, batch := range batches {
		if batch[0].Type == core.MsgDML && targetTableDef == nil {
			return errors.Errorf("[output-mysql] schema: %v, table: %v, targetTable nil", batch[0].Database, batch[0].Table)
		}

		err := output.sqlExecutionEngine.Execute(batch, targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}
		if batch[0].DdlMsg != nil {
			if err := output.targetSchemaStore.InvalidateCache(); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func splitMsgBatchWithDelete(msgBatch []*core.Msg) [][]*core.Msg {
	if len(msgBatch) == 0 {
		return [][]*core.Msg{}
	}

	batches := make([][]*core.Msg, 1)
	batches[0] = []*core.Msg{msgBatch[0]}

	for i := 1; i < len(msgBatch); i++ {
		msg := msgBatch[i]

		// if the current msg is DELETE, we create a new batch
		if msg.DmlMsg.Operation == core.Delete {
			batches = append(batches, []*core.Msg{msg})
			continue
		}

		lastBatchIdx := len(batches) - 1

		// if previous batch is DELETE, we create a new batch
		if batches[lastBatchIdx][0].DmlMsg.Operation == core.Delete {
			batches = append(batches, []*core.Msg{msg})
			continue
		}

		// otherwise, append the message to the last batch
		batches[lastBatchIdx] = append(batches[lastBatchIdx], msg)
	}

	return batches
}
