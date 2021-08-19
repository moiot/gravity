package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/env"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/outputs/routers"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/schema_store"
	"github.com/moiot/gravity/pkg/sql_execution_engine"
	"github.com/moiot/gravity/pkg/utils"
)

const (
	Name = "mysql"
)

type MySQLPluginConfig struct {
	DBConfig     *config.DBConfig            `mapstructure:"target"  json:"target"`
	Routes       []map[string]interface{}    `mapstructure:"routes"  json:"routes"`
	EnableDDL    bool                        `mapstructure:"enable-ddl" json:"enable-ddl"`
	EngineConfig *config.GenericPluginConfig `mapstructure:"sql-engine-config"  json:"sql-engine-config"`
}

type MySQLOutput struct {
	pipelineName             string
	cfg                      *MySQLPluginConfig
	routes                   []*routers.MySQLRoute
	db                       *sql.DB
	targetSchemaStore        schema_store.SchemaStore
	sqlExecutionEnginePlugin registry.Plugin
	sqlExecutor              sql_execution_engine.EngineExecutor
	tableConfigs             []config.TableConfig
	isTiDB                   bool

	// MySQL ignores comment in drop table stmt in some versions, see https://bugs.mysql.com/bug.php?id=87852
	// to prevent endless bidirectional drop tables, we keep the recent dropped table names
	droppedTable sync.Map
}

const keepDropTableSeconds = 30

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &MySQLOutput{}, false)
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

	if pluginConfig.EngineConfig == nil {
		pluginConfig.EngineConfig = &config.GenericPluginConfig{
			Type:   sql_execution_engine.MySQLReplaceEngine,
			Config: sql_execution_engine.DefaultMySQLReplaceEngineConfig,
		}
	}

	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, pluginConfig.EngineConfig.Type)
	if err != nil {
		return errors.Errorf("failed to find plugin: %v", errors.ErrorStack(err))
	}

	log.Infof("[output-mysql] Using %s", pluginConfig.EngineConfig.Type)

	if err := p.Configure(pipelineName, pluginConfig.EngineConfig.Config); err != nil {
		return errors.Trace(err)
	}
	output.sqlExecutionEnginePlugin = p

	output.cfg = &pluginConfig

	// init routes
	output.routes, err = routers.NewMySQLRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (output *MySQLOutput) Start() error {

	db, err := utils.CreateDBConnection(output.cfg.DBConfig)
	if err != nil {
		return errors.Trace(err)
	}
	output.db = db

	targetSchemaStore, err := schema_store.NewSimpleSchemaStoreFromDBConn(output.db)
	if err != nil {
		return errors.Trace(err)
	}

	output.targetSchemaStore = targetSchemaStore

	engineInitializer, ok := output.sqlExecutionEnginePlugin.(sql_execution_engine.EngineInitializer)
	if !ok {
		return errors.Errorf("sql engine plugin is not a db conn setter")
	}

	if err := engineInitializer.Init(db); err != nil {
		return errors.Trace(err)
	}

	sqlExecutor, ok := output.sqlExecutionEnginePlugin.(sql_execution_engine.EngineExecutor)
	if !ok {
		return errors.Errorf("sql engine plugin is not a executor")
	}

	output.sqlExecutor = sqlExecutor
	output.isTiDB = utils.IsTiDB(db)
	return nil
}

func (output *MySQLOutput) Close() {
	if output.db != nil {
		output.db.Close()
	}
	if output.targetSchemaStore != nil {
		output.targetSchemaStore.Close()
	}
}

func (output *MySQLOutput) GetRouter() core.Router {
	return routers.MySQLRouter(output.routes)
}

func (output *MySQLOutput) route0(s, t string) (schema, table string) {
	fakeMsg := core.Msg{
		Database: s,
		Table:    t,
	}

	for _, route := range output.routes {
		if route.Match(&fakeMsg) {
			schema, table = route.GetTarget(fakeMsg.Database, fakeMsg.Table)
			break
		}
	}

	return
}

func (output *MySQLOutput) markTableDropped(schema, table string) {
	output.droppedTable.Store(utils.TableIdentity(schema, table), time.Now())
	output.cleanupDroppedTable()
}

func (output *MySQLOutput) markTableCreated(schema, table string) {
	output.droppedTable.Delete(utils.TableIdentity(schema, table))
	output.cleanupDroppedTable()
}

func (output *MySQLOutput) hasDropped(schema, table string) bool {
	_, ok := output.droppedTable.Load(utils.TableIdentity(schema, table))
	return ok
}

func (output *MySQLOutput) cleanupDroppedTable() {
	now := time.Now()
	var toDelete []string
	output.droppedTable.Range(func(key, value interface{}) bool {
		if now.Sub(value.(time.Time)).Seconds() > keepDropTableSeconds {
			toDelete = append(toDelete, key.(string))
		}
		return true
	})

	for _, k := range toDelete {
		output.droppedTable.Delete(k)
	}
}

func toTableName(s, t string) *ast.TableName {
	return &ast.TableName{
		Schema: model.CIStr{
			O: s,
			L: strings.ToLower(s),
		},
		Name: model.CIStr{
			O: t,
			L: strings.ToLower(t),
		},
	}
}

func defaultIfEmpty(target, def string) string {
	if target == "" {
		return def
	} else {
		return target
	}
}

// msgs in the same batch should have the same table name
func (output *MySQLOutput) Execute(msgs []*core.Msg) error {
	var targetTableDef *schema_store.Table
	var targetMsgs []*core.Msg

	for _, msg := range msgs {
		// ddl msg filter
		if !output.cfg.EnableDDL && msg.Type == core.MsgDDL {
			continue
		}

		targetSchema, targetTable := output.route0(msg.Database, msg.Table)

		switch msg.Type {
		case core.MsgDDL:
			if msg.DdlMsg.AST == nil {
				log.Info("[output-mysql] ignore unsupported ddl: ", msg.DdlMsg.Statement)
				return nil
			}

			if targetSchema == "" {
				log.Infof("[output-mysql] ignore no router ddl. schema: %s, table: %s, msg: %s", msg.Database, msg.Table, msg)
				continue
			}

			switch node := msg.DdlMsg.AST.(type) {
			case *ast.CreateDatabaseStmt:
				tmp := *node
				tmp.Name = targetSchema
				tmp.IfNotExists = true
				stmt := restore(&tmp)
				err := output.executeDDL(targetSchema, stmt)
				if err != nil {
					log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err)
				}
				log.Info("[output-mysql] executed ddl: ", stmt)
				metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "create-db").Add(1)

			case *ast.DropDatabaseStmt:
				tmp := *node
				tmp.Name = targetSchema
				tmp.IfExists = true
				stmt := restore(&tmp)
				err := output.executeDDL(targetSchema, stmt)
				if err != nil {
					log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err)
				}
				log.Info("[output-mysql] executed ddl: ", stmt)
				metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "drop-db").Add(1)

			case *ast.CreateTableStmt:
				tmp := *node
				tmp.Table = toTableName(targetSchema, targetTable)

				//handle create table like if the referenced table has been renamed
				if tmp.ReferTable != nil {
					refSchema, refTable := output.route0(tmp.ReferTable.Schema.O, tmp.ReferTable.Name.O)
					if refSchema != "" {
						tmp.ReferTable = toTableName(refSchema, refTable)
					}
				}

				tmp.IfNotExists = true
				stmt := restore(&tmp)
				err := output.executeDDL(targetSchema, stmt)
				if err != nil {
					log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err)
				}
				log.Info("[output-mysql] executed ddl: ", stmt)
				metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "create-table").Add(1)
				output.targetSchemaStore.InvalidateSchemaCache(targetSchema)
				output.markTableCreated(msg.Database, msg.Table)

			case *ast.DropTableStmt:
				if !output.hasDropped(msg.Database, msg.Table) {
					tmp := *node
					tmp.Tables[0] = toTableName(targetSchema, targetTable)
					tmp.IfExists = true
					stmt := restore(&tmp)
					err := output.executeDDL(targetSchema, stmt)
					if err != nil {
						log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err)
					}
					log.Info("[output-mysql] executed ddl: ", stmt)
					metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "drop-table").Add(1)
					output.targetSchemaStore.InvalidateSchemaCache(targetSchema)
					output.markTableDropped(msg.Database, msg.Table)
				} else {
					log.Warnf("table %s has been dropped recently. This might be a bidirectional stmt, ignore", utils.TableIdentity(msg.Database, msg.Table))
				}

			case *ast.AlterTableStmt:
				var targetDDLs []string
				if output.isTiDB {
					for _, spec := range node.Specs {
						tmp := &ast.AlterTableStmt{
							Table: toTableName(targetSchema, targetTable),
							Specs: []*ast.AlterTableSpec{spec},
						}
						targetDDLs = append(targetDDLs, restore(tmp))
					}
				} else {
					tmp := *node
					tmp.Table = toTableName(targetSchema, targetTable)
					targetDDLs = append(targetDDLs, restore(&tmp))
				}
				for _, stmt := range targetDDLs {
					err := output.executeDDL(targetSchema, stmt)
					if err != nil {
						if e, ok := errors.Cause(err).(*mysqldriver.MySQLError); ok && (e.Number == 1060 || e.Number == 1061) {
							log.Errorf("[output-mysql] ignore duplicate column or index. ddl: %s. err: %s", stmt, e)
						} else {
							log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err, ". gtid: ", msg.InputContext)
						}
					} else {
						log.Info("[output-mysql] executed ddl: ", stmt)
						metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "alter-table").Add(1)
						output.targetSchemaStore.InvalidateSchemaCache(targetSchema)
					}
				}

			case *ast.RenameTableStmt:
				var targetDDLs []string
				tmp := *node
				for i, tt := range node.TableToTables {
					os, ot := output.route0(defaultIfEmpty(tt.OldTable.Schema.O, msg.Database), tt.OldTable.Name.O)
					ns, nt := output.route0(defaultIfEmpty(tt.NewTable.Schema.O, msg.Database), tt.NewTable.Name.O)

					if ns == "" {
						ns = tt.NewTable.Schema.O
					}

					if nt == "" {
						nt = tt.NewTable.Name.O
					}

					if output.isTiDB {
						a := &ast.RenameTableStmt{
							OldTable: toTableName(os, ot),
							NewTable: toTableName(ns, nt),
							TableToTables: []*ast.TableToTable{
								{
									OldTable: toTableName(os, ot),
									NewTable: toTableName(ns, nt),
								},
							},
						}
						targetDDLs = append(targetDDLs, restore(a))
					} else {
						tmp.TableToTables[i].OldTable = toTableName(os, ot)
						tmp.TableToTables[i].NewTable = toTableName(ns, nt)
					}
				}

				if !output.isTiDB {
					targetDDLs = append(targetDDLs, restore(&tmp))
				}

				for _, stmt := range targetDDLs {
					err := output.executeDDL(targetSchema, stmt)
					if err != nil {
						log.Fatal("[output-mysql] error exec ddl: ", stmt, ". err:", err)
					} else {
						log.Info("[output-mysql] executed ddl: ", stmt)
						metrics.OutputCounter.WithLabelValues(output.pipelineName, targetSchema, targetTable, string(core.MsgDDL), "rename-table").Add(1)
						output.targetSchemaStore.InvalidateSchemaCache(targetSchema)
					}
				}

			default:
				log.Info("[output-mysql] ignore unsupported ddl: ", msg.DdlMsg.Statement)
			}

			return nil

		case core.MsgDML:
			if targetSchema == "" {
				log.Debugf("[output-mysql] ignore no router DML. schema: %s, table: %s, msg: %s", msg.Database, msg.Table, msg)
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
	}

	batches := splitMsgBatchWithDelete(targetMsgs)

	for _, batch := range batches {
		if batch[0].Type == core.MsgDDL {
			return errors.Errorf("[output-mysql] shouldn't see ddl in sql engine")
		}
		if targetTableDef == nil {
			return errors.Errorf("[output-mysql] schema %v.%v not found", batch[0].Database, batch[0].Table)
		}

		err := output.sqlExecutor.Execute(batch, targetTableDef)
		if err != nil {
			return errors.Trace(err)
		}

		metrics.OutputCounter.WithLabelValues(output.pipelineName, targetTableDef.Schema, targetTableDef.Name, string(core.MsgDML), output.cfg.EngineConfig.Type).Add(float64(len(batch)))
	}

	return nil
}

func restore(node ast.Node) string {
	writer := &strings.Builder{}
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|format.RestoreNameBackQuotes, writer)
	err := node.Restore(ctx)
	if err != nil {
		log.Fatalf("error restore ddl %s, err: %s", node.Text(), err)
	}
	return writer.String()
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

func (output *MySQLOutput) executeDDL(targetSchema, stmt string) error {
	stmt = consts.DDLTag + fmt.Sprintf("/*%s*/", env.PipelineName) + stmt
	if targetSchema != "" {
		tx, err := output.db.Begin()
		if err != nil {
			return errors.Trace(err)
		}
		_, err = tx.Exec("use `" + targetSchema + "`")
		if err != nil {
			_ = tx.Rollback()
			return errors.Trace(err)
		}
		_, err = tx.Exec(stmt)
		if err != nil {
			_ = tx.Rollback()
			return errors.Trace(err)
		}
		err = tx.Commit()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		_, err := output.db.Exec(stmt)
		return errors.Trace(err)
	}

	return nil
}
