package config

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/sql_execution_engine"
	log "github.com/sirupsen/logrus"
)

type TargetMySQLWorkerConfig struct {
	EnableDDL          bool     `toml:"enable-ddl" json:"enable-ddl"`
	UseBidirection     bool     `toml:"use-bidirection" json:"use-bidirection"`
	UseShadingProxy    bool     `toml:"use-shading-proxy" json:"use-shading-proxy"`
	SQLExecutionEngine string   `toml:"sql-execution-engine" json:"sql-execution-engine"`
	Plugins            []string `toml:"plugins" json:"plugins"`
}

func (cfg *TargetMySQLWorkerConfig) SetSQLEngine() {
	engine, err := sql_execution_engine.SelectEngine(false, cfg.UseBidirection, cfg.UseShadingProxy)
	if err != nil {
		log.Fatalf("failed err: %v", errors.ErrorStack(err))
	}
	cfg.SQLExecutionEngine = engine
}
