package sql_execution_engine

import (
	"github.com/moiot/gravity/gravity/registry"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySQLReplaceEngineConfigure(t *testing.T) {
	r := require.New(t)

	p, err := registry.GetPlugin(registry.SQLExecutionEnginePlugin, MySQLReplaceEngine)
	r.NoError(err)

	e := p.Configure("test", make(map[string]interface{}))
	r.NoError(e)

	_, ok := p.(EngineInitializer)
	r.True(ok)

	_, ok = p.(EngineExecutor)
	r.True(ok)
}