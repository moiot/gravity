package matchers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/utils"
)

const TableMatcherName = "match-table"

type TableMatcher struct {
	TableGlob string
}

func (m *TableMatcher) Configure(data interface{}) error {
	if arg, ok := data.(string); !ok {
		return errors.Errorf("match-table only receives a string, you provided: %v", data)
	} else {
		m.TableGlob = arg
		return nil
	}
}

func (m *TableMatcher) Match(msg *core.Msg) bool {
	if utils.Glob(m.TableGlob, msg.Table) {
		return true
	} else {
		return false
	}
}

type tableMatcherFactoryType struct {
}

func (f *tableMatcherFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (f *tableMatcherFactoryType) NewMatcher() core.IMatcher {
	return &TableMatcher{}
}

func init() {
	registry.RegisterPlugin(registry.MatcherPlugin, TableMatcherName, &tableMatcherFactoryType{}, true)
}
