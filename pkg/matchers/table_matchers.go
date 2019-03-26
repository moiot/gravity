package matchers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

const TableMatcherName = "match-table"

type TableMatcher struct {
	TableGlob []string
}

func (m *TableMatcher) Configure(data interface{}) error {
	switch d := data.(type) {
	case string:
		m.TableGlob = append(m.TableGlob, d)
	case []interface{}:
		for _, o := range d {
			s := o.(string)
			m.TableGlob = append(m.TableGlob, s)
		}
	case []string:
		m.TableGlob = append(m.TableGlob, d...)
	default:
		return errors.Errorf("match-table only accept string or string slice, actual: %v", data)
	}
	return nil
}

func (m *TableMatcher) Match(msg *core.Msg) bool {
	for _, g := range m.TableGlob {
		if utils.Glob(g, msg.Table) {
			return true
		}
	}
	return false
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
