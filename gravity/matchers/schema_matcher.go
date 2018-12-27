package matchers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/utils"
)

const SchemaMatcherName = "match-schema"

type SchemaMatcher struct {
	SchemaGlob string
}

func (m *SchemaMatcher) Configure(data interface{}) error {
	if arg, ok := data.(string); !ok {
		return errors.Errorf("match-schema only receives a string value, you provided: %v", data)
	} else {
		m.SchemaGlob = arg
		return nil
	}
}

func (m *SchemaMatcher) Match(msg *core.Msg) bool {
	if utils.Glob(m.SchemaGlob, msg.Database) {
		return true
	} else {
		return false
	}
}

type schemaMatcherFactoryType struct {
}

func (f *schemaMatcherFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (f *schemaMatcherFactoryType) NewMatcher() core.IMatcher {
	return &SchemaMatcher{}
}

func init() {
	registry.RegisterPlugin(registry.MatcherPlugin, SchemaMatcherName, &schemaMatcherFactoryType{}, true)
}
