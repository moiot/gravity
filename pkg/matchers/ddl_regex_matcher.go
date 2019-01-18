package matchers

import (
	"regexp"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const DDLRegexMatcherName = "match-ddl-regex"

type DDLRegexMatcher struct {
	reg *regexp.Regexp
}

func (m *DDLRegexMatcher) Configure(data interface{}) error {
	if arg, ok := data.(string); !ok {
		return errors.Errorf("match-ddl-regex only receives a string value, you provided: %v", data)
	} else {
		reg, err := regexp.Compile(arg)
		if err != nil {
			return errors.Annotatef(err, "fail to compile regexp %s", arg)
		}
		m.reg = reg
	}
	return nil
}

func (m *DDLRegexMatcher) Match(msg *core.Msg) bool {
	if msg.Type == core.MsgDML {
		return false
	}

	return m.reg.Match([]byte(msg.DdlMsg.Statement))
}

type ddlRegexMatcherFactoryType struct {
}

func (f *ddlRegexMatcherFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (f *ddlRegexMatcherFactoryType) NewMatcher() core.IMatcher {
	return &DDLRegexMatcher{}
}

func init() {
	registry.RegisterPlugin(registry.MatcherPlugin, DDLRegexMatcherName, &ddlRegexMatcherFactoryType{}, true)
}
