package matchers

import (
	"regexp"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const TableRegexMatcherName = "match-table-regex"

type TableRegexMatcher struct {
	Regex []*regexp.Regexp
}

func (m *TableRegexMatcher) Configure(data interface{}) error {
	switch d := data.(type) {
	case string:
		m.Regex = append(m.Regex, regexp.MustCompile(d))
	case []interface{}:
		for _, o := range d {
			s := o.(string)
			m.Regex = append(m.Regex, regexp.MustCompile(s))
		}
	case []string:
		for _, s := range d {
			m.Regex = append(m.Regex, regexp.MustCompile(s))
		}
	default:
		return errors.Errorf("match-table-regex only accept string or string slice, actual: %v", data)
	}
	return nil
}

func (m *TableRegexMatcher) Match(msg *core.Msg) bool {
	for _, r := range m.Regex {
		if r.MatchString(msg.Table) {
			return true
		}
	}
	return false
}

type TableRegexMatcherFactoryType struct {
}

func (f *TableRegexMatcherFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (f *TableRegexMatcherFactoryType) NewMatcher() core.IMatcher {
	return &TableRegexMatcher{}
}

func init() {
	registry.RegisterPlugin(registry.MatcherPlugin, TableRegexMatcherName, &TableRegexMatcherFactoryType{}, true)
}
