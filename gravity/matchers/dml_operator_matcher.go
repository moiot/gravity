package matchers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/registry"
	"github.com/moiot/gravity/pkg/core"
)

const dmlOpMatcherName = "match-dml-op"

type dmlOpMatcher struct {
	op map[string]bool
}

func (m *dmlOpMatcher) Configure(data interface{}) error {
	switch d := data.(type) {
	case string:
		if validateOp(d) {
			m.op[d] = true
		} else {
			return errors.Errorf("match-dml-op invalid operation type %v", data)
		}
	case []interface{}:
		for _, o := range d {
			s := o.(string)
			if validateOp(s) {
				m.op[s] = true
			} else {
				return errors.Errorf("match-dml-op invalid operation type %v", data)
			}
		}
	case []string:
		for _, o := range d {
			if validateOp(o) {
				m.op[o] = true
			} else {
				return errors.Errorf("match-dml-op invalid operation type %v", data)
			}
		}
	default:
		return errors.Errorf("match-dml-op only accept string or string slice, actual: %v", data)
	}
	return nil
}

func validateOp(op string) bool {
	if op == string(core.Insert) || op == string(core.Update) || op == string(core.Delete) {
		return true
	}

	return false
}

func (m *dmlOpMatcher) Match(msg *core.Msg) bool {
	if msg.DmlMsg == nil {
		return false
	}

	return m.op[string(msg.DmlMsg.Operation)]
}

type dmlOpMatcherFactoryType struct {
}

func (f *dmlOpMatcherFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (f *dmlOpMatcherFactoryType) NewMatcher() core.IMatcher {
	return &dmlOpMatcher{
		make(map[string]bool),
	}
}

func init() {
	registry.RegisterPlugin(registry.MatcherPlugin, dmlOpMatcherName, &dmlOpMatcherFactoryType{}, true)
}
