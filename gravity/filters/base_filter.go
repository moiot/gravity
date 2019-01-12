package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/matchers"
	"github.com/moiot/gravity/pkg/core"
)

type BaseFilter struct {
	matchers core.IMatcherGroup
}

func (baseFilter *BaseFilter) ConfigureMatchers(configData map[string]interface{}) error {
	// find matchers based on configData
	retMatchers, err := matchers.NewMatchers(configData)
	if err != nil {
		return errors.Trace(err)
	}
	baseFilter.matchers = retMatchers

	if len(baseFilter.matchers) == 0 {
		return errors.Errorf("no matcher configured for this filter. config: %v", configData)
	}
	return nil
}

func (baseFilter *BaseFilter) MatchMsg(msg *core.Msg) bool {
	return baseFilter.matchers.Match(msg)
}
