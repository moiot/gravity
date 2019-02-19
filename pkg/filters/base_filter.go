package filters

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
)

type BaseFilter struct {
	Matchers core.IMatcherGroup
}

func (baseFilter *BaseFilter) ConfigureMatchers(configData map[string]interface{}) error {
	// find Matchers based on configData
	retMatchers, err := matchers.NewMatchers(configData)
	if err != nil {
		return errors.Trace(err)
	}
	baseFilter.Matchers = retMatchers

	if len(baseFilter.Matchers) == 0 {
		return errors.Errorf("no matcher configured for this filter. config: %v", configData)
	}
	return nil
}

func (baseFilter *BaseFilter) MatchMsg(msg *core.Msg) bool {
	return baseFilter.Matchers.Match(msg)
}
