package matchers

import (
	"reflect"
	"strings"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

func NewMatchers(configData map[string]interface{}) (core.IMatcherGroup, error) {
	var retMatchers []core.IMatcher
	// find matchers based on configData
	for k, v := range configData {
		if strings.HasPrefix(k, "match") {
			matcherFactory, err := registry.GetPlugin(registry.MatcherPlugin, k)
			if err != nil {
				return nil, errors.Trace(err)
			}

			factory, ok := matcherFactory.(core.IMatcherFactory)
			if !ok {
				return nil, errors.Errorf("wrong type: %v", reflect.TypeOf(matcherFactory))
			}
			matcher := factory.NewMatcher()
			if err := matcher.Configure(v); err != nil {
				return nil, errors.Trace(err)
			}
			retMatchers = append(retMatchers, matcher)
		}
	}
	return retMatchers, nil
}
