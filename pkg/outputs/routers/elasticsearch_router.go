package routers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
)

type ElasticsearchRoute struct {
	RouteMatchers
	TargetIndex        string
	TargetType         string
	IgnoreNoPrimaryKey bool
}

type ElasticsearchRouter []*ElasticsearchRoute

func (r ElasticsearchRouter) Exists(msg *core.Msg) bool {
	_, ok := r.Match(msg)
	return ok
}

func (r ElasticsearchRouter) Match(msg *core.Msg) (*ElasticsearchRoute, bool) {
	for _, route := range r {
		if route.Match(msg) {
			return route, true
		}
	}
	return nil, false
}

func NewElasticsearchRoutes(configData []map[string]interface{}) ([]*ElasticsearchRoute, error) {
	var routes []*ElasticsearchRoute

	for _, routeConfig := range configData {
		route := ElasticsearchRoute{}
		retMatchers, err := matchers.NewMatchers(routeConfig)
		if err != nil {
			return nil, errors.Trace(err)
		}

		route.AllMatchers = retMatchers

		targetIndex, err := getString(routeConfig, "target-index", "")
		if err != nil {
			return nil, err
		}
		route.TargetIndex = targetIndex

		targetType, err := getString(routeConfig, "target-type", "doc")
		if err != nil {
			return nil, err
		}
		route.TargetType = targetType

		ignoreNoPrimaryKey, err := getBool(routeConfig, "ignore-no-primary-key", false)
		if err != nil {
			return nil, err
		}
		route.IgnoreNoPrimaryKey = ignoreNoPrimaryKey

		routes = append(routes, &route)
	}
	return routes, nil
}
