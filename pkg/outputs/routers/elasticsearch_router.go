package routers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
)

type ElasticsearchRoute struct {
	RouteMatchers
	TargetIndex string
	TargetType  string
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

		if targetIndex, ok := routeConfig["target-index"]; !ok {
			// the default value is the table name
			route.TargetIndex = ""
		} else {
			targetIndexString, ok := targetIndex.(string)
			if !ok {
				return nil, errors.Errorf("target-index is invalid")
			}
			route.TargetIndex = targetIndexString
		}

		if targetType, ok := routeConfig["target-type"]; !ok {
			route.TargetType = "doc"
		} else {
			targetTypeString, ok := targetType.(string)
			if !ok {
				return nil, errors.Errorf("target-index is invalid")
			}
			route.TargetType = targetTypeString
		}

		routes = append(routes, &route)
	}
	return routes, nil
}
