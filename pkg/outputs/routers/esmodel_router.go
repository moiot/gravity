package routers

import (
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
	"strconv"
)

const (
	EsModelOneOneObject = int64(1)
	EsModelOneOneExtend = int64(2)

	EsModelTypeMappingObject = "object"
	EsModelTypeMappingNested = "nested"

	EsModelVersion7 = "7"
	EsModelVersion6 = "6"

	EsModelRelationMain    = 1
	EsModelRelationOneOne  = 2
	EsModelRelationOneMany = 3
)

type EsModelBaseRoute struct {
	RouteMatchers
	DataBase      string
	Table         string
	PkColumn      string
	ConvertColumn *map[string]string
	ExcludeColumn *map[string]string
	IncludeColumn *map[string]string

	RouteType int
}

type EsModelOneManyRoute struct {
	EsModelBaseRoute
	FkColumn     string
	PropertyName string
}

type EsModelOneOneRoute struct {
	EsModelOneManyRoute
	Mode        int64
	PropertyPre string
}

type EsModelRoute struct {
	EsModelBaseRoute

	IndexName          string
	TypeName           string
	ShardsNum          int64
	ReplicasNum        int64
	EsVer              string
	RetryCount         int
	IgnoreNoPrimaryKey bool
	OneOne             *[]*EsModelOneOneRoute
	OneMany            *[]*EsModelOneManyRoute
}

type EsModelRouter []*EsModelRoute

func (r EsModelRouter) Exists(msg *core.Msg) bool {
	for _, route := range r {
		if route.Match(msg) {
			return true
		}
		if route.OneOne != nil {
			for _, r := range *route.OneOne {
				if r.Match(msg) {
					return true
				}
			}
		}
		if route.OneMany != nil {
			for _, r := range *route.OneMany {
				if r.Match(msg) {
					return true
				}
			}
		}
	}
	return false
}

func (r EsModelRouter) Match(msg *core.Msg) (*[]*EsModelRoute, bool) {
	routes := make([]*EsModelRoute, 0, 3)
	for _, route := range r {
		if route.Match(msg) {
			routes = append(routes, route)
			continue
		}
		mtype := false
		if route.OneOne != nil {
			for _, r := range *route.OneOne {
				if r.Match(msg) {
					routes = append(routes, route)
					mtype = true
					break
				}
			}
		}
		if mtype {
			continue
		}
		if route.OneMany != nil {
			for _, r := range *route.OneMany {
				if r.Match(msg) {
					routes = append(routes, route)
					break
				}
			}
		}
	}
	if len(routes) > 0 {
		return &routes, true
	}
	return nil, false
}

func NewEsModelRoutes(configData []map[string]interface{}) ([]*EsModelRoute, error) {
	var routes []*EsModelRoute

	for _, routeConfig := range configData {
		route := EsModelRoute{}

		baseRouter, err := NewEsModelBaseRoute(routeConfig, &route.EsModelBaseRoute)
		if err != nil {
			return nil, err
		}
		route.EsModelBaseRoute = *baseRouter
		route.RouteType = EsModelRelationMain

		indexName, err := getString(routeConfig, "index-name", "")
		if err != nil {
			return nil, err
		}
		route.IndexName = indexName

		typeName, err := getString(routeConfig, "type-name", "")
		if err != nil {
			return nil, err
		}
		route.TypeName = typeName

		shardsNum, err := getInt64(routeConfig, "shards-num", int64(1))
		if err != nil {
			return nil, err
		}
		route.ShardsNum = shardsNum

		replicasNum, err := getInt64(routeConfig, "replicas-num", int64(0))
		if err != nil {
			return nil, err
		}
		route.ReplicasNum = replicasNum

		retry, err := getInt64(routeConfig, "retry-count", 3)
		if err != nil {
			return nil, err
		}
		ret := strconv.FormatInt(retry, 10)
		re, err := strconv.Atoi(ret)
		if err != nil {
			return nil, err
		}
		route.RetryCount = re

		ignoreNoPrimaryKey, err := getBool(routeConfig, "ignore-no-primary-key", false)
		if err != nil {
			return nil, err
		}
		route.IgnoreNoPrimaryKey = ignoreNoPrimaryKey

		oneRouters, err := NewEsModelOneOneRoutes(routeConfig)
		if err != nil {
			return nil, err
		}
		route.OneOne = &oneRouters

		manyRouters, err := NewEsModelOneManyRoutes(routeConfig)
		if err != nil {
			return nil, err
		}
		route.OneMany = &manyRouters

		routes = append(routes, &route)
	}
	return routes, nil
}

func NewEsModelOneOneRoutes(routeConfig map[string]interface{}) ([]*EsModelOneOneRoute, error) {

	ones, err := getListMap(routeConfig, "one-one", []map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	oneCount := len(ones)
	oneRouters := make([]*EsModelOneOneRoute, 0, oneCount)
	for _, v := range ones {
		oneRouter := &EsModelOneOneRoute{}

		manyRouter, err := NewEsModelOneManyRoute(v, &oneRouter.EsModelOneManyRoute)
		if err != nil {
			return nil, err
		}
		oneRouter.EsModelOneManyRoute = *manyRouter

		oneRouter.RouteType = EsModelRelationOneOne

		ppre, err := getString(v, "property-pre", "")
		if err != nil {
			return nil, err
		}
		oneRouter.PropertyPre = ppre

		mode, err := getInt64(v, "mode", EsModelOneOneObject)
		if err != nil {
			return nil, err
		}
		if mode == EsModelOneOneExtend && "" == oneRouter.PropertyPre {
			return nil, errors.Errorf("EsModelOneOneExtend mode property-pre is nil")
		} else if "" == oneRouter.PropertyName {
			return nil, errors.Errorf("property-name is nil")
		}
		oneRouter.Mode = mode

		oneRouters = append(oneRouters, oneRouter)
	}
	return oneRouters, nil
}

func NewEsModelOneManyRoutes(routeConfig map[string]interface{}) ([]*EsModelOneManyRoute, error) {

	ones, err := getListMap(routeConfig, "one-many", []map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	manyCount := len(ones)
	manyRouters := make([]*EsModelOneManyRoute, 0, manyCount)
	for _, v := range ones {
		manyRouter := &EsModelOneManyRoute{}
		manyRouter, err := NewEsModelOneManyRoute(v, manyRouter)
		if err != nil {
			return nil, err
		}
		manyRouters = append(manyRouters, manyRouter)
	}
	return manyRouters, nil
}

func NewEsModelOneManyRoute(routeConfig map[string]interface{}, manyRoute *EsModelOneManyRoute) (*EsModelOneManyRoute, error) {

	baseRouter, err := NewEsModelBaseRoute(routeConfig, &manyRoute.EsModelBaseRoute)
	if err != nil {
		return nil, err
	}
	manyRoute.EsModelBaseRoute = *baseRouter

	fkColumn, err := getString(routeConfig, "fk-column", "")
	if err != nil {
		return nil, err
	}
	if fkColumn == "" {
		return nil, errors.Errorf("%s fk-column is nil", manyRoute.AllMatchers)
	}
	manyRoute.FkColumn = fkColumn

	pname, err := getString(routeConfig, "property-name", "")
	if err != nil {
		return nil, err
	}
	manyRoute.PropertyName = pname

	return manyRoute, nil
}

func NewEsModelBaseRoute(routeConfig map[string]interface{}, baseRoute *EsModelBaseRoute) (*EsModelBaseRoute, error) {

	baseRoute.PkColumn = ""
	baseRoute.RouteType = EsModelRelationOneMany

	matchers, err := matchers.NewMatchers(routeConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	baseRoute.AllMatchers = matchers

	database, err := getString(routeConfig, "match-schema", "")
	if err != nil {
		return nil, err
	}
	if database == "" {
		return nil, errors.Errorf("%s match-schema is nil", baseRoute.AllMatchers)
	}
	baseRoute.DataBase = database

	table, err := getString(routeConfig, "match-table", "")
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, errors.Errorf("%s match-table is nil", baseRoute.AllMatchers)
	}
	baseRoute.Table = table

	excludeColumn, err := getListString(routeConfig, "exclude-column", []string{})
	if err != nil {
		return nil, err
	}
	baseRoute.ExcludeColumn = transList2Map(&excludeColumn)

	includeColumn, err := getListString(routeConfig, "include-column", []string{})
	if err != nil {
		return nil, err
	}
	baseRoute.IncludeColumn = transList2Map(&includeColumn)

	convertColumn, err := getMapString(routeConfig, "convert-column", map[string]string{})
	if err != nil {
		return nil, err
	}
	baseRoute.ConvertColumn = &convertColumn

	return baseRoute, nil
}

func transList2Map(list *[]string) *map[string]string {
	m := &map[string]string{}
	if list == nil || len(*list) <= 0 {
		return m
	}
	for _, v := range *list {
		(*m)[v] = v
	}
	return m
}
