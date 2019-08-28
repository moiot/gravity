package routers

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
)

const (
	EsModelOneModeObject = int64(1)
	EsModelOneModeExtend = int64(2)

	EsTypeMappingObject = "object"
	EsTypeMappingNested = "nested"
)

type EsModelBaseRoute struct {
	RouteMatchers
	DataBase         string
	Table            string
	PkColumn         string
	ConvertColumn    *map[string]string
	ExcludeColumn    *[]string
	IncludeColumn    *[]string
	IndexTypeMapping *map[string]string
}

type EsModelOneMoreRoute struct {
	EsModelBaseRoute
	FkColumn     string
	PropertyName string
}

type EsModelOneOneRoute struct {
	EsModelOneMoreRoute
	Mode        int64
	PropertyPre string
}

type EsModelRoute struct {
	IndexName string
	TypeName  string
	EsModelBaseRoute
	IgnoreNoPrimaryKey bool

	OneOne  *[]*EsModelOneOneRoute
	OneMore *[]*EsModelOneMoreRoute
}

type EsModelRouter []*EsModelRoute

func (r EsModelRouter) Exists(msg *core.Msg) bool {
	_, ok := r.Match(msg)
	return ok
}

func match(database string, table string, msg *core.Msg) bool {
	fmt.Printf("%s, %s, %v \n", database, table, msg)
	if database == msg.Database && table == msg.Table {
		return true
	}
	return false
}

func (r EsModelRouter) Match(msg *core.Msg) (*[]*EsModelRoute, bool) {
	routes := make([]*EsModelRoute, 0, 3)
	// 不做模糊匹配
	fmt.Printf(" %v \n", r)
	for _, route := range r {
		if match(route.DataBase, route.Table, msg) {
			routes = append(routes, route)
			continue
		}
		mtype := false
		if route.OneOne != nil {
			for _, r := range *route.OneOne {
				if match(r.DataBase, r.Table, msg) {
					routes = append(routes, route)
					mtype = true
					break
				}
			}
		}
		if mtype {
			continue
		}
		if route.OneMore != nil {
			for _, r := range *route.OneMore {
				if match(r.DataBase, r.Table, msg) {
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

		moreRouters, err := NewEsModelOneMoreRoutes(routeConfig)
		if err != nil {
			return nil, err
		}
		route.OneMore = &moreRouters

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

		moreRouter, err := NewEsModelOneMoreRoute(v, &oneRouter.EsModelOneMoreRoute)
		if err != nil {
			return nil, err
		}
		oneRouter.EsModelOneMoreRoute = *moreRouter

		mode, err := getInt64(v, "mode", EsModelOneModeObject)
		if err != nil {
			return nil, err
		}
		oneRouter.Mode = mode

		ppre, err := getString(v, "property-pre", "id")
		if err != nil {
			return nil, err
		}
		oneRouter.PropertyPre = ppre

		oneRouters = append(oneRouters, oneRouter)
	}
	return oneRouters, nil
}

func NewEsModelOneMoreRoutes(routeConfig map[string]interface{}) ([]*EsModelOneMoreRoute, error) {

	ones, err := getListMap(routeConfig, "one-more", []map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	moreCount := len(ones)
	moreRouters := make([]*EsModelOneMoreRoute, 0, moreCount)
	for _, v := range ones {
		moreRouter := &EsModelOneMoreRoute{}
		moreRouter, err := NewEsModelOneMoreRoute(v, moreRouter)
		if err != nil {
			return nil, err
		}
		moreRouters = append(moreRouters, moreRouter)
	}
	return moreRouters, nil
}

func NewEsModelOneMoreRoute(routeConfig map[string]interface{}, moreRoute *EsModelOneMoreRoute) (*EsModelOneMoreRoute, error) {

	baseRouter, err := NewEsModelBaseRoute(routeConfig, &moreRoute.EsModelBaseRoute)
	if err != nil {
		return nil, err
	}
	moreRoute.EsModelBaseRoute = *baseRouter

	fkColumn, err := getString(routeConfig, "fk-column", "")
	if err != nil {
		return nil, err
	}
	if fkColumn == "" {
		return nil, errors.Errorf("%s fk-column is nil", moreRoute.AllMatchers)
	}
	moreRoute.FkColumn = fkColumn

	pname, err := getString(routeConfig, "property-name", "")
	if err != nil {
		return nil, err
	}
	moreRoute.PropertyName = pname

	return moreRoute, nil
}

func NewEsModelBaseRoute(routeConfig map[string]interface{}, baseRoute *EsModelBaseRoute) (*EsModelBaseRoute, error) {

	baseRoute.PkColumn = ""
	baseRoute.IndexTypeMapping = &map[string]string{}

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
	baseRoute.ExcludeColumn = &excludeColumn

	includeColumn, err := getListString(routeConfig, "include-column", []string{})
	if err != nil {
		return nil, err
	}
	baseRoute.IncludeColumn = &includeColumn

	convertColumn, err := getMapString(routeConfig, "convert-column", map[string]string{})
	if err != nil {
		return nil, err
	}
	baseRoute.ConvertColumn = &convertColumn

	return baseRoute, nil
}
