package routers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/matchers"
)

type MySQLRoute struct {
	RouteMatchers
	TargetSchema string
	TargetTable  string
}

func (route *MySQLRoute) GetTarget(msgSchema string, msgTable string) (string, string) {
	var targetSchema string
	var targetTable string

	if route.TargetSchema == "*" || route.TargetSchema == "" {
		targetSchema = msgSchema
	} else {
		targetSchema = route.TargetSchema
	}

	if route.TargetTable == "*" || route.TargetTable == "" {
		targetTable = msgTable
	} else {
		targetTable = route.TargetTable
	}
	return targetSchema, targetTable
}

type MySQLRouter []*MySQLRoute

func (r MySQLRouter) Exists(msg *core.Msg) bool {
	for i := range r {
		if r[i].Match(msg) {
			return true
		}
	}
	return false
}

func NewMySQLRoutes(configData []map[string]interface{}) ([]*MySQLRoute, error) {
	var mysqlRoutes []*MySQLRoute

	// init matchers
	if len(configData) == 0 {
		// default match all and target to the same as source
		return []*MySQLRoute{{}}, nil
	} else {
		for _, routeConfig := range configData {
			route := MySQLRoute{}
			retMatchers, err := matchers.NewMatchers(routeConfig)
			if err != nil {
				return nil, errors.Trace(err)
			}

			route.AllMatchers = retMatchers

			if targetSchema, ok := routeConfig["target-schema"]; !ok {
				route.TargetSchema = "*"
			} else {
				if targetSchemaString, ok := targetSchema.(string); !ok {
					return nil, errors.Errorf("target-schema invalid type")
				} else {
					route.TargetSchema = targetSchemaString
				}
			}

			if targetTable, ok := routeConfig["target-table"]; !ok {
				route.TargetTable = "*"
			} else {
				if targetTableString, ok := targetTable.(string); !ok {
					return nil, errors.Errorf("target-table invalid type")
				} else {
					route.TargetTable = targetTableString
				}
			}
			mysqlRoutes = append(mysqlRoutes, &route)
		}
	}
	return mysqlRoutes, nil
}
