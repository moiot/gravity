package routers

import (
	"github.com/juju/errors"

	"github.com/moiot/gravity/gravity/matchers"
)

type KafkaRoute struct {
	RouteMatchers
	DMLTargetTopic string
	// DDLTargetTopic string
}

func NewKafkaRoutes(configData []map[string]interface{}) ([]*KafkaRoute, error) {
	var kafkaRoutes []*KafkaRoute

	for _, routeConfig := range configData {
		route := KafkaRoute{}
		retMatchers, err := matchers.NewMatchers(routeConfig)
		if err != nil {
			return nil, errors.Trace(err)
		}

		route.AllMatchers = retMatchers

		if dmlTargetTopic, ok := routeConfig["dml-topic"]; !ok {
			route.DMLTargetTopic = ""
		} else {
			dmlTargetTopicString, ok := dmlTargetTopic.(string)
			if !ok {
				return nil, errors.Errorf("dml-topic invalid type")
			}
			route.DMLTargetTopic = dmlTargetTopicString
		}
		// TODO add ddl topic
		// if ddlTargetTopic, ok := routeConfig["ddl-target-topic"]; !ok {
		// 	route.DDLTargetTopic = ""
		// } else {
		// 	ddlTargetTopicString, ok := ddlTargetTopic.(string)
		// 	if !ok {
		// 		return nil, errors.Errorf("ddl-target-topic invalid type")
		// 	}
		// 	route.DDLTargetTopic = ddlTargetTopicString
		// }
		kafkaRoutes = append(kafkaRoutes, &route)
	}
	return kafkaRoutes, nil
}
