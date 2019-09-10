package routers

import (
	"encoding/json"
	"github.com/juju/errors"
)

func getString(data map[string]interface{}, key, defaultVal string) (string, error) {
	val, ok := data[key]
	if !ok {
		return defaultVal, nil
	}
	valString, ok := val.(string)
	if !ok {
		return "", errors.Errorf("%s is invalid", key)
	}
	return valString, nil
}

func getInt(data map[string]interface{}, key string, defaultVal int) (int, error) {
	val, ok := data[key]
	if !ok {
		return defaultVal, nil
	}
	valInt, ok := val.(int)
	if !ok {
		return defaultVal, errors.Errorf("%s is invalid", key)
	}
	return valInt, nil
}

func getInt64(data map[string]interface{}, key string, defaultVal int64) (int64, error) {
	val, ok := data[key]
	if !ok {
		return defaultVal, nil
	}
	switch val.(type) {
	case int64:
		return val.(int64), nil
	case float64:
		return int64(val.(float64)), nil
	case int32:
		return int64(val.(int32)), nil
	case float32:
		return int64(val.(float32)), nil
	}
	valInt, ok := val.(int64)
	if !ok {
		return defaultVal, errors.Errorf("%s is invalid", key)
	}
	return valInt, nil
}

func getBool(data map[string]interface{}, key string, defaultVal bool) (bool, error) {
	val, ok := data[key]
	if !ok {
		return defaultVal, nil
	}
	valBool, ok := val.(bool)
	if !ok {
		return false, errors.Errorf("%s is invalid", key)
	}
	return valBool, nil
}

func getListString(data map[string]interface{}, key string, defaultVal []string) ([]string, error) {
	val, ok := data[key]
	if !ok || val == nil {
		return defaultVal, nil
	}
	vs, ok := val.([]interface{})
	if !ok {
		return nil, errors.Errorf("%s is invalid", key)
	}
	valList := make([]string, 0, len(vs))
	for _, v := range vs {
		valList = append(valList, v.(string))
	}
	return valList, nil
}

func getListMap(data map[string]interface{}, key string, defaultVal []map[string]interface{}) ([]map[string]interface{}, error) {
	val, ok := data[key]
	if !ok || val == nil {
		return defaultVal, nil
	}
	jsonStr, err := ToJson(val)
	if err != nil {
		return defaultVal, err
	}
	valMap := &[]map[string]interface{}{}
	err = FromJson(jsonStr, valMap)
	if err != nil {
		return defaultVal, err
	}
	return *valMap, nil
}

func getMapString(data map[string]interface{}, key string, defaultVal map[string]string) (map[string]string, error) {
	val, ok := data[key]
	if !ok {
		return defaultVal, nil
	}

	vals, ok := val.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("%s is invalid", key)
	}
	valMap := map[string]string{}
	for k, v := range vals {
		valMap[k] = v.(string)
	}

	return valMap, nil
}

func ToJson(obj interface{}) (string, error) {
	bs, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func FromJson(jsonStr string, obj interface{}) error {
	return json.Unmarshal([]byte(jsonStr), obj)
}
