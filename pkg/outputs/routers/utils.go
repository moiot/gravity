package routers

import (
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
	if !ok {
		return defaultVal, nil
	}
	valList, ok := val.([]map[string]interface{})
	if !ok {
		return nil, errors.Errorf("%s is invalid", key)
	}
	return valList, nil
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
