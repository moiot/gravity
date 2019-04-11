package routers

import "github.com/juju/errors"

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
