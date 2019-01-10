package filters

import (
	"encoding/json"

	"github.com/juju/errors"
)

func newFilterConfigFromJson(s string) (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	err := json.Unmarshal([]byte(s), &cfg)
	return cfg, err
}

func arrayInterfaceToArrayString(a []interface{}) ([]string, error) {
	aStrings := make([]string, len(a))
	for i, c := range a {
		name, ok := c.(string)
		if !ok {
			return nil, errors.Errorf("should be an array of string")
		}
		aStrings[i] = name
	}
	return aStrings, nil
}
