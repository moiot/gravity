package filters

import (
	"encoding/json"
)

func newFilterConfigFromJson(s string) (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	err := json.Unmarshal([]byte(s), &cfg)
	return cfg, err
}
