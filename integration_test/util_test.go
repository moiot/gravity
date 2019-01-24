package integration_test

import "encoding/json"

func struct2Map(i interface{}) (ret map[string]interface{}) {
	t, _ := json.Marshal(i)
	_ = json.Unmarshal(t, &ret)
	return
}
