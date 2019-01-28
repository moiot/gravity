package integration_test

import "encoding/json"

func struct2Map(i interface{}) (ret map[string]interface{}) {
	t, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(t, &ret)
	if err != nil {
		panic(err)
	}
	return
}
