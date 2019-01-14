package mysqlbatch

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
)

func TestStringOrStringSlice(t *testing.T) {
	s := `[{"schema": "t", "table":"123"}, {"schema":"t", "table":["1","2","3"]}]`
	var m []map[string]interface{}
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		t.Fatal(err)
	}

	var ret []TableConfig
	err = mapstructure.WeakDecode(m, &ret)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(ret)
	require.Equal(t, []string{"123"}, ret[0].Table)
	require.Equal(t, []string{"1", "2", "3"}, ret[1].Table)
}
