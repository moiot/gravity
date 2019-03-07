package mysqlbatch

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/moiot/gravity/pkg/utils"

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

func TestDetectScanColumn(t *testing.T) {
	r := require.New(t)
	t.Run("multiple primary", func(tt *testing.T) {
		testDBName := utils.TestCaseMd5Name(tt)
		db := mysql_test.MustSetupSourceDB(testDBName)

		col, err := DetectScanColumns(db, testDBName, mysql_test.TestScanColumnTableCompositePrimary, 1000, 10000)
		r.NoError(err)
		r.Equal([]string{"id", "name"}, col)
	})
	t.Run(" single the primary", func(tt *testing.T) {
		testDBName := utils.TestCaseMd5Name(tt)
		db := mysql_test.MustSetupSourceDB(testDBName)
		col, err := DetectScanColumns(db, testDBName, mysql_test.TestScanColumnTableIdPrimary, 1000, 10000)
		r.Nil(err)
		r.Equal([]string{"id"}, col)
	})

	t.Run("uniq index", func(tt *testing.T) {
		testDBName := utils.TestCaseMd5Name(tt)
		db := mysql_test.MustSetupSourceDB(testDBName)
		col, err := DetectScanColumns(db, testDBName, mysql_test.TestScanColumnTableUniqueIndexEmailString, 1000, 10000)
		r.Nil(err)
		r.Equal([]string{"email"}, col)
	})
}
