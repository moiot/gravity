package mysqlbatch

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableMatch(t *testing.T) {
	r := require.New(t)

	exp := "a_*"

	// make sure regular expression is a superset of glob style
	rexp, err := regexp.Compile(exp)
	r.NoError(err)

	r.True(rexp.Match([]byte("a_0")))
	r.True(rexp.Match([]byte("a_1")))

}
