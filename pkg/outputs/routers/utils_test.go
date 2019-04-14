package routers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetString(t *testing.T) {
	r := require.New(t)
	data := map[string]interface{}{
		"a": "a",
		"b": 1,
	}

	val, err := getString(data, "a", "default")
	r.NoError(err)
	r.Equal("a", val)

	val, err = getString(data, "b", "default")
	r.Error(err)
	r.Equal("b is invalid", err.Error())

	val, err = getString(data, "c", "default")
	r.NoError(err)
	r.Equal("default", val)
}

func TestGetBool(t *testing.T) {
	r := require.New(t)
	data := map[string]interface{}{
		"a": true,
		"b": 1,
	}

	val, err := getBool(data, "a", false)
	r.NoError(err)
	r.Equal(true, val)

	val, err = getBool(data, "b", false)
	r.Error(err)
	r.Equal("b is invalid", err.Error())

	val, err = getBool(data, "c", false)
	r.NoError(err)
	r.Equal(false, val)
}
