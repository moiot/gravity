package utils

import (
	"reflect"

	"github.com/juju/errors"
)

// arg can be slice of any type
func CastToSlice(arg interface{}) (out []interface{}, ok bool) {
	slice, success := TakeArg(arg, reflect.Slice)
	if !success {
		ok = false
		return
	}
	c := slice.Len()
	out = make([]interface{}, c)
	for i := 0; i < c; i++ {
		out[i] = slice.Index(i).Interface()
	}
	return out, true
}

func TakeArg(arg interface{}, kind reflect.Kind) (val reflect.Value, ok bool) {
	val = reflect.ValueOf(arg)
	if val.Kind() == kind {
		ok = true
	}
	return
}

func CastSliceInterfaceToSliceString(a []interface{}) ([]string, error) {
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
