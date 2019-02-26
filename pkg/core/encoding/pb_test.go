/*
 *
 * // Copyright 2019 , Beijing Mobike Technology Co., Ltd.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package encoding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDataMapToPB(t *testing.T) {
	r := require.New(t)

	currentTime := time.Now()
	data := map[string]interface{}{
		"a": "v1",
		"b": 2,
		"c": 1.234,
		"d": currentTime,
		"e": true,
	}

	pbMap, err := DataMapToPB(data)
	r.NoError(err)
	r.Equal(len(data), len(pbMap))

	newData, err := PBToDataMap(pbMap)
	r.NoError(err)

	for k, v := range data {
		t, ok := v.(time.Time)
		if ok {
			kt := newData[k].(time.Time)
			r.True(t.Equal(kt))
		} else {
			r.EqualValues(v, newData[k])
		}
	}
}
