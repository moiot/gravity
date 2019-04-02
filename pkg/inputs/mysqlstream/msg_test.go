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

package mysqlstream

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateDataHashes(t *testing.T) {
	r := require.New(t)

	t.Run("check the number of hashes", func(tt *testing.T) {
		cases := []struct {
			name              string
			schema            string
			table             string
			uniqKeys          map[string][]string
			oldData           map[string]interface{}
			newData           map[string]interface{}
			expectedNumHashes int
		}{
			{
				"old data is nil, and unique key is nil",
				"test",
				"test",
				nil,
				nil,
				map[string]interface{}{"a": 1},
				0,
			},
			{
				"old data is nil, and unique key is NOT nil",
				"test",
				"test",
				map[string][]string{
					"PRIM": {"a"},
				},
				nil,
				map[string]interface{}{"a": 1},
				1,
			},
			{
				"old data is not nil, and unique key is nil",
				"test",
				"test",
				nil,
				map[string]interface{}{"a": 1},
				map[string]interface{}{"a": 2},
				0,
			},
			{
				"old data is not nil, and unique key is NOT nil, and unique key Not changed",
				"test",
				"test",
				map[string][]string{"PRIM": {"a"}},
				map[string]interface{}{"a": 1, "b": 1},
				map[string]interface{}{"a": 1, "b": 2},
				1,
			},
			{
				"only one unique key, one unique key changed",
				"test",
				"test",
				map[string][]string{"PRIM": {"a"}},
				map[string]interface{}{"a": 1, "b": 2},
				map[string]interface{}{"a": 2, "b": 3},
				2,
			},
			{
				"multiple unique key, No unique key change",
				"test",
				"test",
				map[string][]string{"PRIM": {"a"}, "idx_b_c": {"b", "c"}},
				map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5},
				map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 6},
				2,
			},
			{
				"multiple unique key, one unique key changed",
				"test",
				"test",
				map[string][]string{"PRIM": {"a"}, "idx_b_c": {"b", "c"}},
				map[string]interface{}{"a": 1, "b": 2, "c": 3},
				map[string]interface{}{"a": 1, "b": 2, "c": 4},
				3,
			},
			{
				"multiple unique kye, multiple unique key changed",
				"test",
				"test",
				map[string][]string{"PRIM": {"a"}, "idx_b_c": {"b", "c"}, "idx_d": {"d"}},
				map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5, "e": 6},
				map[string]interface{}{"a": 2, "b": 2, "c": 4, "d": 5, "e": 7},
				5,
			},
		}

		for _, c := range cases {
			hashed := GenerateDataHashes(c.schema, c.table, c.uniqKeys, c.oldData, c.newData)
			r.Equalf(c.expectedNumHashes, len(hashed), c.name)
		}
	})

	t.Run("check hash values", func(tt *testing.T) {
		// multiple unique kye, multiple unique key changed
		h1 := GenerateDataHashes(
			"test",
			"test",
			map[string][]string{MySQLPrimaryKeyName: {"a"}, "idx_b_c": {"b", "c"}, "idx_d": {"d"}},
			map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5, "e": 6},
			map[string]interface{}{"a": 2, "b": 2, "c": 4, "d": 5, "e": 7},
		)

		h2 := GenerateDataHashes(
			"test",
			"test",
			map[string][]string{MySQLPrimaryKeyName: {"a"}, "idx_b_c": {"b", "c"}, "idx_d": {"d"}},
			map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5, "e": 8},
			map[string]interface{}{"a": 2, "b": 2, "c": 4, "d": 5, "e": 9},
		)

		r.Equal(len(h1), len(h2))

		// new primary key is always the first hash
		r.Equal(h1[0].H, h2[0].H)

		sort.SliceStable(h1, func(i, j int) bool {
			return h1[i].H < h1[j].H
		})
		sort.SliceStable(h2, func(i, j int) bool {
			return h2[i].H < h2[j].H
		})
		for i := range h1 {
			r.Equal(h1[i].Name, h2[i].Name)
			r.Equal(h1[i].H, h2[i].H)
		}

		// the same change return the same hash
		h3 := GenerateDataHashes(
			"test",
			"test",
			map[string][]string{MySQLPrimaryKeyName: {"a"}, "idx_b_c": {"b", "c"}, "idx_d": {"d"}},
			map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5, "e": 6},
			map[string]interface{}{"a": 2, "b": 2, "c": 4, "d": 5, "e": 7},
		)

		h4 := GenerateDataHashes(
			"test",
			"test",
			map[string][]string{MySQLPrimaryKeyName: {"a"}, "idx_b_c": {"b", "c"}, "idx_d": {"d"}},
			map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 5, "e": 6},
			map[string]interface{}{"a": 2, "b": 2, "c": 4, "d": 5, "e": 7},
		)

		// new primary key is always the first hash
		r.Equal(h3[0].H, h4[0].H)

		sort.SliceStable(h3, func(i, j int) bool {
			return h3[i].H < h3[j].H
		})
		sort.SliceStable(h4, func(i, j int) bool {
			return h4[i].H < h4[j].H
		})

		for i := range h4 {
			r.Equal(h4[i].Name, h3[i].Name)
			r.Equal(h4[i].H, h3[i].H)
		}
	})

}
