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

package filters

import (
	"runtime"
	"testing"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestGrpcFilterFactoryType(t *testing.T) {
	r := require.New(t)

	var binaryURL string
	if runtime.GOOS == "darwin" {
		binaryURL = "https://github.com/moiot/gravity-grpc-sidecar-filter-example/releases/download/v0.2/gravity-grpc-sidecar-filter-example.darwin"
	} else if runtime.GOOS == "linux" {
		binaryURL = "https://github.com/moiot/gravity-grpc-sidecar-filter-example/releases/download/v0.2/gravity-grpc-sidecar-filter-example.linux"
	} else {
		r.FailNow("runtime not supported")
	}
	data := []config.GenericConfig{
		{
			Type: "grpc-sidecar",
			Config: map[string]interface{}{
				"binary-url":   binaryURL,
				"name":         "test-grpc-sidecar",
				"match-schema": "test",
			},
		},
	}

	filters, err := NewFilters(data)
	r.NoError(err)

	defer func() {
		for _, f := range filters {
			f.Close()
		}
	}()

	r.Equal(1, len(filters))

	f1 := filters[0]

	msg := core.Msg{
		Database: "test",
		Type:     core.MsgDML,
		DmlMsg: &core.DMLMsg{
			Data: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
			},
		},
	}

	continueNext, err := f1.Filter(&msg)
	r.NoError(err)
	r.True(continueNext)

	for _, v := range msg.DmlMsg.Data {
		r.Equal("hello grpc", v)
	}
}
