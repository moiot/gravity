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

package stdout

import (
	"fmt"
	"os"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const Name = "stdout"

type stdOutput struct {
	pipelineName string
}

func (plugin *stdOutput) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName
	return nil
}

func (plugin *stdOutput) GetRouter() core.Router {
	return core.EmptyRouter{}
}

func (plugin *stdOutput) Start() error {
	return nil
}

func (plugin *stdOutput) Close() {
}

func (plugin *stdOutput) Execute(msgs []*core.Msg) error {
	for _, msg := range msgs {
		if msg.DmlMsg != nil {
			fmt.Fprintf(os.Stdout, "schema: %v, table: %v, dml op: %v, data: %v\n", msg.Database, msg.Table, msg.DmlMsg.Operation, msg.DmlMsg.Data)
		} else if msg.DdlMsg != nil {
			fmt.Fprintf(os.Stdout, "schema: %v, table: %v, ddl: %v\n", msg.Database, msg.Table, msg.DdlMsg.Statement)
		} else {
			fmt.Fprintf(os.Stdout, "schema: %v, table: %v, type: %v", msg.Database, msg.Table, msg.Type)
		}

	}
	return nil
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &stdOutput{}, false)
}
