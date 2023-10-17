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
	"time"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/outputs/routers"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/registry"
)

const Name = "stdout"

type StdoutPluginConfig struct {
	Routes     []map[string]interface{} `mapstructure:"routes"  json:"routes"`
	DisableLog bool                     `mapstructure:"disable-log" json:"disable-log"`
}

type stdOutput struct {
	pipelineName string
	disableLog   bool
	routes       []*routers.MySQLRoute
}

func (plugin *stdOutput) Configure(pipelineName string, data map[string]interface{}) error {
	plugin.pipelineName = pipelineName

	pluginConfig := StdoutPluginConfig{}
	err := mapstructure.Decode(data, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	plugin.disableLog = pluginConfig.DisableLog
	// init routes
	plugin.routes, err = routers.NewMySQLRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (plugin *stdOutput) GetRouter() core.Router {
	return routers.MySQLRouter(plugin.routes)
}

func (plugin *stdOutput) Start() error {
	return nil
}

func (plugin *stdOutput) Close() {
}

func (plugin *stdOutput) Execute(msgs []*core.Msg) error {
	for _, msg := range msgs {

		matched := false
		for _, route := range plugin.routes {
			if route.Match(msg) {
				matched = true
				break
			}
		}

		if !matched {
			continue
		}

		if !plugin.disableLog {
			if msg.DmlMsg != nil {
				fmt.Fprintf(os.Stdout, "[%s] schema: %v, table: %v, dml op: %v, data: %v\n", time.Now(), msg.Database, msg.Table, msg.DmlMsg.Operation, msg.DmlMsg.Data)
			} else if msg.DdlMsg != nil {
				fmt.Fprintf(os.Stdout, "[%s] schema: %v, table: %v, ddl: %v\n", time.Now(), msg.Database, msg.Table, msg.DdlMsg.Statement)
			} else {
				fmt.Fprintf(os.Stdout, "[%s] schema: %v, table: %v, type: %v", time.Now(), msg.Database, msg.Table, msg.Type)
			}
		}
	}
	return nil
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &stdOutput{}, false)
}
