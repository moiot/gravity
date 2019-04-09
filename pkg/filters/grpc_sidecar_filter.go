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
	"os/exec"

	log "github.com/sirupsen/logrus"

	hplugin "github.com/hashicorp/go-plugin"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/filters/grpc"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
)

//
// grpc-sidecar filter use grpc service to manipulate the core.Msg.
// Only DmlMsg modificaiton is supported right now.
//
// [[filters]]
// type = "grpc-sidecar"
// [filters.config]
// binary-url = "some url"
// name = "some name"

const (
	GRPCFilterName = "grpc-sidecar"
	BinaryDir      = "./grpc-sidecar"
)

type grpcFilterType struct {
	configData map[string]interface{}
	BaseFilter
	binaryURL string
	name      string
	client    *hplugin.Client
	delegate  core.IFilter
}

func (f *grpcFilterType) Configure(data map[string]interface{}) error {
	// url is the location of the binary
	url, ok := data["binary-url"]
	if !ok {
		return errors.Errorf("empty binary-url")
	}

	// name is the binary name. when we download the binary, we use this name to
	// launch the process
	name, ok := data["name"]
	if !ok {
		return errors.Errorf("empty binary name")
	}

	f.configData = data
	f.binaryURL = url.(string)
	f.name = name.(string)

	err := f.ConfigureMatchers(data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (f *grpcFilterType) Filter(msg *core.Msg) (bool, error) {

	if f.client == nil {
		log.Infof("[grpcFilter] init client")
		fileName, err := utils.GetExecutable(f.binaryURL, BinaryDir, f.name)
		if err != nil {
			return false, errors.Trace(err)
		}

		// start the process
		client := hplugin.NewClient(&hplugin.ClientConfig{
			HandshakeConfig: grpc.HandshakeConfig,
			Plugins:         grpc.PluginMap,
			Cmd:             exec.Command(fileName),
			AllowedProtocols: []hplugin.Protocol{
				hplugin.ProtocolGRPC,
			},
		})

		rpcClient, err := client.Client()
		if err != nil {
			return false, errors.Trace(err)
		}

		raw, err := rpcClient.Dispense(grpc.PluginName)
		if err != nil {
			return false, errors.Trace(err)
		}

		log.Infof("[grpcFilter] configure remote server")
		delegate := raw.(core.IFilter)
		if err := delegate.Configure(f.configData); err != nil {
			return false, errors.Trace(err)
		}

		f.client = client
		f.delegate = delegate
	}

	if !f.Matchers.Match(msg) {
		return true, nil
	}

	// only supports dml msg right now
	if msg.Type != core.MsgDML {
		return true, nil
	}

	return f.delegate.Filter(msg)
}

func (f *grpcFilterType) Close() error {
	f.client.Kill()
	f.client = nil
	f.delegate = nil
	return nil
}

type grpcFilterFactoryType struct{}

func (factory *grpcFilterFactoryType) Configure(_ string, _ map[string]interface{}) error {
	return nil
}

func (factory *grpcFilterFactoryType) NewFilter() core.IFilter {
	return &grpcFilterType{}
}

func init() {
	registry.RegisterPlugin(registry.FilterPlugin, GRPCFilterName, &grpcFilterFactoryType{}, true)
}
