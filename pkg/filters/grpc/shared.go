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

package grpc

import (
	"context"

	hplugin "github.com/hashicorp/go-plugin"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/protocol/msgpb"
	"google.golang.org/grpc"
)

const PluginName = "filter_grpc"

var (
	HandshakeConfig = hplugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "BASIC_PLUGIN",
		MagicCookieValue: "hello",
	}

	PluginMap = map[string]hplugin.Plugin{
		PluginName: &FilterGRPCPlugin{},
	}
)

type FilterGRPCPlugin struct {
	hplugin.Plugin
	Impl core.IFilter
}

func (p *FilterGRPCPlugin) GRPCServer(broker *hplugin.GRPCBroker, s *grpc.Server) error {
	msgpb.RegisterFilterPluginServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

func (p *FilterGRPCPlugin) GRPCClient(ctx context.Context, broker *hplugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{client: msgpb.NewFilterPluginClient(c)}, nil
}
