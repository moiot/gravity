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

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/protocol/msgpb"
)

type GRPCServer struct {
	Impl core.IFilter
}

func (m *GRPCServer) Configure(context.Context, *msgpb.ConfigureRequest) (*msgpb.ConfigureResponse, error) {

}

func (m *GRPCServer) Filter(context.Context, *msgpb.FilterRequest) (*msgpb.FilterResponse, error) {

}
