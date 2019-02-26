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

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/core/encoding"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/protocol/msgpb"
)

type GRPCServer struct {
	Impl core.IFilter
}

func (m *GRPCServer) Configure(ctx context.Context, req *msgpb.ConfigureRequest) (*msgpb.ConfigureResponse, error) {
	rsp := msgpb.ConfigureResponse{}

	data, err := encoding.PBToDataMap(req.Data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := m.Impl.Configure(data); err != nil {
		return nil, errors.Trace(err)
	}

	return &rsp, nil

}

func (m *GRPCServer) Filter(ctx context.Context, req *msgpb.FilterRequest) (*msgpb.FilterResponse, error) {
	rsp := msgpb.FilterResponse{}

	msg, err := encoding.DecodeMsgFromPB(req.Msg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	continueNext, err := m.Impl.Filter(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pbmsg, err := encoding.EncodeMsgToPB(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rsp.ContinueNext = continueNext
	rsp.Msg = pbmsg
	return &rsp, nil
}
