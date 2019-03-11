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

type GRPCClient struct{ client msgpb.FilterPluginClient }

func (m *GRPCClient) Configure(data map[string]interface{}) error {
	configData, err := encoding.DataMapToPB(data)
	if err != nil {
		return errors.Trace(err)
	}

	req := msgpb.ConfigureRequest{Data: configData}

	rsp, err := m.client.Configure(context.Background(), &req)
	if err != nil {
		return errors.Trace(err)
	}
	if rsp.GetError() != nil {
		return errors.Errorf(rsp.GetError().Value)
	}
	return nil
}

func (m *GRPCClient) Filter(msg *core.Msg) (bool, error) {
	pbmsg, err := encoding.EncodeMsgToPB(msg)
	if err != nil {
		return false, errors.Trace(err)
	}

	req := msgpb.FilterRequest{Msg: pbmsg}
	rsp, err := m.client.Filter(context.Background(), &req)
	if err != nil {
		return false, errors.Trace(err)
	}

	if rsp.GetError() != nil {
		return false, errors.Errorf(rsp.GetError().Value)
	}

	newMsg, err := encoding.DecodeMsgFromPB(rsp.Msg)
	if err != nil {
		return false, errors.Trace(err)
	}

	// We only change the DmlMsg here.
	// Meta data inside the message header keep untouched.
	msg.DmlMsg = newMsg.DmlMsg
	return rsp.GetContinueNext(), nil
}

func (m *GRPCClient) Close() error {
	return nil
}
