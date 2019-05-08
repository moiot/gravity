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
	"github.com/gogo/protobuf/types"
	"github.com/juju/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

func DataMapToPB(data map[string]interface{}) (map[string]*types.Any, error) {
	res := make(map[string]*types.Any)

	for k, v := range data {
		a, err := InterfaceValueToPB(v)
		if err != nil {
			return nil, errors.Annotatef(err, "key: %v", k)
		}
		res[k] = a
	}
	return res, nil
}

func PBToDataMap(data map[string]*types.Any) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	for k, v := range data {
		d, err := PbToInterface(v)
		if err != nil {
			return nil, errors.Annotatef(err, "key: %v", k)
		}
		res[k] = d
	}
	return res, nil
}

func InterfaceValueToPB(v interface{}) (*types.Any, error) {
	switch v := v.(type) {
	case int8:
		return types.MarshalAny(&types.Int32Value{Value: int32(v)})
	case int16:
		return types.MarshalAny(&types.Int32Value{Value: int32(v)})
	case int:
		return types.MarshalAny(&types.Int64Value{Value: int64(v)})
	case int32:
		return types.MarshalAny(&types.Int64Value{Value: int64(v)})
	case int64:
		return types.MarshalAny(&types.Int64Value{Value: int64(v)})
	case uint8:
		return types.MarshalAny(&types.UInt32Value{Value: uint32(v)})
	case uint16:
		return types.MarshalAny(&types.UInt32Value{Value: uint32(v)})
	case uint32:
		return types.MarshalAny(&types.UInt32Value{Value: v})
	case uint64:
		return types.MarshalAny(&types.UInt64Value{Value: v})
	case string:
		return types.MarshalAny(&types.StringValue{Value: v})
	case time.Time:
		t, err := types.TimestampProto(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return types.MarshalAny(t)
	case primitive.DateTime:
		// bson primitive DateTime
		t := time.Unix(int64(v)/1000, int64(v)%1000*1000000)
		pbt, err := types.TimestampProto(t)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return types.MarshalAny(pbt)
	case float32:
		return types.MarshalAny(&types.FloatValue{Value: v})
	case float64:
		return types.MarshalAny(&types.DoubleValue{Value: v})
	case bool:
		return types.MarshalAny(&types.BoolValue{Value: v})
	case []byte:
		return types.MarshalAny(&types.BytesValue{Value: v})
	case nil:
		return types.MarshalAny(&types.Empty{})
	default:
		return nil, errors.Errorf("unknown type: %v", reflect.TypeOf(v))
	}
}


func PbToInterface(v *types.Any) (interface{}, error) {
	typeUrl := v.GetTypeUrl()
	switch typeUrl {
	case "type.googleapis.com/google.protobuf.Int32Value":
		res := types.Int32Value{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.Int64Value":
		res := types.Int64Value{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.UInt32Value":
		res := types.UInt32Value{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.UInt64Value":
		res := types.UInt64Value{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "google.protobuf.FloatValue":
		res := types.FloatValue{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.DoubleValue":
		res := types.DoubleValue{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.BoolValue":
		res := types.BoolValue{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.StringValue":
		res := types.StringValue{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.BytesValue":
		res := types.BytesValue{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		return res.Value, nil
	case "type.googleapis.com/google.protobuf.Timestamp":
		res := types.Timestamp{}
		if err := types.UnmarshalAny(v, &res); err != nil {
			return nil, errors.Trace(err)
		}
		t, err := types.TimestampFromProto(&res)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return t, nil
	case "type.googleapis.com/google.protobuf.Empty":
		return nil, nil
	default:
		return nil, errors.Errorf("unknown type: %v", v)
	}

	panic("should not happens here")
}