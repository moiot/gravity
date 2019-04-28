package encoding

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/protocol/msgpb"

	"github.com/moiot/gravity/pkg/core"
)

const (
	Version01      = "0.1"
	Version20Alpha = "2.0.alpha"
)

type Encoder interface {
	Serialize(msg *core.Msg, version string) ([]byte, error)
	Deserialize(b []byte) (core.Msg, error)
}

func NewEncoder(input string, format string) Encoder {
	switch input {
	case "mysql":
		switch format {
		case "json":
			return &rdbJsonSerde{timezone: *time.Local}
		case "pb":
		}

	case "mongo":
		switch format {
		case "json":
			return &mongoJsonSerde{}
		case "pb":
		}
	}

	panic(fmt.Sprintf("no serde find for input %s, format %s", input, format))
}

func EncodeMsgHeaderToPB(msg *core.Msg) (*msgpb.Msg, error) {
	timestamp, err := types.TimestampProto(msg.Timestamp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pb := msgpb.Msg{
		Database:  msg.Database,
		Table:     msg.Table,
		Timestamp: timestamp,
		MsgType:   string(msg.Type),
	}

	return &pb, nil
}

func DecodeMsgHeaderFromPB(pbmsg *msgpb.Msg) (*core.Msg, error) {
	msg := core.Msg{
		Database: pbmsg.Database,
		Table:    pbmsg.Table,
		Type:     core.MsgType(pbmsg.MsgType),
	}
	t, err := types.TimestampFromProto(pbmsg.Timestamp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg.Timestamp = t
	return &msg, nil
}

func EncodeMsgToPB(msg *core.Msg) (*msgpb.Msg, error) {

	pb, err := EncodeMsgHeaderToPB(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// only supports dml message right now
	if msg.DmlMsg == nil {
		return nil, errors.Errorf("dml is nil")
	}

	data, err := DataMapToPB(msg.DmlMsg.Data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pks, err := DataMapToPB(msg.DmlMsg.Pks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pb.DmlMsg = &msgpb.DMLMsg{
		Op:   string(msg.DmlMsg.Operation),
		Data: data,
		Pks:  pks,
	}

	return pb, nil
}

func DecodeMsgFromPB(pbmsg *msgpb.Msg) (*core.Msg, error) {
	msg, err := DecodeMsgHeaderFromPB(pbmsg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg.DmlMsg = &core.DMLMsg{
		Operation: core.DMLOp(pbmsg.DmlMsg.Op),
		Data:      make(map[string]interface{}),
		Pks:       make(map[string]interface{}),
	}

	data, err := PBToDataMap(pbmsg.DmlMsg.Data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pks, err := PBToDataMap(pbmsg.DmlMsg.Pks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg.DmlMsg.Data = data
	msg.DmlMsg.Pks = pks
	return msg, nil
}
