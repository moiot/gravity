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
	}

	return &pb, nil
}

func EncodeMsgToPB(msg *core.Msg) (*msgpb.Msg, error) {

	pb, err := EncodeMsgHeaderToPB(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// only supports dml message right now
	if msg.DmlMsg != nil {
		return nil, errors.Errorf("dml is nil")
	}

	data, err := DataMapToPB(msg.DmlMsg.Data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pb.DmlMsg = &msgpb.DMLMsg{
		Data: data,
	}

	return pb, nil
}
