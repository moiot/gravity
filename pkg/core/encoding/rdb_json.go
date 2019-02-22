package encoding

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/core"
)

type rdbJsonSerde struct {
	timezone time.Location
}

// version 0.1
type MysqlJson_0_1_header struct {
	Version   string `json:"version"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	Type      string `json:"type"`
	Timestamp int64  `json:"ts"`
	TimeZone  string `json:"time_zone"`
	Host      string `json:"host"`
}

type DmlStringMsg_0_1 struct {
	MysqlJson_0_1_header
	Data map[string]*string `json:"data"`
	Old  map[string]*string `json:"old"`
	Pks  map[string]*string `json:"pks,omitempty"`
}

type DDLMsg_0_1 struct {
	MysqlJson_0_1_header
	SQL string `json:"sql"`
}

const timeLayout = "2006-01-02 15:04:05.999999999"

func formatVersion01Data(d map[string]interface{}) map[string]*string {
	ret := make(map[string]*string)
	for k, v := range d {
		if v == nil {
			ret[k] = nil
			continue
		}
		switch v := v.(type) {
		case string:
			ret[k] = new(string)
			*ret[k] = v
		case nil:
			ret[k] = nil
		case time.Time:
			ret[k] = new(string)
			*ret[k] = v.Format(timeLayout)
		default:
			ret[k] = new(string)
			*ret[k] = fmt.Sprint(v)
		}
	}
	return ret
}

func serializeVersion01JsonMsg(msg *core.Msg) ([]byte, error) {
	header := MysqlJson_0_1_header{
		Version:   Version01,
		Database:  msg.Database,
		Table:     msg.Table,
		Timestamp: msg.Timestamp.Unix(),
		TimeZone:  "Asia/Shanghai",
		Host:      msg.Host,
	}
	if msg.DmlMsg != nil {
		dmlStringMsg := &DmlStringMsg_0_1{
			MysqlJson_0_1_header: header,
			Data:                 formatVersion01Data(msg.DmlMsg.Data),
			Old:                  formatVersion01Data(msg.DmlMsg.Old),
			Pks:                  formatVersion01Data(msg.DmlMsg.Pks),
		}
		switch msg.DmlMsg.Operation {
		case core.Insert:
			dmlStringMsg.MysqlJson_0_1_header.Type = "insert"
		case core.Update:
			dmlStringMsg.MysqlJson_0_1_header.Type = "update"
		case core.Delete:
			dmlStringMsg.MysqlJson_0_1_header.Type = "delete"
		}

		return json.Marshal(dmlStringMsg)
	}

	if msg.DdlMsg != nil {
		header.Type = "ddl"
		return json.Marshal(DDLMsg_0_1{
			MysqlJson_0_1_header: header,
			SQL:                  msg.DdlMsg.Statement,
		})
	}
	return nil, errors.Errorf("invalid msg")

}

// version 2.0
type MysqlJson_2_0_header struct {
	Version  string `json:"version"`
	Database string `json:"database"`
	Table    string `json:"table"`
	Type     string `json:"type"`
}

type DmlJson_2_0 struct {
	MysqlJson_2_0_header
	Data map[string]interface{}
	Old  map[string]interface{}
	Pks  map[string]interface{}
}

type DDLJson_2_0 struct {
	MysqlJson_2_0_header
	SQL string `json:"sql"`
}

func formatVersion20Data(d map[string]interface{}) map[string]interface{} {
	for k, v := range d {
		switch v := v.(type) {
		case time.Time:
			d[k] = v.Format(time.RFC3339Nano)
		}
	}
	return d
}

func serializeVersion20JsonMsg(msg *core.Msg) ([]byte, error) {
	header := MysqlJson_2_0_header{
		Version:  Version20Alpha,
		Database: msg.Database,
		Table:    msg.Table,
	}

	if msg.DmlMsg != nil {
		dmlMsg := &DmlJson_2_0{
			MysqlJson_2_0_header: header,
			Data:                 formatVersion20Data(msg.DmlMsg.Data),
			Old:                  formatVersion20Data(msg.DmlMsg.Old),
			Pks:                  formatVersion20Data(msg.DmlMsg.Pks),
		}
		switch msg.DmlMsg.Operation {
		case core.Insert:
			dmlMsg.MysqlJson_2_0_header.Type = "insert"
		case core.Update:
			dmlMsg.MysqlJson_2_0_header.Type = "update"
		case core.Delete:
			dmlMsg.MysqlJson_2_0_header.Type = "delete"
		}
		return json.Marshal(dmlMsg)
	}

	if msg.DdlMsg != nil {
		header.Type = "ddl"
		ddlMsg := DDLJson_2_0{
			MysqlJson_2_0_header: header,
			SQL:                  msg.DdlMsg.Statement,
		}
		return json.Marshal(ddlMsg)
	}
	return nil, errors.Errorf("invalid msg")
}

func (s *rdbJsonSerde) Serialize(msg *core.Msg, version string) ([]byte, error) {
	switch version {
	case Version01:
		return serializeVersion01JsonMsg(msg)
	case Version20Alpha:
		return serializeVersion20JsonMsg(msg)

	default:
		return nil, errors.Errorf("invalid version")
	}
}

func (*rdbJsonSerde) Deserialize(b []byte) (core.Msg, error) {
	panic("implement me")
}
