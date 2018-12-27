package encoding

import (
	"encoding/json"

	"github.com/moiot/gravity/pkg/mongo/gtm"

	"github.com/moiot/gravity/pkg/core"
)

type JsonMsg01 struct {
	Version          string  `json:"version"`
	Database         string  `json:"database"`
	Collection       string  `json:"collection"`
	UniqueSourceName string  `json:"unique_source_name"`
	Oplog            *gtm.Op `json:"oplog"`
}

type JsonMsgVersion20 struct {
	Version    string                 `json:"version"`
	Database   string                 `json:"database"`
	Collection string                 `json:"collection"`
	Data       map[string]interface{} `json:"data"`
}

type mongoJsonSerde struct {
}

func (s *mongoJsonSerde) Serialize(msg *core.Msg, version string) ([]byte, error) {
	switch version {
	case Version01:
		jsonMsg := JsonMsg01{}
		jsonMsg.Version = version
		jsonMsg.Database = msg.Oplog.GetDatabase()
		jsonMsg.Collection = msg.Oplog.GetCollection()
		jsonMsg.UniqueSourceName = msg.Host
		jsonMsg.Oplog = msg.Oplog
		return json.Marshal(jsonMsg)
	case Version20Alpha:
		jsonMsg := JsonMsgVersion20{}
		jsonMsg.Version = version
		jsonMsg.Database = msg.Database
		jsonMsg.Collection = msg.Table
		jsonMsg.Data = msg.Oplog.Data
		return json.Marshal(jsonMsg)
	default:
		return nil, nil
	}

}

func (*mongoJsonSerde) Deserialize(b []byte) (core.Msg, error) {
	panic("implement me")
}
