package encoding

import (
	"fmt"
	"time"

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
