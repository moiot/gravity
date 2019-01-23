package helper

import (
	"fmt"

	"github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/utils"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type SourceProbeCfg struct {
	SourceMySQL *utils.DBConfig `mapstructure:"mysql"json:"mysql"`
	Annotation  string          `mapstructure:"annotation"json:"annotation"`
}

type BinlogPositions struct {
	CurrentPosition *utils.MySQLBinlogPosition `json:"current_position"`
	StartPosition   *utils.MySQLBinlogPosition `json:"start_position"`
}

func SerializeBinlogPositions(position *BinlogPositions) (string, error) {
	s, err := myJson.MarshalToString(position)
	if err != nil {
		return "", errors.Trace(err)
	}

	return s, nil
}

func DeserializeBinlogPositions(value string) (*BinlogPositions, error) {
	position := BinlogPositions{}
	if err := myJson.UnmarshalFromString(value, &position); err != nil {
		return nil, errors.Trace(err)
	}
	return &position, nil
}
func GetProbCfg(sourceProbeCfg *SourceProbeCfg, sourceDBCfg *utils.DBConfig) (*utils.DBConfig, string) {
	var probeDBCfg *utils.DBConfig
	var probeAnnotation string

	if sourceProbeCfg != nil {
		if sourceProbeCfg.SourceMySQL != nil {
			probeDBCfg = sourceProbeCfg.SourceMySQL
		} else {
			probeDBCfg = sourceDBCfg
		}
		probeAnnotation = sourceProbeCfg.Annotation
	} else {
		probeDBCfg = sourceDBCfg
	}

	if probeAnnotation != "" {
		probeAnnotation = fmt.Sprintf("/*%s*/", probeAnnotation)
	}
	return probeDBCfg, probeAnnotation
}
