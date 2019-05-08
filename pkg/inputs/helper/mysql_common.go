package helper

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/moiot/gravity/pkg/config"

	"github.com/juju/errors"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

type SourceProbeCfg struct {
	SourceMySQL *config.DBConfig `mapstructure:"mysql"json:"mysql"`
	Annotation  string           `mapstructure:"annotation"json:"annotation"`
}

type BinlogPositionsValue struct {
	CurrentPosition *config.MySQLBinlogPosition `json:"current_position"`
	StartPosition   *config.MySQLBinlogPosition `json:"start_position"`
}

func BinlogPositionValueEncoder(v interface{}) (string, error) {
	return myJson.MarshalToString(v)
}

func BinlogPositionValueDecoder(s string) (interface{}, error) {
	return DeserializeBinlogPositionValue(s)
}

func SerializeBinlogPositionValue(position BinlogPositionsValue) (string, error) {
	return BinlogPositionValueEncoder(position)
}

func DeserializeBinlogPositionValue(value string) (BinlogPositionsValue, error) {
	position := BinlogPositionsValue{}
	if err := myJson.UnmarshalFromString(value, &position); err != nil {
		return BinlogPositionsValue{}, errors.Trace(err)
	}
	return position, nil
}

func GetProbCfg(sourceProbeCfg *SourceProbeCfg, sourceDBCfg *config.DBConfig) (*config.DBConfig, string) {
	var probeDBCfg *config.DBConfig
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
