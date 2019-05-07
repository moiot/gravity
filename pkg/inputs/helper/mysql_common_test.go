package helper

import (
	"fmt"
	"testing"

	"github.com/moiot/gravity/pkg/config"

	"github.com/stretchr/testify/require"
)

func TestSerializeBinlogPositions(t *testing.T) {
	r := require.New(t)
	gtidSmall := "123edf:1-10"
	gtidBig := "23456:1-20"

	position := BinlogPositionsValue{
		CurrentPosition: &config.MySQLBinlogPosition{BinlogGTID: fmt.Sprintf("%s,%s", gtidBig, gtidSmall)},
	}

	v, err := SerializeBinlogPositionValue(position)
	r.NoError(err)

	p2, err := DeserializeBinlogPositionValue(v)
	r.NoError(err)
	r.Equal(fmt.Sprintf("%s,%s", gtidBig, gtidSmall), p2.CurrentPosition.BinlogGTID)
	r.Nil(p2.StartPosition)
}
