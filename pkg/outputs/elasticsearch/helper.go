package elasticsearch

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/moiot/gravity/pkg/core"
	"github.com/olivere/elastic"
)

func genDocID(msg *core.Msg) string {
	pks := []string{}
	for _, v := range msg.DmlMsg.Pks {
		pks = append(pks, fmt.Sprint(v))
	}
	return strings.Join(pks, "_")
}

// TODO validate index name
// https://github.com/elastic/elasticsearch/blob/608a61ab85e82f8f6e88002ba7d8458411e7da62
// /core/src/test/java/org/elasticsearch/cluster/metadata/MetaDataCreateIndexServiceTests.java#L188-L202
func genIndexName(table string) string {
	return strings.ToLower(strings.TrimLeft(table, "_-+"))
}

func marshalError(err *elastic.ErrorDetails) string {
	bytes, _ := json.Marshal(err)
	return string(bytes)
}
