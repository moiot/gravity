package esmodel

import (
	"fmt"
	"github.com/moiot/gravity/pkg/core"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/common/log"
)

func genDocID(msg *core.Msg, fk string) string {

	if fk != "" {
		return fmt.Sprint(msg.DmlMsg.Data[fk])
	}
	for _, v := range msg.DmlMsg.Pks {
		return fmt.Sprint(v)
	}
	return ""
}

func genDocIDBySon(msg *core.Msg, fk string) string {
	return fmt.Sprint(msg.DmlMsg.Old[fk])
}

func genPrimary(msg *core.Msg) (string, interface{}) {
	for k, v := range msg.DmlMsg.Pks {
		return k, v
	}
	return "", ""
}

func marshalError(err *elastic.ErrorDetails) string {
	bytes, _ := json.Marshal(err)
	return string(bytes)
}

func Capitalize(str string) string {
	var upperStr string
	vv := []rune(str)
	for i := 0; i < len(vv); i++ {
		if i == 0 {
			if vv[i] >= 97 && vv[i] <= 122 {
				vv[i] -= 32
				upperStr += string(vv[i])
			} else {
				fmt.Println("Not begins with lowercase letter,")
				return str
			}
		} else {
			upperStr += string(vv[i])
		}
	}
	return upperStr
}

func printJsonEncodef(format string, data ...interface{}) {
	jsons := make([]interface{}, 0, 1)
	for _, v := range data {
		switch v.(type) {
		case string:
			jsons = append(jsons, v)
			break
		default:
			bs, _ := json.Marshal(v)
			jsons = append(jsons, string(bs))
			break
		}
	}

	fmt.Printf(format, jsons...)
	log.Infof(format, jsons...)
}
