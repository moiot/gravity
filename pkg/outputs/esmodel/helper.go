package esmodel

import (
	"encoding/json"
	"fmt"
	"github.com/moiot/gravity/pkg/core"
	"github.com/olivere/elastic/v7"
	"strings"
)

func genDocID(msg *core.Msg) string {
	pks := []string{}
	for _, v := range msg.DmlMsg.Pks {
		pks = append(pks, fmt.Sprint(v))
	}
	return strings.Join(pks, "_")
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
