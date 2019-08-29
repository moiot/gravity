package esmodel

import (
	"fmt"
	"github.com/moiot/gravity/pkg/core"
	"github.com/olivere/elastic/v7"
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

func genDocIDDeleteUpdate(msg *core.Msg, fk string) string {
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

func printJsonEncodef(format string, data interface{}) {
	bs, _ := json.Marshal(data)
	fmt.Printf(format, string(bs))
}
