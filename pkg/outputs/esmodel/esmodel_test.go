package esmodel

import (
	"fmt"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"strconv"
	"testing"
	"time"
)

func TestEsModelOutput_Configure(t *testing.T) {

	cfg := config.NewConfig()

	if err := cfg.ConfigFromFile("../../../bin/test.toml"); err != nil {
		println(err)
	}

	if bs, err := json.Marshal(cfg.PipelineConfig.OutputPlugin); err != nil {
		println(err)
	} else {
		fmt.Printf("%s\n", string(bs))
	}

}

func getConfig() *config.Config {
	cfg := config.NewConfig()

	if err := cfg.ConfigFromFile("../../../bin/test.toml"); err != nil {
		println(err)
	}

	return cfg
}

func getMainInsertMsgs(msgs *[]*core.Msg) *[]*core.Msg {
	for i := 0; i < 10; i++ {
		msg := core.Msg{
			Type: core.MsgDML,

			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":       int64(i),
					"name":     "name-" + strconv.Itoa(i),
					"age":      i,
					"birthday": time.Now(),
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}
	return msgs
}

func getSonInsertMsgs(msgs *[]*core.Msg) *[]*core.Msg {

	for i := 1; i < 5; i++ {
		msg := core.Msg{
			Type:     core.MsgDML,
			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_detail",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":         int64(i),
					"student_id": int64(i),
					"name":       "detail-name-" + strconv.Itoa(i),
					"introduce":  "detail-introduce-" + strconv.Itoa(i),
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}

	for i := 3; i < 7; i++ {
		msg := core.Msg{
			Type:     core.MsgDML,
			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_class",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":            int64(i),
					"student_id":    int64(i),
					"name":          "class-name-" + strconv.Itoa(i),
					"student_count": 20 + i,
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}

	id := 1
	for i := 6; i < 8; i++ {
		id += 1
		msg := core.Msg{
			Type:     core.MsgDML,
			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_parent",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":         id,
					"student_id": int64(i),
					"name":       "student_parent1-name-" + strconv.Itoa(i),
					"introduce":  "detail-introduce-" + strconv.Itoa(i),
				},
				Pks: map[string]interface{}{
					"id": id,
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)

		id += 1
		msg2 := core.Msg{
			Type:     core.MsgDML,
			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_parent",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":         id,
					"student_id": int64(i),
					"name":       "student_parent2-name-" + strconv.Itoa(i),
					"introduce":  "detail-introduce-" + strconv.Itoa(i),
				},
				Pks: map[string]interface{}{
					"id": id,
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg2)
	}

	return msgs
}

func getMainDeleteMsgs(msgs *[]*core.Msg) *[]*core.Msg {
	for i := 5; i < 7; i++ {
		msg := core.Msg{
			Type: core.MsgDML,

			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Delete,
				Old: map[string]interface{}{
					"id":       int64(i),
					"name":     "name-" + strconv.Itoa(i),
					"age":      i,
					"birthday": time.Now(),
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}
	return msgs
}

func getSonDeleteMsgs(msgs *[]*core.Msg) *[]*core.Msg {
	for i := 2; i < 3; i++ {
		msg := core.Msg{
			Type: core.MsgDML,

			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_detail",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Delete,
				Old: map[string]interface{}{
					"id":         int64(i),
					"student_id": int64(i),
					"name":       "detail-name-" + strconv.Itoa(i),
					"introduce":  "detail-introduce-" + strconv.Itoa(i),
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}

	for i := 5; i < 6; i++ {
		msg := core.Msg{
			Type: core.MsgDML,

			Host:     "192.168.1.148",
			Database: "test_database",
			Table:    "student_class",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Delete,
				Old: map[string]interface{}{
					"id":            int64(i),
					"student_id":    int64(i),
					"name":          "class-name-" + strconv.Itoa(i),
					"student_count": 20 + i,
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},
			Timestamp: time.Now(),
		}
		*msgs = append(*(msgs), &msg)
	}

	id := 2
	msg2 := core.Msg{
		Type:     core.MsgDML,
		Host:     "192.168.1.148",
		Database: "test_database",
		Table:    "student_parent",

		DdlMsg: &core.DDLMsg{},
		DmlMsg: &core.DMLMsg{
			Operation: core.Delete,
			Data: map[string]interface{}{
				"id":         id,
				"student_id": int64(6),
				"name":       "student_parent2-name-" + strconv.Itoa(6),
				"introduce":  "detail-introduce-" + strconv.Itoa(6),
			},
			Pks: map[string]interface{}{
				"id": int64(6),
			},
		},
		Timestamp: time.Now(),
	}
	*msgs = append(*(msgs), &msg2)

	return msgs
}

func TestCapitalize3(t *testing.T) {

	msgs := &([]*core.Msg{})
	//msgs = getMainInsertMsgs(msgs)
	//msgs = getSonInsertMsgs(msgs)
	//msgs = getMainDeleteMsgs(msgs)
	//msgs = getSonDeleteMsgs(msgs)

	//id := 2
	//msg2 := core.Msg{
	//	Type: core.MsgDML,
	//	Host:     "192.168.1.148",
	//	Database: "test_database",
	//	Table:    "student_parent",
	//
	//	DdlMsg: &core.DDLMsg{},
	//	DmlMsg: &core.DMLMsg{
	//		Operation: core.Insert,
	//		Data: map[string]interface{}{
	//			"id":              id,
	//			"student_id":      int64(6),
	//			"name":            "student_parent2-aaaa-" + strconv.Itoa(6),
	//			"introduce":       "detail-aaaa-" + strconv.Itoa(6),
	//		},
	//		Pks: map[string]interface{}{
	//			"id": id,
	//		},
	//	},
	//	Timestamp: time.Now(),
	//}
	//*msgs = append(*(msgs), &msg2)

	conf := getConfig()
	output := EsModelOutput{}
	output.Configure(conf.PipelineConfig.PipelineName, conf.PipelineConfig.OutputPlugin.Config)

	output.Start()

	output.Execute(*msgs)

}
