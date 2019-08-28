package esmodel

import (
	"encoding/json"
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

func getMsgs() *[]*core.Msg {
	msgs := make([]*core.Msg, 0, 10)

	for i := 0; i < 10; i++ {
		msg := core.Msg{
			Type: core.MsgDML,

			Host:     "192.168.1.148",
			Database: "polaris_project_manage",
			Table:    "ppm_pri_issue",

			DdlMsg: &core.DDLMsg{},
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id":              int64(i),
					"org_id":          int64(i),
					"code":            "CODE-" + strconv.Itoa(i),
					"title":           "任务-" + strconv.Itoa(i),
					"plan_start_time": time.Now(),
					"plan_end_time":   time.Now(),
				},
				Old: map[string]interface{}{
					"id": int64(i),
				},
				Pks: map[string]interface{}{
					"id": int64(i),
				},
			},

			Timestamp: time.Now(),
		}

		msgs = append(msgs, &msg)
	}
	return &msgs
}

func TestCapitalize3(t *testing.T) {

	msgs := getMsgs()

	conf := getConfig()

	output := EsModelOutput{}
	output.Configure(conf.PipelineConfig.PipelineName, conf.PipelineConfig.OutputPlugin.Config)

	output.Start()

	output.Execute(*msgs)

}

func TestCapitalize2(t *testing.T) {
	a := EsModelIndex{
		TypeName:  "a",
		IndexName: "1",
	}

	mm := map[EsModelIndex]string{}
	mm[a] = "aaa"

	b := EsModelIndex{
		TypeName:  "a",
		IndexName: "1",
	}

	fmt.Println(mm[b])

}
