package esmodel

import (
	"context"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/core"
	"github.com/stretchr/testify/require"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const (
	db = "test"

	studentTable       = "student"
	studentClassTable  = "student_class"
	studentDetailTable = "student_detail"
	studentParentTable = "student_parent"

	responseOK = `{
	"took": 30,
	"errors": false,
	"items": [
	   {
		  "index": {
			 "_index": "student_index",
			 "_type": "doc",
			 "_id": "1",
			 "_version": 1,
			 "result": "created",
			 "_shards": {
				"total": 2,
				"successful": 1,
				"failed": 0
			 },
			 "status": 201,
			 "_seq_no" : 0,
			 "_primary_term": 1
		  }
	   }
	]
}`

	mapping = `{"student_index":{"mappings":{"properties":{"birthday":{"type":"date"},"id":{"type":"long"},"introduceInfo":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"sd_id":{"type":"long"},"sd_student_id":{"type":"long"},"studentClass":{"properties":{"className":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"id":{"type":"long"},"student_id":{"type":"long"}}},"studentName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"studentParent":{"type":"nested","properties":{"id":{"type":"long"},"parentName":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"student_id":{"type":"long"}}}}}}}`

	esversion = `{"name":"elasticsearch7-1","cluster_name":"docker-cluster","cluster_uuid":"M_Mz4bsmRH60PuE_JBfLbw","version":{"number":"7.3.1","build_flavor":"default","build_type":"docker","build_hash":"4749ba6","build_date":"2019-08-19T20:19:25.651794Z","build_snapshot":false,"lucene_version":"8.1.0","minimum_wire_compatibility_version":"6.8.0","minimum_index_compatibility_version":"6.0.0-beta1"},"tagline":"You Know, for Search"}`
)

func initConfig() *config.Config {
	cfg := config.NewConfig()

	if err := cfg.ConfigFromFile("../../../docs/2.0/example-mysql2esmodel.toml"); err != nil {
		println(err)
		return nil
	}

	cfg.PipelineConfig.PipelineName = "mock"
	return cfg
}

func buildMsg(db string, table string, op core.DMLOp, pk *map[string]interface{}, data *map[string]interface{}, old *map[string]interface{}) *core.Msg {
	msg := core.Msg{
		Type: core.MsgDML,

		Host:     "127.0.0.1",
		Database: db,
		Table:    table,

		DdlMsg: &core.DDLMsg{},
		DmlMsg: &core.DMLMsg{
			Operation: op,
			Pks:       *pk,
		},
		Timestamp: time.Now(),
	}

	if data != nil {
		msg.DmlMsg.Data = *data
	} else {
		msg.DmlMsg.Data = map[string]interface{}{}
	}

	if op == core.Update {
		msg.DmlMsg.Old = *old
	}

	return &msg
}

func buildPkMap(id int64) *map[string]interface{} {
	return &map[string]interface{}{
		"id": id,
	}
}

func buildStudentDataMap(id int64, name string, birthday time.Time) *map[string]interface{} {
	return &map[string]interface{}{
		"id":       id,
		"name":     name,
		"birthday": birthday,
	}
}
func buildStudentClassDataMap(id int64, name string, studentId int64) *map[string]interface{} {
	return &map[string]interface{}{
		"id":         id,
		"name":       name,
		"student_id": studentId,
	}
}
func buildStudentDetailDataMap(id int64, introduce string, studentId int64) *map[string]interface{} {
	return &map[string]interface{}{
		"id":         id,
		"introduce":  introduce,
		"student_id": studentId,
	}
}
func buildStudentParentDataMap(id int64, name string, studentId int64) *map[string]interface{} {
	return &map[string]interface{}{
		"id":         id,
		"name":       name,
		"student_id": studentId,
	}
}

func getMainInsertMsgs(msgs *[]*core.Msg, count int64) *[]*core.Msg {
	for i := int64(1); i <= count; i++ {
		msg := buildMsg(db, studentTable, core.Insert,
			buildPkMap(i),
			buildStudentDataMap(i, "name_"+strconv.FormatInt(i, 10), time.Now()),
			nil)

		*msgs = append(*(msgs), msg)
	}
	return msgs
}

func buildServer() *http.Server {

	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		//time.Sleep(time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(responseOK))
	})
	mux.HandleFunc("/", staticHandler(http.StatusOK, esversion))
	mux.HandleFunc("/_all/_mapping/_all", staticHandler(http.StatusOK, mapping))

	srv := &http.Server{Addr: ":9200", Handler: mux}
	return srv
}

func TestConfigureStart(t *testing.T) {

	r := require.New(t)

	srv := buildServer()
	defer srv.Shutdown(context.TODO())
	go srv.ListenAndServe()

	output := defaultEsModelOutput()
	r.NoError(output.Start())
}

func TestGetRouter(t *testing.T) {
	r := require.New(t)
	output := defaultEsModelOutput()
	r.NotNil(output.GetRouter())
}

func TestExecute(t *testing.T) {

	r := require.New(t)

	msgs := &[]*core.Msg{}
	msgs = getMainInsertMsgs(msgs, 3)

	studentId1 := int64(1)
	studentId2 := int64(2)
	studentId3 := int64(3)
	classId1 := int64(1)
	classId2 := int64(2)
	detailId1 := int64(1)
	detailId3 := int64(3)
	parentId1 := int64(1)
	parentId2 := int64(2)
	parentId3 := int64(3)
	parentId4 := int64(4)

	*msgs = append(*(msgs), buildMsg(db, studentClassTable, core.Insert, buildPkMap(classId1), buildStudentClassDataMap(classId1, "class1", studentId1), nil))
	*msgs = append(*(msgs), buildMsg(db, studentDetailTable, core.Insert, buildPkMap(detailId1), buildStudentDetailDataMap(detailId1, "introduce1", studentId1), nil))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Insert, buildPkMap(parentId1), buildStudentParentDataMap(parentId1, "parent1", studentId1), nil))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Insert, buildPkMap(parentId2), buildStudentParentDataMap(parentId2, "parent2", studentId1), nil))

	*msgs = append(*(msgs), buildMsg(db, studentClassTable, core.Insert, buildPkMap(classId2), buildStudentClassDataMap(classId2, "class2", studentId2), nil))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Insert, buildPkMap(parentId3), buildStudentParentDataMap(parentId3, "parent3", studentId2), nil))

	*msgs = append(*(msgs), buildMsg(db, studentDetailTable, core.Insert, buildPkMap(detailId3), buildStudentDetailDataMap(detailId3, "introduce3", studentId3), nil))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Insert, buildPkMap(parentId4), buildStudentParentDataMap(parentId4, "parent4", studentId3), nil))

	*msgs = append(*(msgs), buildMsg(db, studentTable, core.Delete, buildPkMap(studentId2), nil, nil))
	*msgs = append(*(msgs), buildMsg(db, studentClassTable, core.Delete, buildPkMap(classId2), nil, nil))
	*msgs = append(*(msgs), buildMsg(db, studentDetailTable, core.Delete, buildPkMap(detailId1), nil, nil))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Delete, buildPkMap(parentId2), nil, nil))

	*msgs = append(*(msgs), buildMsg(db, studentTable, core.Update, buildPkMap(studentId1), buildStudentDataMap(studentId1, "update_name_1", time.Now()), buildStudentDataMap(studentId1, "name_1", time.Now())))
	*msgs = append(*(msgs), buildMsg(db, studentClassTable, core.Update, buildPkMap(classId1), buildStudentClassDataMap(classId1, "update_class1", studentId1), buildStudentDetailDataMap(detailId1, "introduce1", studentId1)))
	*msgs = append(*(msgs), buildMsg(db, studentParentTable, core.Update, buildPkMap(parentId1), buildStudentParentDataMap(parentId1, "update_parent1", studentId1), buildStudentParentDataMap(parentId1, "parent1", studentId1)))

	srv := buildServer()
	defer srv.Shutdown(context.TODO())
	go srv.ListenAndServe()

	output := defaultEsModelOutput()
	r.NoError(output.Start())

	r.NoError(output.Execute(*msgs))

}

func defaultEsModelOutput() *EsModelOutput {
	conf := initConfig()
	output := &EsModelOutput{}
	output.Configure(conf.PipelineConfig.PipelineName, conf.PipelineConfig.OutputPlugin.Config)
	return output
}

func staticHandler(code int, content string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		if content != "" {
			w.Write([]byte(content))
		}
	}
}
