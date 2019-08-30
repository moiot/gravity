package esmodel

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/outputs/elasticsearch"
	"github.com/moiot/gravity/pkg/outputs/routers"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

const (
	Name = "esmodel"

	esModelInsertListScriptName = "GravityEsModelListInsertScript"
	/**
	if(ctx._source.containsKey(params.field)){
	    Map it= ctx._source.get(params.field).find(item -> item.get(params.key) == params.value);
	    if(it != null && !it.isEmpty()){
	        ctx._source.get(params.field).removeIf(item -> item.get(params.key) == params.value);
	    }
	    ctx._source.get(params.field).add(params.message);
	}else{
	    ctx._source.put(params.field,[params.message])
	}
	*/
	esModelInsertListScript = "if(ctx._source.containsKey(params.field)){Map it= ctx._source.get(params.field).find(item -> item.get(params.key) == params.value);if(it != null && !it.isEmpty()){ctx._source.get(params.field).removeIf(item -> item.get(params.key) == params.value);}ctx._source.get(params.field).add(params.message);}else{ctx._source.put(params.field,[params.message]);}"

	esModelUpdateListScriptName = "GravityEsModelListUpdateScript"
	/**
	if(ctx._source.containsKey(params.field)){
	    Map it= ctx._source.get(params.field).find(item -> item.get(params.key) == params.value);
	    if(it != null && !it.isEmpty()){
	        it.putAll(params.updates)
	    }else{
	        ctx._source.get(params.field).add(params.message)
	    }
	}else{
	    ctx._source.put(params.field,[params.message])
	}
	*/
	esModelUpdateListScript = "if(ctx._source.containsKey(params.field))" +
		"{Map it= ctx._source.get(params.field).find(item -> item.get(params.key) == params.value);" +
		"if(it != null && !it.isEmpty()){it.putAll(params.updates)}" +
		"else{ctx._source.get(params.field).add(params.message)}}" +
		"else{ctx._source.put(params.field,[params.message])}"

	esModelDeleteListScriptName = "GravityEsModelListDeleteScript"
	/**
	if(ctx._source.containsKey(params.field)){
	  ctx._source.get(params.field).removeIf(item -> item.get(params.key) == params.value)
	}
	*/
	esModelDeleteListScript = "if(ctx._source.containsKey(params.field))" +
		"{ctx._source.get(params.field).removeIf(item -> item.get(params.key) == params.value)}"
)

var (
	json           = jsoniter.ConfigCompatibleWithStandardLibrary
	esModelScripts = map[string]string{
		esModelInsertListScriptName: esModelInsertListScript,
		esModelUpdateListScriptName: esModelUpdateListScript,
		esModelDeleteListScriptName: esModelDeleteListScript,
	}
)

type EsScript struct {
	Script EsScriptInfo `json:"script"`
}

type EsScriptInfo struct {
	Lang   string `json:"lang"`
	Source string `json:"source"`
}

type EsModelOutput struct {
	pipelineName string
	config       *elasticsearch.ElasticsearchPluginConfig
	client       *elastic.Client
	router       routers.EsModelRouter
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &EsModelOutput{}, false)
}

func (output *EsModelOutput) Configure(pipelineName string, data map[string]interface{}) error {
	// setup output
	output.pipelineName = pipelineName

	// setup plugin config
	pluginConfig := elasticsearch.ElasticsearchPluginConfig{}

	err := mapstructure.Decode(data, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	if pluginConfig.ServerConfig == nil {
		return errors.Errorf("empty esmodel config")
	}

	if len(pluginConfig.ServerConfig.URLs) == 0 {
		return errors.Errorf("empty esmodel urls")
	}
	output.config = &pluginConfig

	routes, err := routers.NewEsModelRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}

	output.router = routers.EsModelRouter(routes)
	return nil
}

func (output *EsModelOutput) GetRouter() core.Router {
	return output.router
}

func (output *EsModelOutput) Start() error {
	serverConfig := output.config.ServerConfig
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(serverConfig.URLs...),
		elastic.SetSniff(serverConfig.Sniff),
	}

	if auth := serverConfig.Auth; auth != nil {
		options = append(options, elastic.SetBasicAuth(auth.Username, auth.Password))
	}

	timeout := serverConfig.Timeout

	if timeout == 0 {
		timeout = 1000
	}
	options = append(options, elastic.SetHttpClient(&http.Client{
		Timeout: time.Duration(timeout) * time.Millisecond,
	}))

	client, err := elastic.NewClient(options...)
	if err != nil {
		return errors.Trace(err)
	}
	output.client = client

	output.getEsVersion()

	for _, v := range output.router {
		if err = output.checkAndSetIndex(v); err != nil {
			log.Errorf("check set index fail. route: %v .", v)
			return errors.Trace(err)
		}
	}

	output.checkEsScript()

	return nil
}

func (output *EsModelOutput) Close() {
	output.client.Stop()
}

func (output *EsModelOutput) Execute(msgs []*core.Msg) error {

	var reqs []elastic.BulkableRequest
	for _, msg := range msgs {
		// only support dml message

		if msg.DmlMsg == nil {
			continue
		}

		// 可能匹配多条索引规则
		routes, ok := output.router.Match(msg)
		if !ok {
			continue
		}

		for _, route := range *routes {

			printJsonEncodef("msg table %s, oper %s, data: %s, old: %s \n", msg.Table, msg.DmlMsg.Operation, msg.DmlMsg.Data, msg.DmlMsg.Old)

			if len(msg.DmlMsg.Pks) == 0 {
				if route.IgnoreNoPrimaryKey {
					continue
				} else {
					return errors.Errorf("[output_esmodel] Table must have at one primary key, database: %s, table: %s.", msg.Database, msg.Table)
				}
			} else if len(msg.DmlMsg.Pks) > 1 {
				return errors.Errorf("[output_esmodel] Table must have at one primary key, database: %s, table: %s.", msg.Database, msg.Table)
			}

			index := route.IndexName
			if index == "" {
				return errors.Errorf("[output_esmodel] elasticsearch index name not exist, database: %s, table: %s.", msg.Database, msg.Table)
			}

			if msg.DmlMsg.Operation == core.Insert {
				reqs = append(reqs, *(output.insertMsg(msg, route))...)
			} else if msg.DmlMsg.Operation == core.Delete {
				reqs = append(reqs, *(output.deleteMsg(msg, route))...)
			} else {
				reqs = append(reqs, *(output.updateMsg(msg, route))...)
			}
		}
	}
	return output.sendBulkRequests(reqs)
}

func (output *EsModelOutput) insertMsg(msg *core.Msg, route *routers.EsModelRoute) *[]elastic.BulkableRequest {
	var reqs []elastic.BulkableRequest

	if route.Match(msg) {
		reqs = append(reqs, output.insertMain(msg, route))
	}
	for _, r := range *route.OneOne {
		if r.Match(msg) {
			reqs = append(reqs, output.insertOneOne(msg, route, r))
		}
	}
	for _, r := range *route.OneMore {
		if r.Match(msg) {
			reqs = append(reqs, output.insertOneMore(msg, route, r))
		}
	}
	return &reqs
}

func (output *EsModelOutput) insertMain(msg *core.Msg, route *routers.EsModelRoute) *elastic.BulkUpdateRequest {
	docId := genDocID(msg, "")
	data := transMsgData(&msg.DmlMsg.Data, route.IncludeColumn, route.ExcludeColumn, route.ConvertColumn, "", false)
	printJsonEncodef("create obj docId: %s, json: %s \n", docId, data)

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Doc(data).
		Upsert(data)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) insertOneOne(msg *core.Msg, route *routers.EsModelRoute, routeOne *routers.EsModelOneOneRoute) *elastic.BulkUpdateRequest {
	docId := genDocID(msg, routeOne.FkColumn)

	data := &map[string]interface{}{}
	if routeOne.Mode == routers.EsModelOneOneObject {
		(*data)[routeOne.PropertyName] = transMsgData(&msg.DmlMsg.Data, routeOne.IncludeColumn, routeOne.ExcludeColumn, routeOne.ConvertColumn, routeOne.PropertyPre, false)
	} else {
		data = transMsgData(&msg.DmlMsg.Data, routeOne.IncludeColumn, routeOne.ExcludeColumn, routeOne.ConvertColumn, routeOne.PropertyPre, false)
	}
	printJsonEncodef("create oneone obj docId: %s, json: %s \n", docId, data)

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Doc(data).
		Upsert(data)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) insertOneMore(msg *core.Msg, route *routers.EsModelRoute, routeMore *routers.EsModelOneMoreRoute) *elastic.BulkUpdateRequest {
	docId := genDocID(msg, routeMore.FkColumn)

	k, v := genPrimary(msg)
	message := transMsgData(&msg.DmlMsg.Data, routeMore.IncludeColumn, routeMore.ExcludeColumn, routeMore.ConvertColumn, "", false)
	data := &map[string]interface{}{
		routeMore.PropertyName: []interface{}{message},
	}
	params := map[string]interface{}{}
	params["message"] = message
	params["field"] = routeMore.PropertyName
	params["value"] = v
	params["key"] = k
	printJsonEncodef("create onemore obj docId: %s, json: %s, params: %s \n", docId, data, params)

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Upsert(data).
		Script(elastic.NewScriptStored(esModelInsertListScriptName).Params(params))

	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) deleteMsg(msg *core.Msg, route *routers.EsModelRoute) *[]elastic.BulkableRequest {
	var reqs []elastic.BulkableRequest

	if route.Match(msg) {
		reqs = append(reqs, output.deleteMain(msg, route))
	}
	for _, r := range *route.OneOne {
		if r.Match(msg) {
			reqs = append(reqs, output.deleteOneOne(msg, route, r))
		}
	}
	for _, r := range *route.OneMore {
		if r.Match(msg) {
			reqs = append(reqs, output.deleteOneMore(msg, route, r))
		}
	}
	return &reqs
}

func (output *EsModelOutput) deleteMain(msg *core.Msg, route *routers.EsModelRoute) *elastic.BulkDeleteRequest {
	docId := genDocID(msg, "")
	printJsonEncodef("delete obj docId: %s, json: %s \n", docId, msg.DmlMsg.Old)

	req := elastic.NewBulkDeleteRequest().
		Index(route.IndexName).
		Id(docId)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) deleteOneOne(msg *core.Msg, route *routers.EsModelRoute, routeOne *routers.EsModelOneOneRoute) *elastic.BulkUpdateRequest {
	docId := genDocIDBySon(msg, routeOne.FkColumn)
	data := &map[string]interface{}{}
	if routeOne.Mode == routers.EsModelOneOneObject {
		(*data)[routeOne.PropertyName] = nil
	} else {
		data = transMsgData(&msg.DmlMsg.Old, routeOne.IncludeColumn, routeOne.ExcludeColumn, routeOne.ConvertColumn, routeOne.PropertyPre, true)
	}
	printJsonEncodef("delete oneone obj docId: %s , json: %s \n", docId, data)

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Doc(data).
		Upsert(data)

	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) deleteOneMore(msg *core.Msg, route *routers.EsModelRoute, routeMore *routers.EsModelOneMoreRoute) *elastic.BulkUpdateRequest {

	docId := genDocIDBySon(msg, routeMore.FkColumn)

	k, v := genPrimary(msg)
	params := map[string]interface{}{}
	params["field"] = routeMore.PropertyName
	params["value"] = v
	params["key"] = k

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Script(elastic.NewScriptStored(esModelDeleteListScriptName).Params(params))

	printJsonEncodef("delete onemore obj %s json: %s \n", docId, params)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) updateMsg(msg *core.Msg, route *routers.EsModelRoute) *[]elastic.BulkableRequest {
	var reqs []elastic.BulkableRequest

	if route.Match(msg) {
		reqs = append(reqs, output.updateMain(msg, route))
	}
	for _, r := range *route.OneOne {
		if r.Match(msg) {
			reqs = append(reqs, output.updateOneOne(msg, route, r))
		}
	}
	for _, r := range *route.OneMore {
		if r.Match(msg) {
			reqs = append(reqs, output.updateOneMore(msg, route, r))
		}
	}
	return &reqs
}

func (output *EsModelOutput) updateMain(msg *core.Msg, route *routers.EsModelRoute) *elastic.BulkUpdateRequest {

	docId := genDocID(msg, "")
	data := transMsgData(&msg.DmlMsg.Data, route.IncludeColumn, route.ExcludeColumn, route.ConvertColumn, "", false)
	printJsonEncodef("update main obj %s json: %s \n", docId, data)

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Doc(data).
		Upsert(data)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) updateOneOne(msg *core.Msg, route *routers.EsModelRoute, routeOne *routers.EsModelOneOneRoute) *elastic.BulkUpdateRequest {

	docId := genDocIDBySon(msg, routeOne.FkColumn)
	data := &map[string]interface{}{}
	if routeOne.Mode == routers.EsModelOneOneObject {
		(*data)[routeOne.PropertyName] = transMsgData(&msg.DmlMsg.Data, routeOne.IncludeColumn, routeOne.ExcludeColumn, routeOne.ConvertColumn, routeOne.PropertyPre, false)
	} else {
		data = transMsgData(&msg.DmlMsg.Data, routeOne.IncludeColumn, routeOne.ExcludeColumn, routeOne.ConvertColumn, routeOne.PropertyPre, false)
	}

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(docId).
		Doc(data).
		Upsert(data)
	printJsonEncodef("update main obj %s json: %s \n", docId, data)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) updateOneMore(msg *core.Msg, route *routers.EsModelRoute, routeMore *routers.EsModelOneMoreRoute) *elastic.BulkUpdateRequest {

	docId := genDocIDBySon(msg, routeMore.FkColumn)
	k, v := genPrimary(msg)
	message := transMsgData(&msg.DmlMsg.Data, routeMore.IncludeColumn, routeMore.ExcludeColumn, routeMore.ConvertColumn, "", false)
	data := &map[string]interface{}{
		routeMore.PropertyName: []interface{}{message},
	}
	params := map[string]interface{}{}
	params["message"] = message
	// updates 可以仅传改动的map，暂不考虑
	params["updates"] = message
	params["field"] = routeMore.PropertyName
	params["value"] = v
	params["key"] = k

	req := elastic.NewBulkUpdateRequest().
		Index(route.IndexName).
		RetryOnConflict(route.RetryCount).
		Id(genDocIDBySon(msg, routeMore.FkColumn)).
		Upsert(data).
		Script(elastic.NewScriptStored(esModelUpdateListScriptName).Params(params))

	printJsonEncodef("update main obj %s json: %s, params: %s. \n", docId, data, params)
	if routers.EsModelVersion6 == route.EsVer {
		req = req.Type(route.TypeName)
	}
	return req
}

func (output *EsModelOutput) sendBulkRequests(reqs []elastic.BulkableRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	bulkRequest := output.client.Bulk()
	bulkRequest.Add(reqs...)
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	for _, item := range bulkResponse.Items {
		for action, result := range item {
			if output.isSuccessful(result, action) {
				// tags: [pipelineName, index, action(index/create/delete/update), status(200/400)].
				// indices created in 6.x only allow a single-type per index, so we don't need the type as a tag.
				var status int
				if result.Status == http.StatusBadRequest {
					printJsonEncodef("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, details: %s.", result.Index, marshalError(result.Error))
					log.Warnf("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, details: %s.", result.Index, marshalError(result.Error))
					status = http.StatusBadRequest
				} else {
					// 200/201/404(delete) -> 200 because the request is successful
					status = http.StatusOK
				}
				metrics.OutputCounter.WithLabelValues(output.pipelineName, result.Index, action, string(status), "").Add(1)
			} else if result.Status == http.StatusTooManyRequests {
				// when the server returns 429, it must be that all requests have failed.
				printJsonEncodef("[output_elasticsearch] The remote server returned an error: (429) Too Many Requests.")
				return errors.Errorf("[output_elasticsearch] The remote server returned an error: (429) Too Many Requests.")
			} else {
				printJsonEncodef("[output_elasticsearch] Received an error from server, status: [%d], index: %s, details: %s.", result.Status, result.Index, marshalError(result.Error))
				return errors.Errorf("[output_elasticsearch] Received an error from server, status: [%d], index: %s, details: %s.", result.Status, result.Index, marshalError(result.Error))
			}
		}
	}
	return nil
}

func (output *EsModelOutput) isSuccessful(result *elastic.BulkResponseItem, action string) bool {
	return (result.Status >= 200 && result.Status <= 299) ||
		(result.Status == http.StatusNotFound && action == "delete") || // delete but not found, just ignore it.
		(result.Status == http.StatusBadRequest && output.config.IgnoreBadRequest) // ignore index not found, parse error, etc.
}

/**
检查设置索引
{"properties":{"xxx":{"type":"nested"},"xx":{"type":"object"}}}
*/
func (output *EsModelOutput) checkAndSetIndex(route *routers.EsModelRoute) error {

	updatemapping := map[string]map[string]string{}

	mappings, err := output.client.GetMapping().Do(context.Background())

	if err != nil {
		return errors.Trace(err)
	}
	needUpdate := false
	mappingExist := false
	mapp := map[string]interface{}{}

	if mappings != nil {
		if mapping, ok := mappings[route.IndexName]; ok {
			// mapping已存在
			if route.EsVer == routers.EsModelVersion7 {
				mapp = (mapping.(map[string]interface{}))["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
			} else {
				mapp = (mapping.(map[string]interface{}))["mappings"].(map[string]interface{})[route.TypeName].(map[string]interface{})["properties"].(map[string]interface{})
			}
			mappingExist = true
		}
	} else {
		needUpdate = true
	}
	for _, one := range *route.OneOne {
		if one.Mode != routers.EsModelOneOneObject {
			// one one 对象不以对象形式写入索引
			continue
		}
		if _, ok := mapp[one.PropertyName]; ok {
			continue
		}
		updatemapping[one.PropertyName] = map[string]string{
			"type": routers.EsModelTypeMappingObject,
		}
		needUpdate = true
	}
	for _, one := range *route.OneMore {
		if _, ok := mapp[one.PropertyName]; ok {
			continue
		}
		updatemapping[one.PropertyName] = map[string]string{
			"type": routers.EsModelTypeMappingNested,
		}
		needUpdate = true
	}

	if needUpdate == false {
		return nil
	}

	updateMapping := &map[string]interface{}{
		"properties": updatemapping,
	}
	log.Infof("modify es mapping : %v ", updateMapping)
	if mappingExist != true {
		if err := output.createIndex(route, updateMapping); err != nil {
			return err
		}
	} else {
		if err := output.updateIndex(route, updateMapping); err != nil {
			return err
		}
	}

	return nil
}

/**
创建索引
*/
func (output *EsModelOutput) createIndex(route *routers.EsModelRoute, mapping *map[string]interface{}) error {
	settings := map[string]interface{}{}
	settings["number_of_shards"] = route.ShardsNum
	settings["number_of_replicas"] = route.ReplicasNum

	index := map[string]interface{}{
		"settings": settings,
		"mappings": mapping,
	}
	if route.EsVer == routers.EsModelVersion6 {
		// 兼容ES6
		index["mappings"] = map[string]interface{}{
			route.TypeName: mapping,
		}
	}

	jstr, err := json.MarshalToString(index)
	if err != nil {
		return errors.Errorf("create mapping convert json fail. index %s type %s mapping %v. ", route.IndexName, route.TypeName, index)
	}
	printJsonEncodef("create index %s mapping json: %s \n ", route.IndexName, index)
	createIndex, err := output.client.CreateIndex(route.IndexName).BodyString(jstr).Do(context.Background())
	if err != nil {
		return errors.Errorf("create %s index %s type fail. err: %v.", route.IndexName, route.TypeName, err)
	}
	if !createIndex.Acknowledged {
		return errors.Errorf("create %s index %s type fail. ", route.IndexName, route.TypeName)
	}
	return nil
}

/**
更新索引
*/
func (output *EsModelOutput) updateIndex(route *routers.EsModelRoute, mapping *map[string]interface{}) error {
	jstr, err := json.MarshalToString(mapping)
	if err != nil {
		return errors.Errorf("create mapping convert json fail. index %s type %s mapping %v. ", route.IndexName, route.TypeName, mapping)
	}
	printJsonEncodef("update index %s mapping json: %s \n ", route.IndexName, jstr)

	updateIndex, err := output.client.PutMapping().Index(route.IndexName).BodyString(jstr).Do(context.Background())

	if err != nil {
		return errors.Errorf("update %s index %s type fail. err: %v.", route.IndexName, route.TypeName, err)
	}
	if !updateIndex.Acknowledged {
		return errors.Errorf("create %s index %s type fail. ", route.IndexName, route.TypeName)
	}
	return nil
}

/**
check es 脚本
*/
func (output *EsModelOutput) checkEsScript() error {
	for k, v := range esModelScripts {
		resp, err := output.client.DeleteScript().Id(k).Do(context.Background())
		fmt.Println(resp)

		getResp, err := output.client.GetScript().Id(k).Do(context.Background())
		if err != nil || !getResp.Found {
			// 查不到也返回 404 err
			scr := EsScript{
				Script: EsScriptInfo{
					Lang:   "painless",
					Source: v,
				},
			}
			putResp, err := output.client.PutScript().Id(k).BodyJson(scr).Do(context.Background())
			if err != nil {
				return errors.Errorf("put script %s fail. ", k)
			}
			if !putResp.Acknowledged {
				return errors.Errorf("put script %s fail. ", k)
			}
			continue
		}

		byts, err := getResp.Script.MarshalJSON()
		if err != nil {
			return errors.Errorf(" script check fail %s . ", k)
		}
		script := &EsScriptInfo{}
		err = json.Unmarshal(byts, script)
		if err != nil {
			return errors.Errorf(" json convert fail. script check fail %s . %v.", k, err)
		}
		if script.Source != v {
			return errors.Errorf(" script diff fail. script check fail %s . es: %s. sys: %s ", k, script.Source, v)
		}
	}

	return nil
}

/**
获取es版本
*/
func (output *EsModelOutput) getEsVersion() error {
	url := output.config.ServerConfig.URLs[0]
	v, err := output.client.ElasticsearchVersion(url)
	if err != nil {
		return err
	}
	if v == "" {
		return errors.Errorf(" get elasticsearch version fail. url: %s ", url)
	}
	printJsonEncodef("elasticsearch version %s. \n", v)
	vs := v[0:1]
	if vs == routers.EsModelVersion7 || vs == routers.EsModelVersion6 {
		for _, r := range output.router {
			r.EsVer = vs
		}
		return nil
	}
	return errors.Errorf(" elasticsearch version not support. version: %s ", v)
}

/**
转义数据集, 如果是oneone子对象删除数据集，value为nil
*/
func transMsgData(data *map[string]interface{}, includes *map[string]string, excludes *map[string]string,
	converts *map[string]string, propPre string, isNil bool) *map[string]interface{} {
	trans := &map[string]interface{}{}

	for k, v := range *data {
		if _, ok := (*excludes)[k]; ok {
			// 过滤策略不写入
			continue
		}
		if len((*includes)) > 0 {
			if _, ok := (*includes)[k]; !ok {
				// 不在包含列中不写入
				continue
			}
		}

		if key, ok := (*converts)[k]; ok {
			// 替换列名
			if isNil {
				(*trans)[key] = nil
			} else {
				(*trans)[key] = v
			}
			continue
		}
		if propPre != "" {
			if isNil {
				(*trans)[fmt.Sprintf("%s%s", propPre, k)] = nil
			} else {
				(*trans)[fmt.Sprintf("%s%s", propPre, k)] = v
			}
			continue
		}
		if isNil {
			(*trans)[k] = nil
		} else {
			(*trans)[k] = v
		}

	}

	return trans
}
