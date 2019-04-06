package elasticsearch

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/outputs/routers"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
)

const (
	Name = "elasticsearch"
)

type ElasticsearchServerConfig struct {
	URLs  []string `mapstructure:"urls" toml:"urls" json:"urls"`
	Sniff bool     `mapstructure:"sniff" toml:"sniff" json:"sniff"`
}

type ElasticsearchPluginConfig struct {
	ServerConfig     *ElasticsearchServerConfig `mapstructure:"server" json:"server"`
	Routes           []map[string]interface{}   `mapstructure:"routes" json:"routes"`
	IgnoreBadRequest bool                       `mapstructure:"ignore-bad-request" json:"ignore-bad-request"`
}

type ElasticsearchOutput struct {
	pipelineName string
	config       *ElasticsearchPluginConfig
	client       *elastic.Client
	router       routers.ElasticsearchRouter
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, Name, &ElasticsearchOutput{}, false)
}

func (output *ElasticsearchOutput) Configure(pipelineName string, data map[string]interface{}) error {
	// setup output
	output.pipelineName = pipelineName

	// setup plugin config
	pluginConfig := ElasticsearchPluginConfig{}

	err := mapstructure.Decode(data, &pluginConfig)
	if err != nil {
		return errors.Trace(err)
	}

	if pluginConfig.ServerConfig == nil {
		return errors.Errorf("empty elasticsearch config")
	}

	if len(pluginConfig.ServerConfig.URLs) == 0 {
		return errors.Errorf("empty elasticsearch urls")
	}
	output.config = &pluginConfig

	routes, err := routers.NewElasticsearchRoutes(pluginConfig.Routes)
	if err != nil {
		return errors.Trace(err)
	}
	output.router = routers.ElasticsearchRouter(routes)
	return nil
}

func (output *ElasticsearchOutput) GetRouter() core.Router {
	return output.router
}

func (output *ElasticsearchOutput) Start() error {
	serverConfig := output.config.ServerConfig
	client, err := elastic.NewClient(elastic.SetSniff(serverConfig.Sniff), elastic.SetURL(serverConfig.URLs...))
	if err != nil {
		return errors.Trace(err)
	}
	output.client = client
	return nil
}

func (output *ElasticsearchOutput) Close() {
	output.client.Stop()
}

func (output *ElasticsearchOutput) Execute(msgs []*core.Msg) error {
	var reqs []elastic.BulkableRequest
	for _, msg := range msgs {
		// only support dml message
		if msg.DmlMsg == nil {
			continue
		}
		// must have a primary key
		if len(msg.DmlMsg.Pks) == 0 {
			continue
		}
		route, ok := output.router.Match(msg)
		if !ok {
			continue
		}

		var req elastic.BulkableRequest
		if msg.DmlMsg.Operation == core.Delete {
			req = elastic.NewBulkDeleteRequest().Index(route.TargetIndex).Type(route.TargetType).Id(genDocID(msg))
		} else {
			req = elastic.NewBulkIndexRequest().Index(route.TargetIndex).Type(route.TargetType).Id(genDocID(msg)).Doc(msg.DmlMsg.Data)
		}
		reqs = append(reqs, req)
	}
	return output.sendBulkRequests(reqs)
}

func genDocID(msg *core.Msg) string {
	pks := []string{}
	for _, v := range msg.DmlMsg.Pks {
		pks = append(pks, fmt.Sprint(v))
	}
	return strings.Join(pks, "_")
}

func (output *ElasticsearchOutput) sendBulkRequests(reqs []elastic.BulkableRequest) error {
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
			if result.Status >= 200 && result.Status <= 299 {
				// tags: [pipelineName, index, action(index/create/delete/update)].
				// indices created in 6.x only allow a single-type per index, so we don't need the type as a tag.
				metrics.OutputCounter.WithLabelValues(output.pipelineName, result.Index, action, "", "").Add(1)
			}
		}
	}

	failedResponses := bulkResponse.Failed()
	if len(failedResponses) == 0 {
		return nil
	}

	canRetry := false
	for _, failedResponse := range failedResponses {
		if failedResponse.Status == http.StatusTooManyRequests {
			canRetry = true
			break
		} else if output.config.IgnoreBadRequest {
			// TODO do we need a metcris in prometheus for this?
			log.Infof("[output_elasticsearch] Received and ignored an error from server, status: [%d], type: %s, reason: %s.", failedResponse.Status, failedResponse.Error.Type, failedResponse.Error.Reason)
		} else {
			log.Panicf("[output_elasticsearch] Received an error from server, status: [%d], type: %s, reason: %s.", failedResponse.Status, failedResponse.Error.Type, failedResponse.Error.Reason)
		}
	}
	if !canRetry {
		return nil
	}
	// when the server returns 429, it must be that all requests have failed.
	time.Sleep(5 * time.Second)
	return output.sendBulkRequests(reqs)
}
