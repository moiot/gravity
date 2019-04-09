package elasticsearch

import (
	"context"
	"encoding/json"
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

type ElasticsearchServerAuth struct {
	Username string `mapstructure:"username" toml:"username" json:"username"`
	Password string `mapstructure:"password" toml:"password" json:"password"`
}

type ElasticsearchServerConfig struct {
	URLs  []string                 `mapstructure:"urls" toml:"urls" json:"urls"`
	Sniff bool                     `mapstructure:"sniff" toml:"sniff" json:"sniff"`
	Auth  *ElasticsearchServerAuth `mapstructure:"auth" toml:"auth" json:"auth"`
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
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(serverConfig.URLs...),
		elastic.SetSniff(serverConfig.Sniff),
	}

	if auth := serverConfig.Auth; auth != nil {
		options = append(options, elastic.SetBasicAuth(auth.Username, auth.Password))
	}

	client, err := elastic.NewClient(options...)
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

		index := route.TargetIndex
		if index == "" {
			index = msg.Table
		}
		var req elastic.BulkableRequest
		if msg.DmlMsg.Operation == core.Delete {
			req = elastic.NewBulkDeleteRequest().
				Index(index).
				Type(route.TargetType).
				Id(genDocID(msg))
		} else {
			req = elastic.NewBulkIndexRequest().
				Index(index).
				Type(route.TargetType).
				Id(genDocID(msg)).
				Doc(msg.DmlMsg.Data)
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

func marshalError(err *elastic.ErrorDetails) string {
	bytes, _ := json.Marshal(err)
	return string(bytes)
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
			} else if result.Status == http.StatusNotFound && action == "delete" {
				// delete but not found, just ignore it.
				metrics.OutputCounter.WithLabelValues(output.pipelineName, result.Index, action, "", "").Add(1)
				continue
			} else if result.Status == http.StatusTooManyRequests {
				// when the server returns 429, it must be that all requests have failed.
				time.Sleep(5 * time.Second)
				return output.sendBulkRequests(reqs)
			} else if output.config.IgnoreBadRequest {
				// TODO do we need a metcris in prometheus for this?
				log.Infof("[output_elasticsearch] Received and ignored an error from server, status: [%d], index: %s, details: %s.", result.Status, result.Index, marshalError(result.Error))
			} else {
				log.Panicf("[output_elasticsearch] Received an error from server, status: [%d], index: %s, details: %s.", result.Status, result.Index, marshalError(result.Error))
			}
		}
	}
	return nil
}
