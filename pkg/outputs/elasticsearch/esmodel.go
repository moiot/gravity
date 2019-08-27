package elasticsearch

import (
	"context"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/metrics"
	"github.com/moiot/gravity/pkg/outputs/routers"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

const (
	EsModelName = "esmodel"
)

type EsModelPluginConfig struct {
	ServerConfig     *ElasticsearchServerConfig `mapstructure:"server" json:"server"`
	Routes           []map[string]interface{}   `mapstructure:"routes" json:"routes"`
	IgnoreBadRequest bool                       `mapstructure:"ignore-bad-request" json:"ignore-bad-request"`
}

type EsModelOutput struct {
	pipelineName string
	config       *EsModelPluginConfig
	client       *elastic.Client
	router       routers.EsModelRouter
}

func init() {
	registry.RegisterPlugin(registry.OutputPlugin, EsModelName, &EsModelOutput{}, false)
}

func (output *EsModelOutput) Configure(pipelineName string, data map[string]interface{}) error {
	// setup output
	output.pipelineName = pipelineName

	// setup plugin config
	pluginConfig := EsModelPluginConfig{}

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

		route, ok := output.router.Match(msg)
		if !ok {
			continue
		}

		if len(msg.DmlMsg.Pks) == 0 {
			if route.IgnoreNoPrimaryKey {
				continue
			} else {
				return errors.Errorf("[output_elasticsearch] Table must have at least one primary key, database: %s, table: %s.", msg.Database, msg.Table)
			}
		}

		index := route.IndexName
		if index == "" {
			index = genIndexName(msg.Table)
		}
		var req elastic.BulkableRequest
		if msg.DmlMsg.Operation == core.Delete {
			req = elastic.NewBulkDeleteRequest().
				Index(index).
				Type(route.TypeName).
				Id(genDocID(msg))
		} else {
			req = elastic.NewBulkIndexRequest().
				Index(index).
				Type(route.TypeName).
				Id(genDocID(msg)).
				Doc(msg.DmlMsg.Data)
		}
		reqs = append(reqs, req)
	}
	return output.sendBulkRequests(reqs)
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
					log.Warnf("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, details: %s.", result.Index, marshalError(result.Error))
					status = http.StatusBadRequest
				} else {
					// 200/201/404(delete) -> 200 because the request is successful
					status = http.StatusOK
				}
				metrics.OutputCounter.WithLabelValues(output.pipelineName, result.Index, action, string(status), "").Add(1)
			} else if result.Status == http.StatusTooManyRequests {
				// when the server returns 429, it must be that all requests have failed.
				return errors.Errorf("[output_elasticsearch] The remote server returned an error: (429) Too Many Requests.")
			} else {
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
