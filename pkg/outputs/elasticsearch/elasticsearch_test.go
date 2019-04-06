package elasticsearch

import (
	"context"
	"net/http"
	"testing"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/utils"
	"github.com/stretchr/testify/require"
)

const ResponseBadRequest = `
{
    "errors": true,
    "items": [
        {
            "index": {
                "_index": "test-index",
                "_type": "doc",
                "_id": "1",
                "status": 400,
                "error": {
                    "type": "action_request_validation_exception",
                    "reason": "Validation Failed: 1: script or doc is missing;2: script or doc is missing;"
                }
            }
		}
	]
}
`

const ResponseTooManyRequests = `
{
    "errors": true,
    "items": [
        {
            "index": {
                "_index": "test-index",
                "_type": "doc",
                "_id": "1",
                "status": 429,
                "error": {
                    "type": "es_rejected_execution_exception",
                    "reason": "rejected execution of org.elasticsearch.transport.TransportService$7@3ee7e07f on EsThreadPoolExecutor[bulk, queue capacity = 200, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@5a67119[Running, pool size = 32, active threads = 32, queued tasks = 200, completed tasks = 794346]]"
                }
            }
		}
	]
}
`

const ResponseOK = `
{
	"took": 30,
	"errors": false,
	"items": [
	   {
		  "index": {
			 "_index": "test-index",
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
}
`

func TestBadRequest(t *testing.T) {
	r := require.New(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(ResponseBadRequest))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	srv := &http.Server{Addr: ":9200", Handler: mux}
	defer srv.Shutdown(context.TODO())
	go srv.ListenAndServe()

	output, err := startElasticsearchOutput()
	r.NoError(err)

	r.Panics(func() {
		output.Execute([]*core.Msg{
			{
				Database: "test-db",
				Type:     core.MsgDML,
				Table:    "test-table",
				DmlMsg: &core.DMLMsg{
					Operation: core.Insert,
					Data: map[string]interface{}{
						"id": "abc",
					},
					Pks: map[string]interface{}{
						"id": "abc",
					},
				},
			},
		})
	})
}

func TestIgnoreBadRequest(t *testing.T) {
	r := require.New(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(ResponseBadRequest))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	srv := &http.Server{Addr: ":9200", Handler: mux}
	defer srv.Shutdown(context.TODO())
	go srv.ListenAndServe()

	output, err := startElasticsearchOutput()
	r.NoError(err)
	output.config.IgnoreBadRequest = true

	r.NoError(output.Execute(defaultMsgs()))
}

func TestTooManyRequests(t *testing.T) {
	r := require.New(t)

	counter := &Counter{
		Count: 0,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_bulk", counter.handler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	srv := &http.Server{Addr: ":9200", Handler: mux}
	defer srv.Shutdown(context.TODO())
	go srv.ListenAndServe()

	output, err := startElasticsearchOutput()
	r.NoError(err)

	r.NoError(output.Execute(defaultMsgs()))
	r.Equal(counter.Count, 2)
}

func defaultMsgs() []*core.Msg {
	return []*core.Msg{
		{
			Database: "test-db",
			Type:     core.MsgDML,
			Table:    "test-table",
			DmlMsg: &core.DMLMsg{
				Operation: core.Insert,
				Data: map[string]interface{}{
					"id": "abc",
				},
				Pks: map[string]interface{}{
					"id": "abc",
				},
			},
		},
	}
}

func startElasticsearchOutput() (*ElasticsearchOutput, error) {
	output := &ElasticsearchOutput{}
	err := output.Configure("mock", utils.Struct2Map(
		&ElasticsearchPluginConfig{
			ServerConfig: &ElasticsearchServerConfig{
				URLs:  []string{"http://127.0.0.1:9200"},
				Sniff: false,
			},
			Routes: []map[string]interface{}{
				{
					"match-schema": "test-db",
					"match-table":  "test-table",
					"target-index": "test-index",
				},
			},
		},
	))
	if err != nil {
		return nil, err
	}
	err = output.Start()
	if err != nil {
		return nil, err
	}
	return output, nil
}

type Counter struct {
	Count int
}

func (c *Counter) handler(w http.ResponseWriter, r *http.Request) {
	c.Count++
	if c.Count == 1 {
		w.Write([]byte(ResponseTooManyRequests))
	} else {
		w.Write([]byte(ResponseOK))
	}
	w.WriteHeader(http.StatusOK)
}
