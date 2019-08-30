package outputs

import (
	"github.com/moiot/gravity/pkg/outputs/async_kafka"
	"github.com/moiot/gravity/pkg/outputs/elasticsearch"
	"github.com/moiot/gravity/pkg/outputs/esmodel"
	"github.com/moiot/gravity/pkg/outputs/mysql"
	"github.com/moiot/gravity/pkg/outputs/stdout"
)

const (
	AsyncKafka    = async_kafka.Name
	Mysql         = mysql.Name
	Stdout        = stdout.Name
	Elasticsearch = elasticsearch.Name
	EsModel       = esmodel.Name
)
