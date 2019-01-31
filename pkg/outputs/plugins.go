package outputs

import (
	"github.com/moiot/gravity/pkg/outputs/async_kafka"
	"github.com/moiot/gravity/pkg/outputs/mysql"
)

const (
	AsyncKafka = async_kafka.Name
	Mysql      = mysql.Name
)
