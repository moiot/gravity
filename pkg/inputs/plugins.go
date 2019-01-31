package inputs

import (
	"github.com/moiot/gravity/pkg/inputs/mongo"
	_ "github.com/moiot/gravity/pkg/inputs/mongobatch"
	_ "github.com/moiot/gravity/pkg/inputs/mongostream"
	"github.com/moiot/gravity/pkg/inputs/mysql"
	_ "github.com/moiot/gravity/pkg/inputs/mysqlbatch"
	_ "github.com/moiot/gravity/pkg/inputs/mysqlstream"
	_ "github.com/moiot/gravity/pkg/inputs/tidb_kafka"
)

const (
	Mongo = mongo.Name
	Mysql = mysql.Name
)
