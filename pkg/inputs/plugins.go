package inputs

import (
	_ "github.com/moiot/gravity/pkg/inputs/mongo"
	_ "github.com/moiot/gravity/pkg/inputs/mongooplog"
	_ "github.com/moiot/gravity/pkg/inputs/mysql"
	_ "github.com/moiot/gravity/pkg/inputs/mysqlbatch"
	_ "github.com/moiot/gravity/pkg/inputs/mysqlstream"
	_ "github.com/moiot/gravity/pkg/inputs/tidb_kafka"
)
