package consts

const (
	MySQLInternalDBName = "mysql"
	OldDrcDBName        = "drc"
	TxnTagTableName     = "_gravity_txn_tags"
	DDLTag              = "/*gravityDDL*/"
)

var GravityDBName = "_gravity"

func IsInternalDBTraffic(schema string) bool {
	return schema == OldDrcDBName || schema == GravityDBName
}
