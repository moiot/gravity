package tidb_kafka

func ParseTimeStamp(tso uint64) uint64 {
	// https://github.com/pingcap/pd/blob/master/tools/pd-ctl/pdctl/command/tso_command.go#L49
	// timstamp in seconds format
	return (tso >> 18) / 1000
}
