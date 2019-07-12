package env

import "time"

func init() {
	StartTime = time.Now()
}

var StartTime time.Time

var PipelineName string
