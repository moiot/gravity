package kafka_test

import (
	"os"
	"strings"
)

func TestBroker() []string {
	sourceDBHost, ok := os.LookupEnv("KAFKA_BROKER")
	if !ok {
		return []string{"localhost:9092"}
	}
	return strings.Split(sourceDBHost, ",")
}
