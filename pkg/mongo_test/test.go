package mongo_test

import (
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/config"
)

func TestConfig() config.MongoConnConfig {
	cfg := config.MongoConnConfig{
		Host:   "127.0.0.1",
		Port:   27017,
		Direct: true,
	}

	sourceHost, ok := os.LookupEnv("MONGO_HOST")
	if ok {
		cfg.Host = sourceHost
	}

	sourceMongoPort, ok := os.LookupEnv("MONGO_PORT")
	if ok {
		p, err := strconv.Atoi(sourceMongoPort)
		if err != nil {
			log.Fatalf("invalid port")
		}
		cfg.Port = p
	}

	sourceMongoUser, ok := os.LookupEnv("MONGO_USER")
	if ok {
		cfg.Username = sourceMongoUser
	}

	sourceMongoPass, ok := os.LookupEnv("MONGO_PASSWORD")
	if ok {
		cfg.Password = sourceMongoPass
	}

	return cfg
}
