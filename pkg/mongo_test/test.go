package mongo_test

import (
	"os"
	"strconv"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

// see https://stackoverflow.com/a/44342358 for mgo and mongo replication init
func InitReplica(session *mgo.Session) {
	// Session mode should be monotonic as the default session used by mgo is primary which performs all operations on primary.
	// Since the replica set has not been initialized yet, there wont be a primary and the operation (in this case, replSetInitiate) will just timeout
	session.SetMode(mgo.Monotonic, true)
	result := bson.M{}
	err := session.Run("replSetInitiate", &result)
	if err != nil {
		if (result["codeName"] != "AlreadyInitialized") && result["code"] != 23 {
			panic(err.Error())
		}
	}
	log.Info("mongo replSet initialized")
}
