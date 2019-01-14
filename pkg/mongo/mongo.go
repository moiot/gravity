package mongo

import (
	"fmt"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"

	"github.com/moiot/gravity/pkg/config"
)

func CreateMongoSession(cfg *config.MongoConnConfig) (*mgo.Session, error) {

	username := ""
	if cfg.Username != "" {
		username = cfg.Username
	}

	password := ""
	if cfg.Password != "" {
		password = cfg.Password
	}

	host := "localhost"
	if cfg.Host != "" {
		host = cfg.Host
	}

	port := 27017
	if cfg.Port != 0 {
		port = cfg.Port
	}

	db := cfg.Database

	var url string
	if username == "" || password == "" {
		url = fmt.Sprintf("mongodb://%s:%d/%s", host, port, db)
	} else {
		url = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s", username, password, host, port, db)
	}

	//If not specified, the connection will timeout, probably because the replica set has not been initialized yet.
	if cfg.Direct {
		url += "?connect=direct"
	}

	session, err := mgo.Dial(url)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open mongo session: %s", url)
	}

	log.Infof("connected to %s:%d", host, port)
	return session, nil
}
