package mongo

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/mongo/gtm"
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
		return nil, errors.Annotatef(err, "failed to open mongo session: %s, err: %v", url, err.Error())
	}

	log.Infof("connected to %s", url)
	return session, nil
}

const deadSignalCollection = "dead_signals"

func SendDeadSignal(session *mgo.Session, pipeline string) error {
	c := session.DB(consts.GravityDBName).C(deadSignalCollection)
	return errors.Trace(c.Insert(bson.M{
		"name": pipeline,
	}))
}

func IsDeadSignal(op *gtm.Op, pipeline string) bool {
	if op.GetDatabase() == consts.GravityDBName && op.GetCollection() == deadSignalCollection {
		if op.Data["name"] == pipeline {
			return true
		}
	}
	return false
}

func ListAllCollections(session *mgo.Session) map[string][]string {
	collections := make(map[string][]string)
	dbs, err := session.DatabaseNames()
	if err != nil {
		log.Fatalf("[mongoBatchInput] error list database. %s", errors.ErrorStack(err))
	}
	for _, db := range dbs {
		colls, err := session.DB(db).CollectionNames()
		if err != nil {
			log.Fatalf("[mongoBatchInput] error list collections for %s. err: %s", db, errors.ErrorStack(err))
		}
		for _, coll := range colls {
			collections[db] = append(collections[db], coll)
		}
	}
	return collections
}

func IsEmpty(session *mgo.Session, db string, collection string) bool {
	var t interface{}
	err := session.DB(db).C(collection).Find(nil).Select(bson.M{"_id": 1}).Limit(1).One(&t)
	return err == mgo.ErrNotFound
}

func Count(session *mgo.Session, db string, collection string) int {
	if IsEmpty(session, db, collection) {
		return 0
	}
	var ret bson.M
	if err := session.DB(db).Run(bson.M{"collStats": collection}, &ret); err != nil {
		log.Fatalf("fail to query collStats for %s.%s. err: %s", db, collection, errors.ErrorStack(err))
	}
	return ret["count"].(int)
}

type MinMax struct {
	Min bson.ObjectId
	Max bson.ObjectId
}

func BucketAuto(session *mgo.Session, db string, collection string, sampleCnt int, bucketCnt int) ([]MinMax, error) {
	start := time.Now()
	iter := session.DB(db).C(collection).Pipe([]bson.M{
		{
			"$sample": bson.M{"size": sampleCnt},
		},
		{
			"$bucketAuto": bson.M{
				"groupBy": "$_id",
				"buckets": bucketCnt,
			},
		},
	}).Iter()

	defer iter.Close()

	var record bson.M
	var ret []MinMax
	for iter.Next(&record) {
		t := record["_id"].(bson.M)
		ret = append(ret, MinMax{
			Min: t["min"].(bson.ObjectId),
			Max: t["max"].(bson.ObjectId),
		})
	}

	if err := iter.Err(); err != nil {
		return ret, errors.Trace(err)
	}

	log.Infof("BucketAuto finished. sample: %d, bucket: %d, eclipsed: %s", sampleCnt, bucketCnt, time.Since(start))
	return ret, nil
}
