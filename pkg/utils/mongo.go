/*
 *
 * // Copyright 2019 , Beijing Mobike Technology Co., Ltd.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package utils

import (
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
	url := cfg.URI()

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

func ListAllUserCollections(session *mgo.Session) map[string][]string {
	collections := make(map[string][]string)
	dbs, err := session.DatabaseNames()
	if err != nil {
		log.Fatalf("[mongoBatchInput] error list database. %s", errors.ErrorStack(err))
	}
	for _, db := range dbs {
		colls, err := session.DB(db).CollectionNames()
		if err != nil {
			log.Errorf("[mongoBatchInput] error list collections for %s. err: %s", db, errors.ErrorStack(err))
			continue
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

type collStats struct {
	Count int64 `json:"count"`
}

func Count(session *mgo.Session, db string, collection string) int64 {
	if IsEmpty(session, db, collection) {
		return 0
	}

	var ret collStats
	if err := session.DB(db).Run(bson.M{"collStats": collection}, &ret); err != nil {
		log.Fatalf("fail to query collStats for %s.%s. err: %s", db, collection, errors.ErrorStack(err))
	}

	return ret.Count
}

type MinMax struct {
	Min interface{}
	Max interface{}
}

func GetMinMax(session *mgo.Session, db string, collection string) (MinMax, error) {
	var min bson.M
	var max bson.M
	var ret MinMax

	if err := session.DB(db).C(collection).Find(nil).Sort("_id").One(&min); err != nil {
		return ret, errors.Trace(err)
	}

	if err := session.DB(db).C(collection).Find(nil).Sort("-_id").One(&max); err != nil {
		return ret, errors.Trace(err)
	}

	ret.Min = min["_id"]
	ret.Max = max["_id"]
	return ret, nil
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
			Min: t["min"],
			Max: t["max"],
		})
	}

	if err := iter.Err(); err != nil {
		return ret, errors.Trace(err)
	}

	log.Infof("BucketAuto finished. sample: %d, bucket: %d, eclipsed: %s", sampleCnt, bucketCnt, time.Since(start))
	return ret, nil
}
