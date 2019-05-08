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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/mongo_test"
)

var session *mgo.Session
var db *mgo.Database

func TestMain(m *testing.M) {
	mongoCfg := mongo_test.TestConfig()
	s, err := CreateMongoSession(&mongoCfg)
	if err != nil {
		panic(err)
	}
	session = s
	mongo_test.InitReplica(session)

	db = s.DB("test_connection")
	db.DropDatabase()

	ret := m.Run()

	session.Close()
	os.Exit(ret)
}

func TestCount(t *testing.T) {
	r := require.New(t)
	coll := db.C(t.Name())

	cnt := 10
	for i := 0; i < cnt; i++ {
		r.NoError(coll.Insert(bson.M{"foo": i}))
	}

	r.EqualValues(cnt, Count(session, db.Name, t.Name()))
	r.NoError(coll.DropCollection())
	r.EqualValues(0, Count(session, db.Name, t.Name()))
}

func TestBucketAuto(t *testing.T) {
	r := require.New(t)

	coll := db.C(t.Name())
	cnt := 100
	for i := 0; i < cnt; i++ {
		r.NoError(coll.Insert(bson.M{"foo": i}))
	}

	BucketAuto(session, db.Name, t.Name(), 10, 2)
}

func TestGetMinMax(t *testing.T) {
	r := require.New(t)
	coll := db.C(t.Name())

	cnt := 100

	for i := 0; i < cnt; i++ {
		r.NoError(coll.Insert(bson.M{"_id": i}))
	}

	mm, err := GetMinMax(session, db.Name, coll.Name)
	r.NoError(err)
	r.EqualValues(0, mm.Min)
	r.EqualValues(99, mm.Max)
}
