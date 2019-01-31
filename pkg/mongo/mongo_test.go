package mongo

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

	db = s.DB("test")
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

	r.Equal(cnt, Count(session, db.Name, t.Name()))
	r.NoError(coll.DropCollection())
	r.Equal(0, Count(session, db.Name, t.Name()))
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
