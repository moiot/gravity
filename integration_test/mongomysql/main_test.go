package mongokafka_test

import (
	"os"
	"testing"

	"github.com/moiot/gravity/pkg/utils"

	mgo "gopkg.in/mgo.v2"

	"github.com/moiot/gravity/pkg/consts"

	"github.com/moiot/gravity/pkg/mongo_test"
)

var session *mgo.Session
var db *mgo.Database

func TestMain(m *testing.M) {
	mongoCfg := mongo_test.TestConfig()
	s, err := utils.CreateMongoSession(&mongoCfg)
	if err != nil {
		panic(err)
	}
	session = s
	mongo_test.InitReplica(session)
	defer session.Close()

	db = s.DB("mongomysql")
	if err := db.DropDatabase(); err != nil {
		panic(err)
	}
	if err := s.DB(consts.GravityDBName).DropDatabase(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}
