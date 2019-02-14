package mongooplog

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/consts"
)

const OplogCheckerCollectionName = "heartbeat"

type OplogChecker struct {
	pipelineName string
	session      *mgo.Session
	sourceHost   string
	ctx          context.Context
}

type OplogHeartbeat struct {
	ID   bson.ObjectId `bson:"_id,omitempty"`
	Name string        `bson:"name"`
	T    string        `bson:"t"`
}

func (checker *OplogChecker) Run() {
	checker.session.SetMode(mgo.Primary, true)
	c := checker.session.DB(consts.GravityDBName).C(OplogCheckerCollectionName)

	heartbeat := OplogHeartbeat{}

	err := c.Find(nil).One(&heartbeat)
	if err != nil {
		if err == mgo.ErrNotFound {
			log.Infof("[oplog_checker] no heartbeat doc found")
			err = c.Insert(&OplogHeartbeat{Name: "gravity", T: time.Now().Format(time.RFC3339Nano)})
			if err != nil {
				log.Infof("[oplog_checker] insert error: %v", err)
			}
		} else {
			log.Errorf("[oplog_checker] err: %v", err)
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			change := bson.M{"$set": bson.M{"t": time.Now().Format(time.RFC3339Nano)}}
			err := c.Update(bson.M{"name": "gravity"}, change)
			if err != nil {
				log.Errorf("[oplog_checker] failed to update heartbeat: %v", err)
			}

		case <-checker.ctx.Done():
			ticker.Stop()
			log.Info("[oplog_checker] stopped")
			return
		}
	}

}

func (checker *OplogChecker) Stop() {

}

func NewOplogChecker(session *mgo.Session, sourceHost string, pipelineName string, ctx context.Context) *OplogChecker {
	if pipelineName == "" {
		log.Fatalf("[oplog_checker] pipeline name is empty")
	}

	checker := OplogChecker{session: session, sourceHost: sourceHost, pipelineName: pipelineName, ctx: ctx}
	return &checker
}
