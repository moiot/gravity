package position_store

import (
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/consts"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/utils/retry"
)

const mongoPositionCollection = "positions"

type mongoPositionStore struct {
	sync.Mutex

	name     string
	position MongoPosition

	session *mgo.Session

	stop       chan struct{}
	wg         sync.WaitGroup
	closeOnce  sync.Once
	dirty      bool
	updateTime time.Time
}

func (store *mongoPositionStore) Stage() config.InputMode {
	return config.Stream
}

func (store *mongoPositionStore) Position() Position {
	store.Lock()
	defer store.Unlock()

	return Position{
		Name:       store.name,
		Stage:      config.Stream,
		Raw:        store.position,
		UpdateTime: store.updateTime,
	}
}

func (store *mongoPositionStore) Update(pos Position) {
	store.Lock()
	defer store.Unlock()

	store.name = pos.Name
	store.updateTime = pos.UpdateTime
	store.position = pos.Raw.(MongoPosition)
}

func (store *mongoPositionStore) Clear() {
	store.Lock()
	defer store.Unlock()

	collection := store.session.DB(consts.GravityDBName).C(mongoPositionCollection)
	collection.Remove(bson.M{"name": store.name})
	store.position = MongoPosition{}
}

func (store *mongoPositionStore) Start() error {
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()

		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				store.savePosition()

			case <-store.stop:
				store.savePosition()
				log.Infof("[mongoPositionStore] stop...")
				return
			}
		}
	}()
	return nil
}

type positionEntity struct {
	Name          string `bson:"name"`
	Stage         string `bson:"stage"`
	MongoPosition `bson:",inline"`
	LastUpdate    string `bson:"last_update"`
}

func (store *mongoPositionStore) savePosition() {
	store.Lock()
	defer store.Unlock()

	if !store.dirty {
		return
	}

	collection := store.session.DB(consts.GravityDBName).C(mongoPositionCollection)
	err := retry.Do(func() error {
		_, err := collection.Upsert(bson.M{"name": store.name}, bson.M{
			"$set": bson.M{
				"stage":            string(config.Stream),
				"start_position":   store.position.StartPosition,
				"current_position": store.position.CurrentPosition,
				"last_update":      time.Now().Format(time.RFC3339Nano),
			},
		})
		return err
	}, 3, retry.DefaultSleep)
	if err != nil {
		log.Fatalf("[mongoPositionStore.savePosition] fails. err: %s", err)
	}
	store.updateTime = time.Now()
	store.dirty = false
	log.Infof("[mongoPositionStore.savePosition] succeed: %s", store.position.toJson())
}

func (store *mongoPositionStore) loadPosition() {
	collection := store.session.DB(consts.GravityDBName).C(mongoPositionCollection)
	err := retry.Do(func() error {
		pos := positionEntity{}
		err := collection.Find(bson.M{"name": store.name}).One(&pos)
		if err == mgo.ErrNotFound {
			log.Infof("[mongoPositionStore.loadPosition] position for %s is empty", store.name)
			return nil
		} else if err != nil {
			log.Warnf("[mongoPositionStore.loadPosition] error query db. %s", err)
			return err
		} else {
			store.updateTime, err = time.Parse(time.RFC3339Nano, pos.LastUpdate)
			if err != nil {
				return err
			}
			store.position.CurrentPosition = pos.CurrentPosition
			store.position.StartPosition = pos.StartPosition
			return nil
		}
	}, 3, retry.DefaultSleep)

	if err != nil {
		log.Fatalf("[mongoPositionStore.loadPosition] error query db. %s", errors.Trace(err))
	}
}

func (store *mongoPositionStore) Close() {
	store.closeOnce.Do(func() {
		log.Infof("[mongoPositionStore] stop...")
		close(store.stop)
		store.wg.Wait()
		store.session.Close()
		log.Infof("[mongoPositionStore] stopped")
	})
}

func (store *mongoPositionStore) Get() config.MongoPosition {
	store.Lock()
	defer store.Unlock()
	return store.position.CurrentPosition
}

func (store *mongoPositionStore) Put(position config.MongoPosition) {
	store.Lock()
	defer store.Unlock()
	store.position.CurrentPosition = position
	store.dirty = true
	log.Debugf("[position_store] put current: %v", store.position.CurrentPosition)
}

func NewMongoPositionStore(pipelineName string, conn *config.MongoConnConfig, pos *config.MongoPosition) (*mongoPositionStore, error) {
	session, err := mongo.CreateMongoSession(conn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	session.SetMode(mgo.Primary, true)
	collection := session.DB(consts.GravityDBName).C(mongoPositionCollection)
	err = collection.EnsureIndex(mgo.Index{
		Key:    []string{"name"},
		Unique: true,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	store := &mongoPositionStore{
		name:     pipelineName,
		position: MongoPosition{},
		session:  session,
		stop:     make(chan struct{}),
	}

	store.loadPosition()

	var runtimePipelinePosition = store.position

	// If the start position in the spec is different than the start position last saved,
	// we use the start position in the spec;
	// otherwise, we just start normally
	log.Infof("[NewMongoPositionStore] start position in spec %v, runtime start position %v", pos, runtimePipelinePosition.StartPosition)
	if pos != nil && *pos != runtimePipelinePosition.StartPosition {
		log.Infof("[NewMongoPositionStore]: spec violation!")
		store.position = MongoPosition{*pos, *pos}
	}

	return store, nil
}
