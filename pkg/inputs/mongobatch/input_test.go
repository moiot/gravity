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

package mongobatch

import (
	"testing"

	"github.com/moiot/gravity/pkg/core"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/mongo_test"
	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2/bson"
)

type fakeEmitter struct {
	count int
}

func (e *fakeEmitter) Emit(msg *core.Msg) error {
	if msg.DmlMsg != nil {
		e.count++
		log.Infof("msg.DmlMsg.Data: %v", msg.DmlMsg.Data)
	}
	close(msg.Done)
	return nil
}

func (e *fakeEmitter) Close() error {
	return nil
}

type fakeRouter struct {
	db  string
	col string
}

func (r *fakeRouter) Exists(msg *core.Msg) bool {
	return msg.Database == r.db && msg.Table == r.col
}

func TestMongoInput(t *testing.T) {
	r := require.New(t)

	plugin, err := registry.GetPlugin(registry.InputPlugin, Name)
	r.NoError(err)

	source := mongo_test.TestConfig()

	config := Config{
		Source:              &source,
		BatchSize:           1,
		WorkerCnt:           1,
		ChunkThreshold:      1000,
		BatchPerSecondLimit: 100,
	}

	configData := utils.MustAny2Map(&config)

	r.NoError(plugin.Configure(t.Name(), configData))

	mongoInput := plugin.(*mongoBatchInput)

	em := &fakeEmitter{}
	router := &fakeRouter{db: "test", col: "test"}
	session, err := mongo.CreateMongoSession(&source)
	session.DB("_gravity").C("gravity_positions").Remove(bson.M{"name": t.Name()})

	positionCache, err := mongoInput.NewPositionCache()
	r.NoError(err)

	r.NoError(err)
	db := session.DB("test")
	db.DropDatabase()

	for i := 0; i < 100; i++ {
		r.NoError(db.C("test").Insert(bson.M{"_id": i}))
	}

	c := db.C("test")
	query := map[string]interface{}{
		"_id": map[string]interface{}{
			"$gte": 0,
			"$lte": 99,
		},
	}
	iter := c.Find(query).
		Sort("_id").
		Limit(100).
		Hint("_id").
		Iter()

	results := make([]map[string]interface{}, 101)
	for i := range results {
		results[i] = make(map[string]interface{})
	}

	count := 0
	for iter.Next(results[count]) {
		count++
	}

	r.NoError(iter.Err())

	r.Equal(100, count)

	r.NoError(positionCache.Start())
	r.NoError(mongoInput.Start(em, router, positionCache))

	mongoInput.Wait()
	r.Equal(100, em.count)
}
