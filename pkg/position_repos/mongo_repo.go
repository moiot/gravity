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

package position_repos

import (
	"github.com/mitchellh/mapstructure"
	"github.com/moiot/gravity/pkg/mongo"
	"github.com/moiot/gravity/pkg/registry"
	"time"

	"github.com/json-iterator/go"
	"github.com/moiot/gravity/pkg/config"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	mongoPositionDB         = "_gravity"
	mongoPositionCollection = "gravity_positions"
	Version                 = "1.0"
	MongoRepoName = "mongo-repo"
)

var myJson = jsoniter.Config{SortMapKeys: true}.Froze()

//
// MongoPosition and PositionEntity is here to keep backward compatible with previous
// position format
//
type MongoPosition struct {
	StartPosition   config.MongoPosition `json:"start_position" bson:"start_position"`
	CurrentPosition config.MongoPosition `json:"current_position" bson:"current_position"`
}

// PositionEntity is the old format, will be deprecated
type PositionEntity struct {
	Name          string `json:"name" bson:"name"`
	Stage         string `json:"stage" bson:"stage"`
	MongoPosition `json:",inline" bson:",inline"`
	LastUpdate    string `json:"last_update" bson:"last_update"`
}

// MongoPositionRet is the new format
type MongoPositionRet struct {
	Version    string `json:"version" bson:"version"`
	Name       string `json:"name" bson:"name"`
	Stage      string `json:"stage" bson:"stage"`
	Value      string `json:"value" bson:"value"`
	LastUpdate string `json:"last_update" bson:"last_update"`
}

//
// [input.config.position-repo]
// type = "mongo-repo"
// [input.config.position-repo.config]
// host = ...
// port = ...
//
type mongoPositionRepo struct {
	mongoConfig config.MongoConnConfig
	session *mgo.Session
}

type PositionWrapper struct {
	PositionMeta
	MongoValue string `bson:"value" json:"value"`
}

func init() {
	registry.RegisterPlugin(registry.PositionRepo, MongoRepoName, &mongoPositionRepo{}, true)
}

func (repo *mongoPositionRepo) Configure(pipelineName string, data map[string]interface{}) error {
	if err := mapstructure.Decode(data, &repo.mongoConfig); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (repo *mongoPositionRepo) Init() error {
	session, err := mongo.CreateMongoSession(&repo.mongoConfig)
	if err != nil {
		return errors.Trace(err)
	}

	repo.session = session

	session.SetMode(mgo.Primary, true)
	collection := session.DB(mongoPositionDB).C(mongoPositionCollection)
	err = collection.EnsureIndex(mgo.Index{
		Key:    []string{"name"},
		Unique: true,
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (repo *mongoPositionRepo) Get(pipelineName string) (PositionMeta, string, bool, error) {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	mongoRet := MongoPositionRet{}
	err := collection.Find(bson.M{"name": pipelineName}).One(&mongoRet)
	if err != nil {
		if err == mgo.ErrNotFound {
			return PositionMeta{}, "", false, nil
		}
		return PositionMeta{}, "", false, errors.Trace(err)
	}

	// backward compatible with old mongoRet schema
	if mongoRet.Version == "" {
		oldPosition := PositionEntity{}
		if err := collection.Find(bson.M{"name": pipelineName}).One(&oldPosition); err != nil {
			return PositionMeta{}, "", true, errors.Trace(err)
		}

		s, err := myJson.MarshalToString(&oldPosition.MongoPosition)
		if err != nil {
			return PositionMeta{}, "", true, errors.Trace(err)
		}

		return PositionMeta{Name: pipelineName, Stage: config.InputMode(oldPosition.Stage)}, s, true, nil

	}

	t, err := time.Parse(time.RFC3339Nano, mongoRet.LastUpdate)
	if err != nil {
		return PositionMeta{}, "", true, errors.Trace(err)
	}

	meta := PositionMeta{Name: mongoRet.Name, Stage: config.InputMode(mongoRet.Stage), UpdateTime: t}
	if err := meta.Validate(); err != nil {
		return PositionMeta{}, "", true, errors.Trace(err)
	}

	if mongoRet.Value == "" {
		return PositionMeta{}, "", true, errors.Errorf("empty value")
	}

	return meta, mongoRet.Value, true, nil
}

func (repo *mongoPositionRepo) Put(pipelineName string, meta PositionMeta, v string) error {
	meta.Name = pipelineName
	meta.Version = Version
	if err := meta.Validate(); err != nil {
		return errors.Trace(err)
	}

	if v == "" {
		return errors.Errorf("empty value")
	}

	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	_, err := collection.Upsert(
		bson.M{"name": pipelineName}, bson.M{
			"$set": bson.M{
				"version":     meta.Version,
				"stage":       string(meta.Stage),
				"value":       v,
				"last_update": time.Now().Format(time.RFC3339Nano),
			},
		})
	return errors.Trace(err)
}

func (repo *mongoPositionRepo) Delete(pipelineName string) error {
	collection := repo.session.DB(mongoPositionDB).C(mongoPositionCollection)
	return errors.Trace(collection.Remove(bson.M{"name": pipelineName}))
}

func (repo *mongoPositionRepo) Close() error {
	repo.session.Close()
	return nil
}

// func NewMongoPositionRepo(session *mgo.Session) (core.PositionRepo, error) {
// 	session.SetMode(mgo.Primary, true)
// 	collection := session.DB(mongoPositionDB).C(mongoPositionCollection)
// 	err := collection.EnsureIndex(mgo.Index{
// 		Key:    []string{"name"},
// 		Unique: true,
// 	})
// 	if err != nil {
// 		return nil, errors.Trace(err)
// 	}
//
// 	return &mongoPositionRepo{session: session}, nil
// }
