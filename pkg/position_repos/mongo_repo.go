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
	"context"
	"time"

	"github.com/moiot/gravity/pkg/consts"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"

	jsoniter "github.com/json-iterator/go"

	"github.com/mitchellh/mapstructure"

	"github.com/moiot/gravity/pkg/registry"
	"github.com/moiot/gravity/pkg/utils"

	"github.com/moiot/gravity/pkg/config"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
)

const (
	mongoPositionCollection = "gravity_positions"
	Version                 = "1.0"
	MongoRepoName           = "mongo-repo"
)

var mongoPositionDB = consts.GravityDBName
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
	client      *mongo.Client
}

type PositionWrapper struct {
	PositionMeta
	MongoValue string `bson:"value" json:"value"`
}

func init() {
	registry.RegisterPlugin(registry.PositionRepo, MongoRepoName, &mongoPositionRepo{}, false)
}

func (repo *mongoPositionRepo) Configure(pipelineName string, data map[string]interface{}) error {
	if err := mapstructure.Decode(data, &repo.mongoConfig); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (repo *mongoPositionRepo) Init() error {
	client, err := utils.CreateMongoClient(&repo.mongoConfig)
	if err != nil {
		return errors.Trace(err)
	}
	repo.client = client

	collection := client.Database(mongoPositionDB).Collection(mongoPositionCollection)

	indexes := collection.Indexes()

	_, _ = indexes.CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bsonx.Doc{{"name", bsonx.Int32(1)}},
			Options: options.Index().SetBackground(false).SetUnique(true),
		})
	return nil
}

func (repo *mongoPositionRepo) Get(pipelineName string) (PositionMeta, string, bool, error) {
	collection := repo.client.Database(mongoPositionDB).Collection(mongoPositionCollection)
	ret := collection.FindOne(context.Background(), bson.M{"name": pipelineName})
	if err := ret.Err(); err != nil {
		return PositionMeta{}, "", false, errors.Trace(err)
	}
	mongoRet := MongoPositionRet{}
	if err := ret.Decode(&mongoRet); err != nil {
		if err == mongo.ErrNoDocuments {
			return PositionMeta{}, "", false, nil
		}
		return PositionMeta{}, "", false, errors.Trace(err)
	}

	// backward compatible with old mongoRet schema
	if mongoRet.Version == "" {
		oldPosition := PositionEntity{}
		oldRet := collection.FindOne(context.Background(), bson.M{"name": pipelineName})
		if err := oldRet.Err(); err != nil {
			return PositionMeta{}, "", false, errors.Trace(err)
		}
		if err := oldRet.Decode(&oldPosition); err != nil {
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

	collection := repo.client.Database(mongoPositionDB).Collection(mongoPositionCollection)
	_, err := collection.UpdateOne(
		context.Background(),
		bson.M{"name": pipelineName},
		bson.M{
			"$set": bson.M{
				"version":     meta.Version,
				"stage":       string(meta.Stage),
				"value":       v,
				"last_update": time.Now().Format(time.RFC3339Nano),
			},
		},
		options.Update().SetUpsert(true))
	return errors.Trace(err)
}

func (repo *mongoPositionRepo) Delete(pipelineName string) error {
	_, err := repo.client.Database(mongoPositionDB).Collection(mongoPositionCollection).DeleteOne(
		context.Background(),
		bson.M{"name": pipelineName},
	)
	return errors.Trace(err)
}

func (repo *mongoPositionRepo) Close() error {
	repo.client.Disconnect(context.Background())
	return nil
}

func NewMongoRepoConfig(source *config.MongoConnConfig) *config.GenericPluginConfig {
	cfg := config.GenericPluginConfig{}
	cfg.Type = MongoRepoName
	cfg.Config = utils.MustAny2Map(source)
	return &cfg
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
