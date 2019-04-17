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
	"testing"

	"github.com/moiot/gravity/pkg/registry"

	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/mysql_test"
	"github.com/stretchr/testify/require"
)

func TestMysqlPositionRepo_GetPut(t *testing.T) {
	r := require.New(t)

	dbCfg := mysql_test.SourceDBConfig()

	repoConfig := NewMySQLRepoConfig("", dbCfg)
	plugin, err := registry.GetPlugin(registry.PositionRepo, repoConfig.Type)
	r.NoError(err)

	r.NoError(plugin.Configure(t.Name(), repoConfig.Config))
	repo := plugin.(PositionRepo)
	r.NoError(repo.Init())

	// delete it first
	r.NoError(repo.Delete(t.Name()))

	_, _, exist, err := repo.Get(t.Name())
	r.NoError(err)

	r.False(exist)

	// put first value
	meta := PositionMeta{
		Name:  t.Name(),
		Stage: config.Stream,
	}
	r.NoError(repo.Put(t.Name(), meta, "test"))

	meta, v, exist, err := repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal("test", v)
	r.Equal(config.Stream, meta.Stage)

	// put another value
	r.NoError(repo.Put(t.Name(), meta, "test2"))

	meta, v, exist, err = repo.Get(t.Name())
	r.NoError(err)
	r.True(exist)
	r.Equal("test2", v)

	// put an invalid value
	err = repo.Put(t.Name(), meta, "")
	r.NotNil(err)
}
