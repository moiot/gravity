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
	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/registry"
)

const MemRepoName = "mem-repo"

type memPosition struct {
	meta PositionMeta
	v    string
}

type memRepo struct {
	positionRepoModels map[string]memPosition
}

func init() {
	registry.RegisterPlugin(registry.PositionRepo, MemRepoName, &memRepo{}, true)
}

func (repo *memRepo) Configure(pipelineName string, data map[string]interface{}) error {
	return nil
}

func (repo *memRepo) Init() error {
	repo.positionRepoModels = make(map[string]memPosition)
	return nil
}

func (repo *memRepo) Get(pipelineName string) (PositionMeta, string, bool, error) {
	p, ok := repo.positionRepoModels[pipelineName]
	if ok {
		if err := p.meta.Validate(); err != nil {
			return PositionMeta{}, "", true, errors.Trace(err)
		} else {
			return p.meta, p.v, true, nil
		}
	} else {
		return PositionMeta{}, "", false, nil
	}
}

func (repo *memRepo) Put(pipelineName string, meta PositionMeta, v string) error {
	meta.Name = pipelineName
	if err := meta.Validate(); err != nil {
		return errors.Trace(err)
	}

	repo.positionRepoModels[pipelineName] = memPosition{meta: meta, v: v}
	return nil
}

func (repo *memRepo) Delete(pipelineName string) error {
	delete(repo.positionRepoModels, pipelineName)
	return nil
}

func (repo *memRepo) Close() error {
	return nil
}
