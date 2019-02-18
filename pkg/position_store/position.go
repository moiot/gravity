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

package position_store

import (
	"time"

	"github.com/juju/errors"

	"github.com/moiot/gravity/pkg/config"
)

type PositionMeta struct {
	// Version is the schema version of position
	Version string `bson:"version" json:"version"`
	// Name is the unique name of a pipeline
	Name       string
	Stage      config.InputMode
	UpdateTime time.Time
}

func (meta PositionMeta) Validate() error {
	if meta.Stage != config.Stream && meta.Stage != config.Batch {
		return errors.Errorf("invalid position stage: %v", meta.Stage)
	}

	if meta.Name == "" {
		return errors.Errorf("empty name")
	}

	return nil
}

type Position struct {
	PositionMeta
	Value interface{} `bson:"-" json:"-"`
}

func (p Position) Validate() error {
	if err := p.PositionMeta.Validate(); err != nil {
		return errors.Trace(err)
	}

	if p.Value == nil {
		return errors.Errorf("empty position value: %v", p.Value)
	}

	return nil
}
