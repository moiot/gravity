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

type Position struct {
	// Version is the schema version of position
	Version string
	// Name is the unique name of a pipeline
	Name       string
	Stage      config.InputMode
	Value      interface{}
	UpdateTime time.Time
}

func (p *Position) Validate() error {
	if p.Stage != config.Stream && p.Stage != config.Batch {
		return errors.Errorf("invalid position stage: %v", p.Stage)
	}

	if p.Value == nil {
		return errors.Errorf("empty position value: %v", p.Value)
	}

	if p.Name == "" {
		return errors.Errorf("empty name")
	}

	return nil
}

type PositionWithValueString struct {
	Version    string
	Name       string
	Stage      string
	Value      string
	UpdateTime time.Time
}

func (m *PositionWithValueString) Validate() error {
	if m.Stage != string(config.Stream) && m.Stage != string(config.Batch) {
		return errors.Errorf("invalid stage: %v", m.Stage)
	}

	if m.Value == "" {
		return errors.Errorf("empty position value")
	}

	if m.Name == "" {
		return errors.Errorf("empty name")
	}

	return nil
}
