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
	Name        string
	Stage       config.InputMode
	Value       interface{} `bson:"-" json:"-"`
	ValueString string      `bson:"value" json:"value"`
	UpdateTime  time.Time
}

func (p Position) ValidateWithoutValue() error {
	if p.Stage != config.Stream && p.Stage != config.Batch {
		return errors.Errorf("invalid position stage: %v", p.Stage)
	}

	if p.Name == "" {
		return errors.Errorf("empty name")
	}

	return nil
}

func (p Position) ValidateWithValue() error {
	if err := p.ValidateWithoutValue(); err != nil {
		return errors.Trace(err)
	}

	if p.Value == nil {
		return errors.Errorf("empty position value: %v", p.Value)
	}

	return nil
}

func (p Position) ValidateWithValueString() error {
	if err := p.ValidateWithoutValue(); err != nil {
		return errors.Trace(err)
	}

	if p.ValueString == "" {
		return errors.Errorf("empty value string")
	}

	return nil
}
