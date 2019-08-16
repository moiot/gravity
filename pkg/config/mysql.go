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

package config

import (
	"time"

	"github.com/juju/errors"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host" mapstructure:"host"`
	Location string `toml:"location" json:"location" mapstructure:"location"`
	Username string `toml:"username" json:"username" mapstructure:"username"`
	Password string `toml:"password" json:"password" mapstructure:"password"`
	Port     int    `toml:"port" json:"port" mapstructure:"port"`
	Schema   string `toml:"schema" json:"schema" mapstructure:"schema"`
	// Timeout for establishing connections, aka dial timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	Timeout string `toml:"timeout" json:"timeout" mapstructure:"timeout"`
	// I/O read timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	ReadTimeout string `toml:"read-timeout" json:"read-timeout" mapstructure:"read-timeout"`

	// I/O write timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	WriteTimeout string `toml:"write-timeout" json:"write-timeout" mapstructure:"write-timeout"`

	MaxIdle                int           `toml:"max-idle" json:"max-idle" mapstructure:"max-idle"`
	MaxOpen                int           `toml:"max-open" json:"max-open" mapstructure:"max-open"`
	MaxLifeTimeDurationStr string        `toml:"max-life-time-duration" json:"max-life-time-duration" mapstructure:"max-life-time-duration"`
	MaxLifeTimeDuration    time.Duration `toml:"-" json:"-" mapstructure:"-"`
}

func (dbc *DBConfig) ValidateAndSetDefault() error {
	// Sets the location for time.Time values (when using parseTime=true). "Local" sets the system's location. See time.LoadLocation for details.
	// Note that this sets the location for time.Time values but does not change MySQL's time_zone setting.
	// For that see the time_zone system variable, which can also be set as a DSN parameter.
	if dbc.Location == "" {
		dbc.Location = time.Local.String()
	}

	// set default values of connection related settings
	// assume the response time of db is 2ms, then
	// then a single connection can have tps of 500 TPS
	if dbc.MaxOpen == 0 {
		dbc.MaxOpen = 200
	}

	if dbc.MaxIdle == 0 {
		dbc.MaxIdle = dbc.MaxOpen
	}

	var err error
	if dbc.MaxLifeTimeDurationStr == "" {
		dbc.MaxLifeTimeDurationStr = "15m"
		dbc.MaxLifeTimeDuration = 15 * time.Minute
	} else {
		dbc.MaxLifeTimeDuration, err = time.ParseDuration(dbc.MaxLifeTimeDurationStr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if dbc.Timeout == "" {
		dbc.Timeout = "5s"
	}

	if dbc.ReadTimeout == "" {
		dbc.ReadTimeout = "5s"
	}

	if dbc.WriteTimeout == "" {
		dbc.WriteTimeout = "5s"
	}

	return nil
}

type MySQLBinlogPosition struct {
	BinLogFileName string `toml:"binlog-name" json:"binlog-name" mapstructure:"binlog-name"`
	BinLogFilePos  uint32 `toml:"binlog-pos" json:"binlog-pos" mapstructure:"binlog-pos"`
	BinlogGTID     string `toml:"binlog-gtid" json:"binlog-gtid" mapstructure:"binlog-gtid"`
}
