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

package sql_execution_engine

import (
	"database/sql"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/utils"
)

type InternalTxnTaggerCfg struct {
	TagInternalTxn bool   `mapstructure:"tag-internal-txn" json:"tag-internal-txn"`
	SQLAnnotation  string `mapstructure:"sql-annotation" json:"sql-annotation"`
}

var DefaultInternalTxnTaggerCfg = map[string]interface{}{
	"tag-internal-txn": false,
	"sql-annotation":   "",
}

func ExecWithInternalTxnTag(
	pipelineName string,
	internalTxnTaggerCfg *InternalTxnTaggerCfg,
	db *sql.DB,
	query string,
	args []interface{}) error {

	newQuery := query
	if internalTxnTaggerCfg.SQLAnnotation != "" {
		newQuery = SQLWithAnnotation(query, internalTxnTaggerCfg.SQLAnnotation)
	}

	if !internalTxnTaggerCfg.TagInternalTxn {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("query: %v, args: %v", newQuery, args)
		}
		_, err := db.Exec(newQuery, args...)
		return errors.Annotatef(err, "query: %v, args: %v", query, args)
	}

	//
	// TagInternalTxn is ON
	//
	txn, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = txn.Exec(utils.GenerateTxnTagSQL(pipelineName))
	if err != nil {
		return errors.Trace(err)
	}

	if log.IsLevelEnabled(log.DebugLevel) {
		log.Debugf("query: %v, args: %v", newQuery, args)
	}
	_, err = txn.Exec(query, args...)
	if err != nil {
		txn.Rollback()
		return errors.Annotatef(err, "query: %v, args: %+v", query, args)
	}

	return errors.Trace(txn.Commit())
}
