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

package utils

import (
	"fmt"
	"os"

	"github.com/hashicorp/go-getter"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func GetExecutable(url string, dir string, name string) (string, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 755); err != nil {
			return "", errors.Trace(err)
		}
	}

	dstFileName := fmt.Sprintf("%s/%s", dir, name)

	pwd, err := os.Getwd()
	if err != nil {
		return "", errors.Trace(err)
	}

	log.Infof("[registry] downloading plugin pwd %s, dstFileName: %s from %s", pwd, dstFileName, url)

	client := getter.Client{
		Src:     url,
		Dst:     dstFileName,
		Dir:     false,
		Mode:    getter.ClientModeFile,
		Getters: getter.Getters,
		Pwd:     pwd,
	}
	if err := client.Get(); err != nil {
		return "", errors.Trace(err)
	}
	return dstFileName, nil
}
