/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package invoke -
package invoke

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"functioncore/pkg/cli/utils"
)

// GetEventBytes get event bytes from eventJSON or eventPath
func GetEventBytes(eventJSON string, eventPath string) ([]byte, error) {
	if eventJSON != "" && eventPath != "" {
		return nil, errors.New("please use event flag or eventPath flag, not both")
	}

	if eventJSON == "" && eventPath == "" {
		eventJSON = `{"request": "Hello World!"}`
		return []byte(eventJSON), nil
	}

	if eventPath != "" {
		eventBytes, err := getEventBytesFromFile(eventPath)
		if err != nil {
			return nil, err
		}
		return eventBytes, nil
	}

	if !json.Valid([]byte(eventJSON)) {
		return nil, fmt.Errorf("invalid json")
	}
	return []byte(eventJSON), nil
}

func getEventBytesFromFile(eventPath string) ([]byte, error) {
	var (
		eventBytes []byte
		err        error
	)

	eventFile, err := os.OpenFile(eventPath, os.O_RDONLY, 0400)
	if err != nil {
		return nil, fmt.Errorf("failed to open event file, %s", err)
	}
	defer eventFile.Close()
	eventBytes, err = ioutil.ReadAll(eventFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read event file, %s", err)
	}
	if !json.Valid(eventBytes) {
		return nil, fmt.Errorf("invalid json")
	}
	if eventBytes, err = utils.ConvertToUTF8(eventBytes); err != nil {
		return nil, err
	}

	return eventBytes, nil
}
