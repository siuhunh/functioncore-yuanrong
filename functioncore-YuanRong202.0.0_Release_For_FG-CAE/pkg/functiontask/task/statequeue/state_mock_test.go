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

package statequeue

import (
	"encoding/json"
	"fmt"

	"functioncore/pkg/common/logger/log"
)

type state struct {
	Count int `json:"count"`
}

func unmarshalState(rawState string) *state {
	var s *state
	if rawState == "" {
		return &state{Count: 0}
	}
	err := json.Unmarshal([]byte(rawState), &s)
	if err != nil {
		log.GetLogger().Errorf("err, rawState: %s", rawState)
	}
	return s
}

func marshalState(s *state) string {
	rawState, err := json.Marshal(s)
	if err != nil {
		log.GetLogger().Errorf("err, state: %s", fmt.Sprintf("%+v", s))
	}
	return string(rawState)
}
