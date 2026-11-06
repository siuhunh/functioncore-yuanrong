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

package frontend

import (
	"errors"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/metadata"
)

const (
	// functionsPrefix function in etcd prefix
	functionsPrefix = "/sn/functions"
)

func processFunctionEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		if err := metadata.ProcessUpdate(event.Key, event.Value, event.Source); err != nil {
			return err
		}
		return nil
	case etcd3.DELETE:
		if err := metadata.ProcessDelete(event.Key, event.Source); err != nil {
			return err
		}
		return nil
	case etcd3.ERROR:
		return errors.New("get unexpected result when watching etcd")
	default:
		return errors.New("undefined etcd event")
	}
}

// key: /sn/functions/business/<businessID>/tenant/<tenantID>/function/<functionName>/version/<version>
func isFunction(key string) bool {
	strs := strings.Split(key, urnutils.FunctionKeySep)

	if len(strs) != urnutils.FuncETCDKeyLength {
		return false
	}

	if strs[2] != "functions" || strs[5] != "tenant" || strs[7] != "function" {
		return false
	}

	return true
}
