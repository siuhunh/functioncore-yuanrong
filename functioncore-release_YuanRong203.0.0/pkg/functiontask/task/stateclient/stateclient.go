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

// Package stateclient describes the interface for state client.
package stateclient

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commonUtils "functioncore/pkg/core-sdk/common"
	"functioncore/pkg/functionstate/client"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
)

// StateCli state client
var StateCli *client.StateClient

// StateClient is the interface that read and write state.
type StateClient interface {
	Set(key string, value string, expireTime int) common.TaskError
	Get(key string) (string, common.TaskError)
	DeleteStateValueAndStateMapping(key string) common.TaskError
	DeleteState(key, funcName string) common.TaskError
	SetTTL(key string, TTL int) common.TaskError
	Scan(pattern string) (map[string]string, common.TaskError)
	SetStateAndResult(key *metadata.Metadata, state string, TTL int,
		req *common.Request, result *common.Response) common.TaskError
	IsNotExist(err error) bool
	StateExists(key string) (bool, common.TaskError)
	GetNumsByServiceID(pattern string) (int, common.TaskError)
	IsStateIDInHash(stateHashKey string, stateID string) (bool, common.TaskError)
}

// HExists if filed exists in hash return true
func HExists(key string, field string) (bool, snerror.SNError) {
	if StateCli == nil {
		return false, snerror.New(commonUtils.StateClientErrCode, commonUtils.StateClientErrMsg)
	}
	result, err := StateCli.HExists(key, field)
	if err != nil {
		log.GetLogger().Errorf("state client HExists method exec error. key: %s, field: %s, error: %s", key, field, err)
		return false, snerror.New(commonUtils.StateClientErrCode, err.Error())
	}
	return result, nil
}

// HDel remove fields from hash by state client
func HDel(key string, fields ...string) (int, snerror.SNError) {
	if StateCli == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(commonUtils.StateClientErrCode, commonUtils.StateClientErrMsg)
	}
	result, err := StateCli.HDel(key, fields...)
	if err != nil {
		log.GetLogger().Errorf("state client HDel method exec error. key: %s, fields: %s, error: %s", key, fields, err)
		return 0, snerror.New(commonUtils.StateClientErrCode, err.Error())
	}
	log.GetLogger().Debugf("state client HDel method exec succeed. key: %s, fields: %s", key, fields)
	return result, nil
}

// HSetNX set the value of the hash table field when the field does not exist by state client
func HSetNX(key, filed, value string) (bool, snerror.SNError) {
	if StateCli == nil {
		log.GetLogger().Error("state client is nil")
		return false, snerror.New(commonUtils.StateClientErrCode, commonUtils.StateClientErrMsg)
	}
	result, err := StateCli.HSetNX(key, filed, value)
	if err != nil {
		log.GetLogger().Errorf("state client HSetNX method exec error. key: %s, error: %s", key, err)
		return false, snerror.New(commonUtils.StateClientErrCode, err.Error())
	}
	return result, nil
}
