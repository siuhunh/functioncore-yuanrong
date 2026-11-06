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

// Package corestorage implements storage interface.
package corestorage

import (
	"functioncore/pkg/common/logger/log"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
)

// Storage is a remote Storage.
type Storage struct {
}

// NewStorage returns a remote Storage.
func NewStorage() kvstorage.Storage {
	return new(Storage)
}

// Set value by key with a expireTime(seconds)
func (s *Storage) Set(key string, value string, expireTime int) common.TaskError {
	err := functioncore.SetKeyWithTTL(key, value, expireTime)
	if err != nil {
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
}

// Get return value by key.
func (s *Storage) Get(key string) (string, common.TaskError) {
	state, err := functioncore.GetState(key)
	if err != nil {
		return state, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return state, nil
}

// Delete delete value by key.
func (s *Storage) Delete(key string) common.TaskError {
	err := functioncore.DeleteState(key)
	if err != nil {
		log.GetLogger().Warnf("delete State error, err: %s", err.Error())
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
}

// Exists check key status.
func (s *Storage) Exists(key string) (bool, common.TaskError) {
	exists, err := functioncore.Exists(key)
	if err != nil {
		return false, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return exists, nil
}

// Scan returns value with specific key pattern.
func (s *Storage) Scan(pattern string) (map[string]string, common.TaskError) {
	result, err := functioncore.ScanState(pattern)
	if err != nil {
		return result, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return result, nil
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s *Storage) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	tenantID, serviceID, _ := metadata.ExtractMetaKey(pattern)
	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, "")
	result, err := functioncore.HLen(stateHashKey)
	if err != nil {
		return 0, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return result, nil
}
