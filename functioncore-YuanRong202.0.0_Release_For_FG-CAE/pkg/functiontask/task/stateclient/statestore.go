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

package stateclient

import (
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
)

// StateStore represents an in-memory state store.
type StateStore struct {
	storage kvstorage.Storage
}

// NewStateStore return a new state store with storage engine.
func NewStateStore(storage kvstorage.Storage) *StateStore {
	return &StateStore{
		storage: storage,
	}
}

// Set the state by state id.
func (s *StateStore) Set(key string, value string, expireTime int) common.TaskError {
	if !config.Conf.PersistentState {
		return nil
	}
	return s.storage.Set(key, value, expireTime)
}

// Get the state by state id.
func (s *StateStore) Get(key string) (string, common.TaskError) {
	return s.storage.Get(key)
}

// DeleteStateValueAndStateMapping delete the state by state id.
func (s *StateStore) DeleteStateValueAndStateMapping(key string) common.TaskError {
	return s.DeleteState(key, "")
}

// SetStateAndResult Sets the state and result.
func (s *StateStore) SetStateAndResult(key *metadata.Metadata, state string, TTL int,
	req *common.Request, result *common.Response) common.TaskError {
	return nil
}

// DeleteState the state.
func (s *StateStore) DeleteState(key, funcName string) common.TaskError {
	if !config.Conf.PersistentState {
		return nil
	}
	return s.storage.Delete(key)
}

// SetTTL sets the ttl for the state.
func (s *StateStore) SetTTL(key string, ttl int) common.TaskError {
	return nil
}

// Scan returns state ids with specific service id.
func (s *StateStore) Scan(pattern string) (map[string]string, common.TaskError) {
	return s.storage.Scan(pattern)
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s *StateStore) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	return s.storage.GetNumsByServiceID(pattern)
}

// IsNotExist check whether it is a state not found error.
func (s *StateStore) IsNotExist(err error) bool {
	return err != nil && err.Error() == common.StateIDNotFoundMsg
}

// StateExists get stateID status directly
func (s *StateStore) StateExists(key string) (bool, common.TaskError) {
	return s.storage.Exists(key)
}

// IsStateIDInHash check stateID is in hash
func (s *StateStore) IsStateIDInHash(stateHashKey string, stateID string) (bool, common.TaskError) {
	return true, nil
}
