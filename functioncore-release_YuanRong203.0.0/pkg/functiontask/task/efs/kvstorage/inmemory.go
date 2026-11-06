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

package kvstorage

import (
	"regexp"
	"sync"

	"functioncore/pkg/functiontask/task/common"
)

const defaultSize = 100

// InMemoryStorage represents an in-memory store.
type InMemoryStorage struct {
	values map[string]string
	mutex  sync.RWMutex
}

// NewStorage returns new InMemoryStorage.
func NewStorage() *InMemoryStorage {
	return &InMemoryStorage{
		values: make(map[string]string, defaultSize),
	}
}

// Get the value by key.
func (s *InMemoryStorage) Get(key string) (string, common.TaskError) {
	s.mutex.RLock()
	state, ok := s.values[key]
	if !ok {
		s.mutex.RUnlock()
		return "", common.NewTaskError(common.EStateIDNotFound, common.StateIDNotFoundMsg)
	}
	s.mutex.RUnlock()
	return state, nil
}

// Set the value by key.
func (s *InMemoryStorage) Set(key string, state string, TTL int) common.TaskError {
	s.mutex.Lock()
	s.values[key] = state
	s.mutex.Unlock()
	return nil
}

// Delete the value by key.
func (s *InMemoryStorage) Delete(key string) common.TaskError {
	s.mutex.Lock()
	delete(s.values, key)
	s.mutex.Unlock()
	return nil
}

// Exists get key status directly
func (s *InMemoryStorage) Exists(key string) (bool, common.TaskError) {
	s.mutex.RLock()
	_, ok := s.values[key]
	s.mutex.RUnlock()
	return ok, nil
}

// Scan returns value with specific key pattern.
func (s *InMemoryStorage) Scan(pattern string) (map[string]string, common.TaskError) {
	prefixLen := len(pattern) - 1
	s.mutex.RLock()
	comp := regexp.MustCompile(pattern)
	ids := make(map[string]string, defaultSize)
	for k := range s.values {
		if comp.MatchString(k) {
			ids[k[prefixLen:]] = ""
		}
	}
	s.mutex.RUnlock()
	return ids, nil
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s *InMemoryStorage) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	result, err := s.Scan(pattern)
	if err != nil {
		return 0, err
	}
	return len(result), nil
}
