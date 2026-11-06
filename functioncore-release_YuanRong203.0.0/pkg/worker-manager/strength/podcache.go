/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package strength is cache pod name, state and func id
package strength

import "sync"

const defaultLen = 10

var podCache = &PodCache{
	instances: make(map[string]*State, defaultLen),
	rwMutex:   sync.RWMutex{},
}

// StateType define pod state type
type StateType int

const (
	// BeforeSpecialized define pod is before specialized
	BeforeSpecialized StateType = iota
	// AfterSpecialized define pod is after specialized
	AfterSpecialized
)

// State define pod state
type State struct {
	FuncSvcID string
	StateType StateType
}

// PodCache instance state
type PodCache struct {
	instances map[string]*State
	rwMutex   sync.RWMutex
}

// GetPodCache get instance state
func GetPodCache() *PodCache {
	return podCache
}

// SetState set state
func (is *PodCache) SetState(podName, funcSvcID string, stateType StateType) {
	is.rwMutex.Lock()
	is.instances[podName] = &State{
		FuncSvcID: funcSvcID,
		StateType: stateType,
	}
	is.rwMutex.Unlock()
}

// GetState get state by pod name
func (is *PodCache) GetState(podName string) (*State, bool) {
	is.rwMutex.RLock()
	state, ok := is.instances[podName]
	is.rwMutex.RUnlock()
	return state, ok
}

// DelState delete state
func (is *PodCache) DelState(podName string) {
	is.rwMutex.Lock()
	delete(is.instances, podName)
	is.rwMutex.Unlock()
}
