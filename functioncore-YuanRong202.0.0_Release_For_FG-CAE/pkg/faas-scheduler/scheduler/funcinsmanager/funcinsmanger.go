/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package funcinsmanager contains function instance manager logic
package funcinsmanager

import (
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

// Manager function instance manager
type Manager struct {
	instanceCaches map[string]*instanceCache
	sync.RWMutex
}

// CreateManager -
func CreateManager() *Manager {
	return &Manager{
		instanceCaches: map[string]*instanceCache{},
	}
}

func (manager *Manager) ignoreInstance(info *types.WorkerInstance) bool {
	// ignore functiontask
	if info.InstanceID == "" {
		return true
	}

	// ignore stateful worker
	if info.StateID != "" {
		return true
	}

	return false
}

func (manager *Manager) convertInstanceInfo(info *types.WorkerInstance) *common.Instance {
	instance := &common.Instance{
		InstanceKey: info.InstanceID,
		Applier:     info.Applier,
		PodIP:       info.PodIP,
		PodPort:     info.PodPort,
		NodeIP:      info.NodeIP,
		NodePort:    info.NodePort,
	}
	return instance
}

// AddInstance -
func (manager *Manager) AddInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	if manager.ignoreInstance(info) {
		return
	}

	instance := manager.convertInstanceInfo(info)
	cache := manager.getOrCreateInstanceCache(funcKeyWithRes)
	cache.addInstance(instance)
}

// DeleteInstance -
func (manager *Manager) DeleteInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	if manager.ignoreInstance(info) {
		return
	}

	cache := manager.getInstanceCache(funcKeyWithRes)
	if cache != nil {
		instance := manager.convertInstanceInfo(info)
		cache.deleteInstance(instance)
	}
}

// AddObserver -
func (manager *Manager) AddObserver(funcKeyWithRes string, observer common.Observer) {
	log.GetLogger().Infof("add function: %s observer", urnutils.AnonymizeTenantKey(funcKeyWithRes))
	cache := manager.getOrCreateInstanceCache(funcKeyWithRes)
	cache.addObserver(observer)
}

// DeleteObserver -
func (manager *Manager) DeleteObserver(funcKeyWithRes string) {
	cache := manager.getInstanceCache(funcKeyWithRes)
	if cache != nil {
		log.GetLogger().Infof("DeleteObserver funcKeyWithRes %s", urnutils.AnonymizeTenantKey(funcKeyWithRes))
		cache.deleteObserver()
	}
}

func (manager *Manager) getInstanceCache(funcKeyWithRes string) *instanceCache {
	manager.RLock()
	cache, _ := manager.instanceCaches[funcKeyWithRes]
	manager.RUnlock()
	return cache
}

func (manager *Manager) getOrCreateInstanceCache(funcKeyWithRes string) *instanceCache {
	cache := manager.getInstanceCache(funcKeyWithRes)
	if cache != nil {
		return cache
	}

	manager.Lock()
	cache, ok := manager.instanceCaches[funcKeyWithRes]
	if ok {
		manager.Unlock()
		return cache
	}
	cache = createInstanceCache()
	manager.instanceCaches[funcKeyWithRes] = cache
	manager.Unlock()
	return cache
}
