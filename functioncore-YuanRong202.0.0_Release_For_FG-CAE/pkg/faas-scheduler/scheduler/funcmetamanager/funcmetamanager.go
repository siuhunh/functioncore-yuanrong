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

// Package funcmetamanager contains function metadata manager logic
package funcmetamanager

import (
	"errors"
	"fmt"
	"sync"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

// CreateManager -
func CreateManager(fetcher common.FuncMetaFetcher) *Manager {
	return &Manager{
		funcMetaFetcher: fetcher,
		funcMetaCache:   make(map[string]*types.FunctionMetaInfo),
	}
}

// Manager -
type Manager struct {
	funcMetaFetcher common.FuncMetaFetcher
	funcMetaCache   map[string]*types.FunctionMetaInfo
	funcMetaLock    sync.RWMutex
}

// GetFuncMeta first get function metadata from cache
// if function metadata not in cache, try to fetch function metadata from remote database
func (manager *Manager) GetFuncMeta(funcKey string) (*types.FunctionMetaInfo, error) {
	info, err := manager.GetFuncMetaFromCache(funcKey)
	if err == nil {
		return info, nil
	}
	log.GetLogger().Warnf("failed to GetFuncMetaFromCache, funcKey %s err %s, try get from database",
		urnutils.AnonymizeTenantKey(funcKey), err.Error())
	if manager.funcMetaFetcher == nil {
		return nil, errors.New("funcMetaFetcher is nil")
	}
	info, err = manager.funcMetaFetcher(funcKey)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("GetFuncMeta from database success, funcKey %s concurrency %d",
		urnutils.AnonymizeTenantKey(funcKey), info.InstanceMetaData.ConcurrentNum)
	manager.funcMetaLock.Lock()
	manager.funcMetaCache[funcKey] = info
	manager.funcMetaLock.Unlock()
	return info, err
}

// GetFuncMetaFromCache -
func (manager *Manager) GetFuncMetaFromCache(funcKey string) (*types.FunctionMetaInfo, error) {
	manager.funcMetaLock.RLock()
	obj, exist := manager.funcMetaCache[funcKey]
	manager.funcMetaLock.RUnlock()
	if !exist {
		return nil, fmt.Errorf("not find metadata from cache %s",
			urnutils.AnonymizeTenantKey(funcKey))
	}
	return obj, nil
}

// UpdateFuncMeta -
func (manager *Manager) UpdateFuncMeta(funcKey string, info *types.FunctionMetaInfo) bool {
	manager.funcMetaLock.Lock()
	if info.Source == etcd3.CAEMeta {
		old, ok := manager.funcMetaCache[funcKey]
		if ok && old.Source == etcd3.Meta {
			manager.funcMetaLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip update from cae, key %s", funcKey)
			return false
		}

	}
	manager.funcMetaCache[funcKey] = info
	manager.funcMetaLock.Unlock()

	log.GetLogger().Debugf("UpdateFuncMeta success, funcKey %s, source %s",
		urnutils.AnonymizeTenantKey(funcKey), info.Source)
	return true
}

// DeleteFuncMeta -
func (manager *Manager) DeleteFuncMeta(funcKey string, source etcd3.EtcdType) bool {
	manager.funcMetaLock.Lock()
	if source == etcd3.CAEMeta {
		old, ok := manager.funcMetaCache[funcKey]
		if ok && old.Source == etcd3.Meta {
			manager.funcMetaLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip delete from cae, key %s", funcKey)
			return false
		}
	}
	delete(manager.funcMetaCache, funcKey)
	manager.funcMetaLock.Unlock()
	log.GetLogger().Infof("DeleteFuncMeta  funcKey %s, source %s", urnutils.AnonymizeTenantKey(funcKey), source)
	return true
}
