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

// Package willdeleteworker is the cache for will delete worker
package willdeleteworker

import (
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
)

var instance *Cache
var once sync.Once

// GetCache to get willdeleteworker cache
func GetCache() *Cache {
	once.Do(func() {
		instance = &Cache{}
	})
	return instance
}

// Cache is the cache for will delete worker
type Cache struct {
	// key: funcSvcName   value: *deleteInstanceCache
	functions sync.Map
}

type deleteInstanceCache struct {
	// key is instanceID
	workerInstanceMap map[string]v1alpha1.WorkerInstance
	sync.RWMutex
}

func (dc *deleteInstanceCache) addWorkerInstance(instanceID string, instance v1alpha1.WorkerInstance) {
	dc.workerInstanceMap[instanceID] = instance
}

func (dc *deleteInstanceCache) deleteWorkerInstance(instanceID string) {
	delete(dc.workerInstanceMap, instanceID)
}

func (w *Cache) getWillDeleteWorkerCache(funcSvcID string) (*deleteInstanceCache, bool) {
	object, exit := w.functions.Load(funcSvcID)
	if !exit {
		return nil, false
	}
	dic, ok := object.(*deleteInstanceCache)
	return dic, ok
}

func (w *Cache) createWillDeleteWorkerCache(funcSvcID string) *deleteInstanceCache {
	obj, _ := w.functions.LoadOrStore(funcSvcID, &deleteInstanceCache{
		workerInstanceMap: make(map[string]v1alpha1.WorkerInstance, 0),
	})
	dic, _ := obj.(*deleteInstanceCache)
	return dic
}

// DeleteFuncWillDeleteWorker will delete funcSvcID function's will delete worker
func (w *Cache) DeleteFuncWillDeleteWorker(funcSvcID string) {
	_, exist := w.functions.LoadAndDelete(funcSvcID)
	if exist {
		log.GetLogger().Infof("[willdeleteworker cache] succeed to delete function, funcSvcID: %s", funcSvcID)
	}
}

// AddWillDeleteWorkerInstances will add delete worker in funcSvcID's willdeleteworker cache
func (w *Cache) AddWillDeleteWorkerInstances(funcSvcID string, instances []v1alpha1.WorkerInstance) {
	var dic *deleteInstanceCache
	var exist bool
	dic, exist = w.getWillDeleteWorkerCache(funcSvcID)
	if !exist {
		dic = w.createWillDeleteWorkerCache(funcSvcID)
	}
	dic.Lock()
	defer dic.Unlock()
	for _, workerInstance := range instances {
		dic.addWorkerInstance(workerInstance.InstanceID, workerInstance)
		log.GetLogger().Infof("[willdeleteworker cache] succeed to add will delete worker instance , "+
			"funcSvcID: %s, instanceID: %s", funcSvcID, workerInstance.InstanceID)
	}
}

// DeleteWillDeleteWorkerInstance will delete instanceID's instance in willdeleteworker cache
func (w *Cache) DeleteWillDeleteWorkerInstance(funcSvcID string, instanceID string) {
	dic, exist := w.getWillDeleteWorkerCache(funcSvcID)
	if !exist {
		return
	}
	dic.Lock()
	defer dic.Unlock()
	dic.deleteWorkerInstance(instanceID)
	log.GetLogger().Infof("[willdeleteworker cache] succeed to delete will delete worker instance , "+
		"funcSvcID: %s, instanceID: %s", funcSvcID, instanceID)
}

// GetWillDeleteWorkerInstances will get funcSvcID's delete workers in willdeleteworker cache
func (w *Cache) GetWillDeleteWorkerInstances(funcSvcID string) []v1alpha1.WorkerInstance {
	workerInstances := make([]v1alpha1.WorkerInstance, 0)
	dic, exist := w.getWillDeleteWorkerCache(funcSvcID)
	if !exist {
		return workerInstances
	}
	dic.RLock()
	defer dic.RUnlock()
	for _, workerInstance := range dic.workerInstanceMap {
		workerInstances = append(workerInstances, workerInstance)
	}
	return workerInstances
}

// GetWillDeleteWorkerInstancesAndDelete will get funcSvcID's delete workers in willdeleteworker cache and delete them
func (w *Cache) GetWillDeleteWorkerInstancesAndDelete(funcSvcID string) []v1alpha1.WorkerInstance {
	workerInstances := make([]v1alpha1.WorkerInstance, 0)
	if object, loaded := w.functions.LoadAndDelete(funcSvcID); loaded {
		dic, ok := object.(*deleteInstanceCache)
		if ok {
			dic.RLock()
			defer dic.RUnlock()
			for _, workerInstance := range dic.workerInstanceMap {
				workerInstances = append(workerInstances, workerInstance)
			}
		}
	}
	return workerInstances
}
