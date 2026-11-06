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

package funcinsmanager

import (
	"sync"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/faas-scheduler/scheduler/common"
)

type instanceCache struct {
	instances sync.Map
	observer  common.Observer
}

func createInstanceCache() *instanceCache {
	return &instanceCache{
		instances: sync.Map{},
	}
}

func (cache *instanceCache) addInstance(instance *common.Instance) {
	_, ok := cache.instances.Load(instance.InstanceKey)
	if ok && !(instance.IsWebsocketCae && instance.WillDelete) { // instance already exist, no need add
		return
	}

	cache.instances.LoadOrStore(instance.InstanceKey, instance)
	observer := cache.observer
	eventType := common.AddInstance
	if observer != nil {
		if instance.WillDelete && instance.IsWebsocketCae {
			eventType = common.DeleteInstance
		}
		observer.OnObserved(instance, common.InstanceEventType(eventType))
	}
	log.GetLogger().Infof("addInstance %s with eventType %d success", instance.InstanceKey, eventType)
}

func (cache *instanceCache) recoverInstance(instanceReq *schedulerapi.CheckInstanceReq) {
	instance := &common.Instance{
		InstanceReq: instanceReq,
	}
	observer := cache.observer
	if observer != nil {
		observer.OnObserved(instance, common.RecoverInstance)
	} else {
		log.GetLogger().Warnf("observer is nil for func %s", instanceReq.FuncKeyWithRes)
	}
}

func (cache *instanceCache) deleteInstance(instance *common.Instance) {
	_, ok := cache.instances.Load(instance.InstanceKey)
	if !ok {
		log.GetLogger().Errorf("DeleteInstance failed, instance %s not exist", instance.InstanceKey)
		return
	}
	cache.instances.Delete(instance.InstanceKey)
	observer := cache.observer
	if observer != nil {
		observer.OnObserved(instance, common.DeleteInstance)
	}

	log.GetLogger().Infof("DeleteInstance %s success", instance.InstanceKey)
}

func (cache *instanceCache) addObserver(observer common.Observer) {
	cache.observer = observer
	cache.instances.Range(func(k, v interface{}) bool {
		if v, ok := v.(*common.Instance); ok {
			observer.OnObserved(v, common.AddInstance)
		}
		return true
	})
}

func (cache *instanceCache) deleteObserver() {
	cache.observer = nil
}
