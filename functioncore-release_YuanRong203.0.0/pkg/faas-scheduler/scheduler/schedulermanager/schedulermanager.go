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

// Package schedulermanager contains instance scheduler manager logic
package schedulermanager

import (
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/scheduler/insscheduler"
	"functioncore/pkg/faas-scheduler/types"
)

// Manager -
type Manager struct {
	schedulers map[string]*insscheduler.Scheduler
	sync.RWMutex
}

// CreateManager -
func CreateManager() *Manager {
	return &Manager{
		schedulers: map[string]*insscheduler.Scheduler{},
	}
}

// GetScheduler -
func (manager *Manager) GetScheduler(funcKeyWithRes string) *insscheduler.Scheduler {
	manager.RLock()
	scheduler, _ := manager.schedulers[funcKeyWithRes]
	manager.RUnlock()
	return scheduler
}

// CreateScheduler -
func (manager *Manager) CreateScheduler(funcKeyWithRes string, funcMeta *types.FunctionMetaInfo,
	register common.AddObserver, possibleRecovering bool) (*insscheduler.Scheduler, error) {
	manager.Lock()
	defer manager.Unlock()
	scheduler, ok := manager.schedulers[funcKeyWithRes]
	if ok {
		return scheduler, nil
	}

	scheduler = insscheduler.CreateScheduler(funcKeyWithRes, funcMeta)
	register(funcKeyWithRes, scheduler)
	// after adding all function instance from cache, recover all connection status by querying worker
	funcMetaData := funcMeta.FuncMetaData
	if possibleRecovering && (commonUtils.IsWebSocketFunc(funcMetaData.BusinessType,
		funcMetaData.EnableRemoteDebug, funcMeta.ExtendedMetaData.EnableWebsocket)) {
		log.GetLogger().Infof("funcKeyWithRes: %s | start to recover connection status",
			urnutils.AnonymizeTenantKey(funcKeyWithRes))
		scheduler.RecoverConnStatus(funcKeyWithRes)
	}
	scheduler.Start()
	manager.schedulers[funcKeyWithRes] = scheduler
	log.GetLogger().Infof("create scheduler successfully, funcKeyWithRes: %s",
		urnutils.AnonymizeTenantKey(funcKeyWithRes))
	return scheduler, nil
}

// DeleteScheduler -
func (manager *Manager) DeleteScheduler(funcKey string, deleter common.DeleteObserver) {
	manager.Lock()
	for funcKeyWithRes, scheduler := range manager.schedulers {
		key := urnutils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithRes)
		if funcKey == key {
			manager.deleteScheduler(funcKeyWithRes, scheduler, deleter)
		}
	}
	manager.Unlock()
}

// UpdateFuncMeta -
func (manager *Manager) UpdateFuncMeta(funcKey string, info *types.FunctionMetaInfo, register common.AddObserver) {
	manager.RLock()
	defer manager.RUnlock()
	var targetScheduler *insscheduler.Scheduler
	var funcKeyWithRes string
	for keyWithRes, scheduler := range manager.schedulers {
		funcKeyInMap := urnutils.GetFuncKeyFromFuncKeyWithResource(keyWithRes)
		if funcKey == funcKeyInMap {
			targetScheduler = scheduler
			funcKeyWithRes = keyWithRes
			break
		}
	}
	if targetScheduler == nil {
		if !commonUtils.IsWebSocketCae(info.FuncMetaData.BusinessType, info.ExtendedMetaData.EnableWebsocket) {
			log.GetLogger().Warnf("funcKey: %s| failed to get scheduler cache", funcKey)
			return
		}
		// Cpu and mem of cae use default 0
		funcKeyWithRes = urnutils.GetFuncKeyWithResource(funcKey, "0", "0")
		log.GetLogger().Infof("create scheduler for %s", funcKeyWithRes)
		manager.RUnlock()
		newScheduler, err := manager.CreateScheduler(funcKeyWithRes, info, register, false)
		manager.RLock()
		if err != nil {
			log.GetLogger().Errorf("failed to create schedule for %s, err:%s", funcKeyWithRes, err.Error())
			return
		}
		manager.schedulers[funcKeyWithRes] = newScheduler
		targetScheduler = newScheduler
	}
	anonymizedKey := urnutils.AnonymizeTenantKey(funcKeyWithRes)
	curBusinessType := targetScheduler.GetBusinessType()
	if info.FuncMetaData.BusinessType != curBusinessType {
		log.GetLogger().Infof("businessType not equal for %s", funcKeyWithRes)
		// business type change remove previous scheduler and add a new one
		targetScheduler.Lock()
		delete(manager.schedulers, funcKeyWithRes)
		newScheduler, err := manager.CreateScheduler(funcKeyWithRes, info, register, false)
		if err != nil {
			log.GetLogger().Errorf("funcKeyWithRes: %s| err: %s | failed to update businessType",
				anonymizedKey, err.Error())
			// use original scheduler if failed to create a new one
			manager.schedulers[funcKeyWithRes] = targetScheduler
			targetScheduler.Unlock()
			return
		}
		manager.schedulers[funcKeyWithRes] = newScheduler
		targetScheduler.Unlock()
		log.GetLogger().Infof("funcKeyWithRes: %s | updated businessType successfully", anonymizedKey)
	} else {
		targetScheduler.UpdateFuncMeta(info)
		log.GetLogger().Infof("funcKeyWithRes: %s | updated concurrency successfully", anonymizedKey)
	}
}

// GcScheduler check scheduler need gc, release goroutine
func (manager *Manager) GcScheduler(funcKeyWithRes string, deleter common.DeleteObserver) {
	manager.Lock()
	scheduler, ok := manager.schedulers[funcKeyWithRes]
	if !ok {
		manager.Unlock()
		return
	}

	if scheduler.NeedGc() {
		manager.deleteScheduler(funcKeyWithRes, scheduler, deleter)
	}
	manager.Unlock()
}

func (manager *Manager) deleteScheduler(funcKeyWithRes string,
	scheduler *insscheduler.Scheduler, deleter common.DeleteObserver) {
	scheduler.Stop()
	delete(manager.schedulers, funcKeyWithRes)
	deleter(funcKeyWithRes)
	log.GetLogger().Infof("delete scheduler funcKeyWithRes %s",
		urnutils.AnonymizeTenantKey(funcKeyWithRes))
}
