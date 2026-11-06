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

// Package insscheduler contains function instance scheduler logic
package insscheduler

import (
	"os"
	"strconv"
	"sync"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

func init() {
	interval, err := strconv.Atoi(os.Getenv("SYNC_WEBSOCKET_CONN_INTERVAL"))
	if err != nil {
		log.GetLogger().Warnf("get syncWebsocketConnInterval from env failed:%s", err.Error())
		return
	}
	syncWebsocketConnInterval = interval
}

var (
	syncWebsocketConnInterval = 30
)

// timeout for scaling out of worker manager
const (
	scalingOutTimeout = 15 * time.Minute
)

// Scheduler -
type Scheduler struct {
	funcKeyWithRes    string
	concurrency       int
	businessType      string
	normalInsStrategy strategy
	stopCh            chan struct{}
	sync.Mutex
	enableWebsocket bool
}

// CreateScheduler -
func CreateScheduler(funcKeyWithRes string, funcMeta *types.FunctionMetaInfo) *Scheduler {
	return &Scheduler{
		funcKeyWithRes:    funcKeyWithRes,
		concurrency:       funcMeta.InstanceMetaData.ConcurrentNum,
		businessType:      funcMeta.FuncMetaData.BusinessType,
		normalInsStrategy: createStrategy(funcMeta, funcKeyWithRes),
		stopCh:            make(chan struct{}, 1),
		enableWebsocket:   funcMeta.ExtendedMetaData.EnableWebsocket,
	}
}

// Start -
func (scheduler *Scheduler) Start() {
	interval := time.Duration(config.FConfig.InstanceLeaseConfig.LeaseCheckInterval) * time.Millisecond
	go scheduler.checkLease(interval)
	if commonUtils.IsWebSocketCae(scheduler.businessType, scheduler.enableWebsocket) {
		go scheduler.syncWebsocketConn()
	}
}

// Stop -
func (scheduler *Scheduler) Stop() {
	close(scheduler.stopCh)
}

// OnObserved -
func (scheduler *Scheduler) OnObserved(instanceInfo *common.Instance, eventType common.InstanceEventType) {
	scheduler.Lock()
	if eventType == common.AddInstance {
		if commonUtils.IsWebSocketCae(scheduler.businessType, scheduler.enableWebsocket) {
			scheduler.normalInsStrategy.addInstance(instanceInfo, caeWebSocketInstance)
		} else if instanceInfo.Applier == constants.WorkerManagerApplier {
			scheduler.normalInsStrategy.addInstance(instanceInfo, reservedInstance)
		} else if instanceInfo.Applier == constants.FaasSchedulerApplier {
			scheduler.normalInsStrategy.addInstance(instanceInfo, websocketInstance)
		} else {
			scheduler.normalInsStrategy.addInstance(instanceInfo, normalInstance)
		}
	} else if eventType == common.DeleteInstance {
		scheduler.normalInsStrategy.deleteInstance(instanceInfo.InstanceKey)
	} else if eventType == common.RecoverInstance {
		scheduler.normalInsStrategy.checkInstance(instanceInfo.InstanceReq)
	}
	scheduler.Unlock()
	return
}

// RemovePlaceHolderInstanceForTimeout -
func (scheduler *Scheduler) RemovePlaceHolderInstanceForTimeout(ins *instance, stopChan chan struct{}) {
	placeholderInstanceKey := ins.instanceKey
	log.GetLogger().Infof("start to countdown for removing placeholder instance %s", placeholderInstanceKey)
	select {
	case <-stopChan:
		log.GetLogger().Infof("stop the countdown for removing placeholder instance: %s, "+
			"new generated instance key: %s", placeholderInstanceKey, ins.instanceKey)
		return
	case <-time.After(scalingOutTimeout):
		scheduler.Lock()
		scheduler.normalInsStrategy.deleteInstance(ins.instanceKey)
		scheduler.Unlock()
	}
}

// AddPlaceHolderInstance -
func (scheduler *Scheduler) AddPlaceHolderInstance(
	instanceInfo *common.Instance, rsp *schedulerapi.AcquireInstanceRsp, stopChan chan struct{}) string {
	scheduler.Lock()
	ins := scheduler.normalInsStrategy.addInstance(instanceInfo, websocketInstance)
	rsp.PlaceHolderChan = ins.websocketReadyChan
	rsp.Error.ErrorCode = snerror.GetPlaceHolderInstanceError
	rsp.InstanceKey = ins.instanceKey
	rsp.ProcessorID = 0
	scheduler.Unlock()
	go scheduler.RemovePlaceHolderInstanceForTimeout(ins, stopChan)
	return rsp.InstanceKey
}

// ReleasePlaceHolderInstanceProcessor -
func (scheduler *Scheduler) ReleasePlaceHolderInstanceProcessor(insKey, funcKeyWithRes string, processorID uint32) {
	scheduler.Lock()
	err := scheduler.normalInsStrategy.releaseInstance(&schedulerapi.ReleaseInstanceReq{
		IsWebSocketReq: true,
		InstanceKey:    insKey,
		ProcessorID:    processorID,
		FuncKeyWithRes: funcKeyWithRes,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to release placeholder instance processor, err: %s", err.Error())
	}
	scheduler.Unlock()
}

// ReleasePlaceHolderInstance -
func (scheduler *Scheduler) ReleasePlaceHolderInstance(key string) {
	scheduler.Lock()
	scheduler.normalInsStrategy.deleteInstance(key)
	scheduler.Unlock()
}

// AcquireInstance -
func (scheduler *Scheduler) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	scheduler.Lock()
	rsp, err := scheduler.normalInsStrategy.acquireInstance(req)
	scheduler.Unlock()
	return rsp, err
}

// ReleaseInstance -
func (scheduler *Scheduler) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	scheduler.Lock()
	err := scheduler.normalInsStrategy.releaseInstance(req)
	scheduler.Unlock()
	return err
}

// CheckInstance -
func (scheduler *Scheduler) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	scheduler.Lock()
	scheduler.normalInsStrategy.checkInstance(req)
	scheduler.Unlock()
	return nil
}

// RetainInstance -
func (scheduler *Scheduler) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	scheduler.Lock()
	rsp, err := scheduler.normalInsStrategy.retainInstance(req)
	scheduler.Unlock()
	return rsp, err
}

// UpdateFuncMeta -
func (scheduler *Scheduler) UpdateFuncMeta(funcMeta *types.FunctionMetaInfo) {
	if funcMeta.InstanceMetaData.ConcurrentNum == scheduler.concurrency ||
		funcMeta.InstanceMetaData.ConcurrentNum <= 0 {
		return
	}
	log.GetLogger().Infof("funcKeyWithRes %s ConcurrentNum update, old %d, new %d",
		urnutils.AnonymizeTenantKey(scheduler.funcKeyWithRes), scheduler.concurrency,
		funcMeta.InstanceMetaData.ConcurrentNum)
	scheduler.Lock()
	scheduler.concurrency = funcMeta.InstanceMetaData.ConcurrentNum
	scheduler.normalInsStrategy.updateConcurrency(funcMeta.InstanceMetaData.ConcurrentNum)
	scheduler.enableWebsocket = funcMeta.ExtendedMetaData.EnableWebsocket
	scheduler.Unlock()
}

// UpdateFuncSchedulingStrategy -
func (scheduler *Scheduler) UpdateFuncSchedulingStrategy(newBusinessType, funcKeyWithRes string, concurrency int) {
	scheduler.Lock()
	scheduler.businessType = newBusinessType
	if commonUtils.IsWebSocketFunc(scheduler.businessType, false, scheduler.enableWebsocket) {
		scheduler.normalInsStrategy = createCentralizedStrategy(concurrency, funcKeyWithRes, newBusinessType,
			false, scheduler.enableWebsocket)
	} else {
		scheduler.normalInsStrategy = createDefaultStrategy(concurrency, false, funcKeyWithRes)
	}
	scheduler.Unlock()
}

// NeedGc -
func (scheduler *Scheduler) NeedGc() bool {
	scheduler.Lock()
	defer scheduler.Unlock()
	instanceNum := scheduler.normalInsStrategy.countInstance()
	if instanceNum == 0 {
		return true
	}
	return false
}

// GetConcurrency -
func (scheduler *Scheduler) GetConcurrency() int {
	scheduler.Lock()
	defer scheduler.Unlock()
	return scheduler.concurrency
}

// GetBusinessType -
func (scheduler *Scheduler) GetBusinessType() string {
	scheduler.Lock()
	defer scheduler.Unlock()
	return scheduler.businessType
}

// EnableWebsocket -
func (scheduler *Scheduler) EnableWebsocket() bool {
	scheduler.Lock()
	defer scheduler.Unlock()
	return scheduler.enableWebsocket
}

// SetBusinessType -
func (scheduler *Scheduler) SetBusinessType(businessType string) {
	scheduler.Lock()
	defer scheduler.Unlock()
	scheduler.businessType = businessType
}

// RecoverConnStatus recover all websocket instance connections status after faas scheduler crashed
func (scheduler *Scheduler) RecoverConnStatus(funcKeyWithRes string) {
	scheduler.Lock()
	defer scheduler.Unlock()
	scheduler.normalInsStrategy.recoverAllConnStatus(funcKeyWithRes)
}

func (scheduler *Scheduler) checkLease(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			scheduler.Lock()
			scheduler.normalInsStrategy.checkLease()
			scheduler.Unlock()
		case <-scheduler.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (scheduler *Scheduler) syncWebsocketConn() {
	ticker := time.NewTicker(time.Duration(syncWebsocketConnInterval) * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			scheduler.RecoverConnStatus(scheduler.funcKeyWithRes)
		case <-scheduler.stopCh:
			log.GetLogger().Infof("stopCh closed, websocket conn sync task of %s exit", scheduler.funcKeyWithRes)
			return
		}
	}
}
