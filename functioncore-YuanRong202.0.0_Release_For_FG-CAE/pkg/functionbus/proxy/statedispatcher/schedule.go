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

// Package statedispatcher-
package statedispatcher

import (
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/utils"
)

const (
	intervalForScaleDown = time.Duration(10) * time.Second
)

// Scheduler -
type Scheduler interface {
	Schedule(req *types.DispatcherRequest) (string, error)
	Update(metrics *workerevaluator.WorkerMetrics)
}

type scheduleEntry struct {
	invokeFinishedTime time.Time
	currentRequests    int
	workerID           string
	nodeID             string
	stateID            string
	sync.Mutex
}

type stateSchedule struct {
	// key: stateID value: entry
	entries    map[string]*scheduleEntry
	autoScaler autoscaler.StateAutoScaler
	sync.Mutex
}

func createStateSchedule(scaler autoscaler.StateAutoScaler, stopCh chan struct{}, insChan chan string,
	isNeedInstanceIdleCheck bool) Scheduler {
	schedule := &stateSchedule{
		autoScaler: scaler,
		entries:    make(map[string]*scheduleEntry, 0),
	}

	if isNeedInstanceIdleCheck {
		// scheduled check for scale down
		go schedule.timerForCheckScaleDown(stopCh)
	}
	go schedule.watchStateIDDelEvent(stopCh, insChan)
	return schedule
}

func (ddp *stateSchedule) watchStateIDDelEvent(stopCh chan struct{}, insChan chan string) {
	if insChan == nil {
		log.GetLogger().Warnf("insChan is nil")
		return
	}
	if stopCh == nil {
		log.GetLogger().Warnf("stopCh is nil")
		return
	}
	for {
		select {
		case stateID, ok := <-insChan:
			if !ok {
				log.GetLogger().Warnf("instance channel exit")
				break
			}
			log.GetLogger().Debugf("process state del event, stateID: %s", stateID)
			ddp.searchStateIDAndDelInstance(stateID)
		case <-stopCh:
			log.GetLogger().Warnf("graceful exit")
			return
		}
	}
}

func (ddp *stateSchedule) searchStateIDAndDelInstance(stateID string) {
	ddp.Lock()
	entry, exist := ddp.entries[stateID]
	if !exist {
		ddp.Unlock()
		return
	}
	delete(ddp.entries, stateID)
	go ddp.autoScaler.ScaleDown(entry.workerID, stateID)
	ddp.Unlock()
}

// Schedule -
func (ddp *stateSchedule) Schedule(req *types.DispatcherRequest) (string, error) {
	ddp.Lock()
	entry, exist := ddp.entries[req.Request.StateID]
	ddp.Unlock()
	if !exist {
		instance, err := ddp.autoScaler.ScaleUp(req)
		if err != nil {
			log.GetLogger().Errorf("failed to scale up worker, funcKey: %s, stateID: %s, traceID: %s",
				req.FunctionKey, req.Request.StateID, req.TraceID)
			return "", err
		}
		if ddp.forward(req, instance) {
			return forwardWorkerID, nil
		}
	}
	ddp.Lock()
	entry, exist = ddp.entries[req.Request.StateID]
	ddp.Unlock()
	if !exist {
		log.GetLogger().Debugf("instance of stateID: %s does not exist, traceID: %s, function:%s",
			req.Request.StateID, req.TraceID, req.FunctionKey)
		return "", nil
	}
	entry.Lock()
	entry.currentRequests++
	entry.Unlock()
	return entry.workerID, nil
}

// Update -
func (ddp *stateSchedule) Update(metrics *workerevaluator.WorkerMetrics) {
	ddp.Lock()
	log.GetLogger().Debugf("update metrics, worker(%s), enable: %d, stateID: %s", metrics.ID,
		atomic.LoadUint32(&metrics.Enable), metrics.StateID)
	if _, exist := ddp.entries[metrics.StateID]; !exist {
		ddp.entries[metrics.StateID] = &scheduleEntry{
			currentRequests: 0,
			workerID:        metrics.ID,
			nodeID:          metrics.NodeIP,
			stateID:         metrics.StateID,
		}
		log.GetLogger().Debugf("successfully to update metrics, worker(%s), enable: %d, stateID: %s", metrics.ID,
			atomic.LoadUint32(&metrics.Enable), metrics.StateID)
	}
	entry := ddp.entries[metrics.StateID]
	ddp.Unlock()
	entry.Lock()
	entry.currentRequests -= len(metrics.Latencies)
	if entry.currentRequests == 0 {
		entry.invokeFinishedTime = time.Now()
	}
	entry.Unlock()
	// ignore deletes triggered by previous instances
	if atomic.LoadUint32(&metrics.Enable) == 0 && entry.workerID == metrics.ID {
		ddp.Lock()
		delete(ddp.entries, metrics.StateID)
		ddp.Unlock()
	}
}

func (ddp *stateSchedule) timerForCheckScaleDown(stopCh chan struct{}) {
	if stopCh == nil {
		return
	}
	timeTicker := time.NewTicker(intervalForScaleDown)
	for {
		select {
		case <-timeTicker.C:
			ddp.checkAndPreScaleDownWorker()
		case <-stopCh:
			timeTicker.Stop()
			log.GetLogger().Warnf("request dispatcher is stopped")
			return
		}
	}
}

func (ddp *stateSchedule) checkAndPreScaleDownWorker() {
	ddp.Lock()
	// traverse workers to be scaled down
	for stateID, entry := range ddp.entries {
		entry.Lock()
		if entry.currentRequests < 0 {
			entry.currentRequests = 0
		}
		if entry.currentRequests == 0 && time.Since(entry.invokeFinishedTime) >= internal.IdleTimeForWorker {
			delete(ddp.entries, stateID)
			go ddp.autoScaler.ScaleDown(entry.workerID, stateID)
		}
		entry.Unlock()
	}
	ddp.Unlock()
}

// when scaleUp return an instance not exist in localBus node, forward the request to the remote
func (ddp *stateSchedule) forward(req *types.DispatcherRequest, instance *types.Instance) bool {
	if instance == nil {
		return false
	}
	if instance.DeployedIP == internal.SelfNodeIP {
		ddp.Lock()
		if _, exist := ddp.entries[req.Request.StateID]; !exist {
			ddp.entries[req.Request.StateID] = &scheduleEntry{
				currentRequests: 0,
				workerID:        instance.InstanceID,
				nodeID:          instance.DeployedIP,
				stateID:         req.Request.StateID,
			}
			log.GetLogger().Infof("Add local state worker, workerID: %s, stateID: %s, nodeID: %s, traceID: %s",
				instance.InstanceID, req.Request.StateID, instance.DeployedIP, req.TraceID)
		}
		ddp.Unlock()
		return false
	}
	funcKeyWithResource := utils.GetFuncKeyWithResource(req.FunctionKey,
		types.InstanceResource{CPU: req.Request.CPU, Memory: req.Request.Memory})
	log.GetLogger().Infof("forward request to %s on %s, funcKeyWithResource: %s, traceID: %s",
		instance.InstanceID, instance.DeployedIP, urnutils.AnonymizeTenantKey(funcKeyWithResource), req.TraceID)
	tenantID, funcName, funcVer := utils.ParseFuncKey(req.FunctionKey)
	dstProperty := &common.FunctionProperty{
		TenantID:        tenantID,
		FunctionName:    funcName,
		FunctionVersion: funcVer,
		StateID:         req.Request.StateID,
		InstanceID:      instance.InstanceID,
	}
	go func() {
		req.Response <- common.SendCallReq(instance.DeployedIP, req.TraceID, req.Request, dstProperty)
	}()
	return true
}
