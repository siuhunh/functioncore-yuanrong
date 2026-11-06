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

// Package localscheduler contains local instance scheduler logic
package localscheduler

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/instance"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	defaultSmoothNum        = 5
	funcKeyWithResSliceNum  = 4
	instanceKeySliceNum     = 3
	functionTaskKeySliceNum = 2
)

// Manager note Manager public method should protect by lock
type Manager struct {
	rnd            *rand.Rand
	releaser       instance.Releaser
	funcKeyWithRes string
	funcKey        string
	tenantID       string
	instances      map[string]instance.Instance
	// instanceOrders is used from total pool scheduler
	instanceOrders []instance.Instance
	leases         sync.Map
	stopCh         chan struct{}
	isCAE          bool
	index          int
}

// CreateManager -
func CreateManager(funcKeyWithRes string, stopCh chan struct{}) *Manager {
	funcKeyWithResSlice := strings.Split(funcKeyWithRes, "/")
	isCAE := false
	if len(funcKeyWithResSlice) == funcKeyWithResSliceNum {
		metaData, exist := metadata.LoadMetaData(funcKeyWithResSlice[1],
			funcKeyWithResSlice[0], funcKeyWithResSlice[2])
		if exist && metaData.FuncMetaDataParts.BusinessType == constants.BusinessTypeCAE {
			isCAE = true
			log.GetLogger().Infof("FuncKeyWithRes %s businessTypeCAE is CAE.", funcKeyWithRes)
		}
	}
	tenantID, funcKey := util.ParseFuncKeyWithRes(funcKeyWithRes)
	return &Manager{
		rnd:            rand.New(rand.NewSource(time.Now().UnixNano())),
		releaser:       instance.CreateReleaser(funcKeyWithRes),
		funcKeyWithRes: funcKeyWithRes,
		instances:      map[string]instance.Instance{},
		instanceOrders: []instance.Instance{},
		stopCh:         stopCh,
		isCAE:          isCAE,
		tenantID:       tenantID,
		funcKey:        funcKey,
	}
}

// Start -
func (manager *Manager) Start() {
	go manager.retainLease()
}

// AddInstance -
func (manager *Manager) AddInstance(instanceKey string) {
	instanceKeySlice := strings.Split(instanceKey, util.WorkerInstanceAZSep)
	_, ok := manager.instances[instanceKey]
	if ok {
		log.GetLogger().Warnf("failed to AddInstance,"+
			" instance %s already exist", instanceKey)
		return
	}
	ins := instance.CreateInstance(manager.funcKeyWithRes, instanceKey)
	if len(instanceKeySlice) == instanceKeySliceNum {
		instanceKey = instanceKeySlice[0] + util.WorkerInstanceAZSep + instanceKeySlice[1]
	}
	manager.instances[instanceKey] = ins
	manager.instanceOrders = append(manager.instanceOrders, ins)
	manager.leases.Store(instanceKey, ins)
	log.GetLogger().Infof("addIns | %s", urnutils.ExtractPodName(instanceKey))
}

// DeleteInstance -
func (manager *Manager) DeleteInstance(instanceKey string) {
	instanceKeySlice := strings.Split(instanceKey, util.WorkerInstanceAZSep)
	if len(instanceKeySlice) == instanceKeySliceNum {
		instanceKey = instanceKeySlice[0] + util.WorkerInstanceAZSep + instanceKeySlice[1]
	}
	delete(manager.instances, instanceKey)
	manager.leases.Delete(instanceKey)
	for i, instance := range manager.instanceOrders {
		if instance.GetInstanceKey() != instanceKey {
			continue
		}
		manager.instanceOrders = append(manager.instanceOrders[:i],
			manager.instanceOrders[i+1:]...)
		break
	}
	log.GetLogger().Infof("removeIns | %s", urnutils.ExtractPodName(instanceKey))
}

// NextFromTotalPool -
func (manager *Manager) NextFromTotalPool(targetIns string) string {
	var chosenIns instance.Instance
	min := math.MaxInt32

	if manager.isCAE {
		// cae strategy is roundRobin
		chosenIns = manager.selectCAEInstance(chosenIns)
	} else {
		// faas strategy is lessConnection
		min, chosenIns = manager.selectInstance(min, chosenIns)
	}

	if targetIns != "" {
		ts, ok := manager.instances[targetIns]
		if ok && (ts.CountRequestNum() < min) {
			chosenIns = ts
			min = ts.CountRequestNum()
		}
	}

	if chosenIns == nil {
		log.GetLogger().Errorf("failed to NextFromTotalPool,"+
			" funcKeyWithRes %s chosenIns is nil",
			urnutils.AnonymizeTenantKey(manager.funcKeyWithRes))
		return ""
	}
	chosenIns.TrySchedule(true)
	return chosenIns.GetInstanceKey()
}

func (manager *Manager) selectInstance(min int, chosenIns instance.Instance) (int, instance.Instance) {
	smoothNum := defaultSmoothNum
	workerNum := len(manager.instanceOrders)
	if workerNum < defaultSmoothNum {
		smoothNum = workerNum
	}
	indexArray := manager.rnd.Perm(workerNum)
	for i := 0; i < smoothNum && i < len(indexArray) &&
		indexArray[i] < len(manager.instanceOrders); i++ {
		ins := manager.instanceOrders[indexArray[i]]
		requestNum := ins.CountRequestNum()
		if requestNum < min {
			chosenIns = ins
			min = requestNum
		}
	}
	return min, chosenIns
}

func (manager *Manager) selectCAEInstance(chosenIns instance.Instance) instance.Instance {
	var unRegisterInstances []instance.Instance
	for i := 0; i < len(manager.instanceOrders); i++ {
		if manager.index > len(manager.instanceOrders)-1 {
			manager.index = 0
		}
		ins := manager.instanceOrders[manager.index]
		status, ok := instance.GetWorkerInstanceManager().GetWorkerInstanceStatus(ins.GetNodeIP())
		if !ok {
			unRegisterInstances = append(unRegisterInstances, ins)
			manager.handleRoundRobinIndex()
			continue
		}
		if !status {
			log.GetLogger().Infof("Skip function task unhealthy node IP %s, "+
				"instanceKey is %s", ins.GetNodeIP(), ins.GetInstanceKey())
			manager.handleRoundRobinIndex()
			continue
		}
		chosenIns = ins
		manager.handleRoundRobinIndex()
		break
	}
	if chosenIns == nil && len(unRegisterInstances) > 0 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		chosenIns = unRegisterInstances[r.Intn(len(unRegisterInstances))]
	}
	return chosenIns
}

func (manager *Manager) handleRoundRobinIndex() {
	if manager.index >= len(manager.instanceOrders)-1 {
		manager.index = 0
	} else {
		manager.index++
	}
}

// Done -
func (manager *Manager) Done(instanceKey string) *common.Processor {
	ins, ok := manager.instances[instanceKey]
	if !ok {
		log.GetLogger().Debugf("RequestDone failed,"+
			" instanceKey %s not exist", instanceKey)
		return nil
	}
	return ins.Done()
}

// Release releases processor
func (manager *Manager) Release(instanceKey string, processor *common.Processor, cost time.Duration) {
	if processor == nil {
		return
	}
	if cost < time.Duration(constant.FConfig.LoadBalanceConfig.SyncReleaseTimeThreshold)*time.Millisecond {
		common.GetPoolInstance().Submit(func() {
			manager.releaseInstance(instanceKey, processor)
		})
	} else {
		manager.releaseInstance(instanceKey, processor)
	}
}

// NextFromAcquiredPool -
func (manager *Manager) NextFromAcquiredPool() string {
	for key, ins := range manager.instances {
		enable := ins.TrySchedule(false)
		if enable {
			return key
		}
	}
	return ""
}

// AddProcessorAndTryScheduler -
func (manager *Manager) AddProcessorAndTryScheduler(
	processor *common.Processor, instanceKey, traceID string) string {
	ins, ok := manager.instances[instanceKey]
	if !ok {
		log.GetLogger().Warnf("failed to find instance,"+
			" traceID %s instanceKey %s", traceID, instanceKey)
		manager.releaseInstance(instanceKey, processor)
		return ""
	}

	err := ins.AddProcessor(processor)
	if err != nil {
		processorKey := urnutils.GetProcessorKey(instanceKey,
			uint32(processor.ProcessorID), processor.LeaseVersion)
		log.GetLogger().Warnf("failed to AddProcessor,"+
			" traceID %s processorKey %s err %s",
			traceID, processorKey, err.Error())
		return ""
	}

	enable := ins.TrySchedule(false)
	if enable {
		return instanceKey
	}
	return ""
}

// GetInstanceNum -
func (manager *Manager) GetInstanceNum() int {
	return len(manager.instances)
}

// AcquireInstance -
func (manager *Manager) AcquireInstance(traceID string) (*common.Processor, string, error) {
	req := &schedulerapi.AcquireInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: manager.funcKeyWithRes,
	}
	var scheduler common.RPCClient
	useStatefulClient := false
	if target := remotescheduler.GetWhitelist().Query(manager.funcKey, manager.tenantID); target != "" {
		useStatefulClient = true
		scheduler = remotescheduler.GetInstance().GetStatefulScheduler(target)
	} else {
		scheduler = remotescheduler.GetInstance().GetScheduler(manager.funcKeyWithRes)
	}

	if scheduler == nil {
		return nil, "", errors.New("failed to GetScheduler")
	}
	log.GetLogger().Debugf("use stateful scheduler %t, used faas-scheduler %s, traceid %s",
		useStatefulClient, scheduler.ServerKey(), traceID)
	rsp, err := scheduler.AcquireInstance(req)
	if err != nil {
		return nil, "", err
	}
	if rsp.Error.ErrorCode != 0 {
		return nil, "", fmt.Errorf("failed to AcquireInstance errcode %d",
			rsp.Error.ErrorCode)
	}
	p := &common.Processor{
		ProcessorID:  int(rsp.ProcessorID),
		ThreadNum:    int(rsp.ThreadNum),
		LeaseVersion: rsp.LeaseVersion,
		ServerKey:    scheduler.ServerKey(),
	}
	return p, rsp.InstanceKey, nil
}

func (manager *Manager) retainLease() {
	interval := constant.FConfig.LoadBalanceConfig.LeaseCheckInterval
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			manager.leases.Range(func(k, v interface{}) bool {
				ins, ok := v.(instance.Instance)
				if !ok {
					return true
				}
				ins.RetainLease()
				return true
			})
		case <-manager.stopCh:
			ticker.Stop()
			log.GetLogger().Debugf("funcKeyWithRes %s retainLease end",
				urnutils.AnonymizeTenantKey(manager.funcKeyWithRes))
			return
		}
	}
}

func (manager *Manager) releaseInstance(instanceKey string, processor *common.Processor) {
	req := &schedulerapi.ReleaseInstanceReq{
		TraceID:        uuid.New().String(),
		FuncKeyWithRes: manager.funcKeyWithRes,
		InstanceKey:    instanceKey,
		ProcessorID:    uint32(processor.ProcessorID),
		LeaseVersion:   processor.LeaseVersion,
	}
	manager.releaser.ReleaseInstance(req)
}
