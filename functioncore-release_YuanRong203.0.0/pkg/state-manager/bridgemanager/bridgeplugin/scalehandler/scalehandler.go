/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package scalehandler -
package scalehandler

import (
	"fmt"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

const defaultSyncPeriod = 60

// Handler -
type Handler struct {
	syncPeriod int64
	stopCh     <-chan struct{}
	// use ticker and for so no need to lock now
	functionInstance map[string]*functionInstanceData
	// will update data from package resource usage, so need lock
	resourceMetricCache ResourceMetric
}

type functionInstanceData struct {
	pods             []string
	maxStateKeyQua   int
	functionMetaInfo *config.FunctionMetaInfo
}

// GetHandler -
func GetHandler() *Handler {
	return &Handler{
		syncPeriod:          defaultSyncPeriod,
		stopCh:              make(<-chan struct{}),
		functionInstance:    make(map[string]*functionInstanceData),
		resourceMetricCache: initResourceMetricCache(),
	}
}

// WithSyncPeriod -
func (handle *Handler) WithSyncPeriod(syncPeriod int64) *Handler {
	if syncPeriod > 0 {
		handle.syncPeriod = syncPeriod
	}
	return handle
}

// WithStopCh -
func (handle *Handler) WithStopCh(stopCh <-chan struct{}) *Handler {
	if stopCh != nil {
		handle.stopCh = stopCh
	}
	return handle
}

// Run -
func (handle *Handler) Run() *Handler {
	log.GetLogger().Infof("start to process scale, syncPeriod is %d", handle.syncPeriod)
	go handle.run()
	return handle
}

// run -
func (handle *Handler) run() {
	// default syncPeriod is 60
	ticker := time.NewTicker(time.Duration(handle.syncPeriod) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-handle.stopCh:
			return
		case <-ticker.C:
			handle.ClearAndUpdateFuncInsCache()
			actions := handle.processScaleGroup()
			actions.DoActions()
		}
	}
}

// ClearAndUpdateFuncInsCache -
func (handle *Handler) ClearAndUpdateFuncInsCache() {
	handle.functionInstance = map[string]*functionInstanceData{}

	cache.Instance().FuncNameInfoCache.Range(func(key, value interface{}) bool {
		functionURN, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to string when update function instance: %v", key)
			return true
		}

		bridgeConfigResp, functionMetaInfo, err := cache.Instance().GetRestrictByURN(functionURN)
		maxStateKeyQua := bridgeConfigResp.MaxStateKeyQua
		if err != nil {
			log.GetLogger().Errorf("failed to get restrict by urn err: %s", err)
			return true
		}

		if functionMetaInfo == nil {
			log.GetLogger().Warnf("func %s not exist in cache, so do not scale", functionURN)
			return true
		}

		if functionMetaInfo.FuncMetaData.IsStatefulFunction {
			log.GetLogger().Debugf("func %s is stateful function, so do not scale", functionURN)
			return true
		}

		if !cache.BridgeManagerCanPullUpInstance(functionURN) {
			log.GetLogger().Debugf("bridge manager can't pull instance for %s, so do not scale", functionURN)
			return true
		}

		handle.updateFuncInsCacheFromWM(functionURN, functionMetaInfo, maxStateKeyQua)
		return true
	})
	log.GetLogger().Info("succeed to update funcInsCache")
}

func (handle *Handler) updateFuncInsCacheFromWM(functionURN string, functionMetaInfo *config.FunctionMetaInfo,
	maxStateKeyQua int) {

	tenantID, serviceID, funcName := utils.GetFunctionInfoByURN(functionURN)
	fullName := fmt.Sprintf("%s@%s", serviceID, funcName)
	funcSvc := urnutils.CrName(tenantID, fullName, utils.BridgeVersionLatest)

	instance, err := nodemgr.Instance().GetFuncInstances(funcSvc)
	if err != nil {
		log.GetLogger().Errorf("failed to query funcInsCache for worker manager, "+
			"funcSvcID: %s, error: %s", funcSvc, err.Error())
		return
	}

	// may be no instance or meet some error, we can analyse detail depend on logs
	if len(instance.PodsName) == 0 {
		log.GetLogger().Infof("bridge func %s can't get pod from worker-manager", funcSvc)
		return
	}

	log.GetLogger().Debugf("add function to functionInstance: %s, len pods: %d", funcSvc, len(instance.PodsName))
	handle.functionInstance[funcSvc] = &functionInstanceData{
		pods:             instance.PodsName,
		maxStateKeyQua:   maxStateKeyQua,
		functionMetaInfo: functionMetaInfo,
	}
	return
}

func (handle *Handler) processScaleGroup() Actions {
	actions := initActions()

	for funcSvc, instanceData := range handle.functionInstance {
		curInsCount := len(instanceData.pods)
		maxInsCount := instanceData.maxStateKeyQua

		totalExpected, needScale := handle.needScaleAction(maxInsCount, curInsCount, funcSvc)
		log.GetLogger().Infof("needScale is %t, maxInsCount is %d, curInsCount is %d for %s totalExpected is %d",
			needScale, maxInsCount, curInsCount, funcSvc, totalExpected)

		if !needScale {
			continue
		}

		action := handle.generateActions(funcSvc, totalExpected, int64(curInsCount), instanceData.functionMetaInfo)
		if action != nil {
			action.addPods(instanceData.pods)
			actions.addAction(funcSvc, action)
		}
	}

	return actions
}

func (handle *Handler) needScaleAction(maxInsCount int, curInsCount int, funcSvc string) (int64, bool) {
	var (
		expected int64
	)

	if curInsCount > maxInsCount {
		// maxInsCount does not include reserved instance(aka min instance)
		log.GetLogger().Infof("curInsCount %d for %s equals or greater than maxInsCount %d",
			curInsCount, funcSvc, maxInsCount)
	}

	if curInsCount == 0 {
		log.GetLogger().Infof("curInsCount is %d for %s, can't scale calculate", curInsCount, funcSvc)
		return expected, false
	}

	// when minIns < curInsCount < maxIns, required instances need to be calculated by ASBRes
	expected, err := handle.calExpectedScaleNums(curInsCount, funcSvc)

	if err != nil {
		log.GetLogger().Errorf("failed to cal expected scale nums, err: %s", err.Error())
		return expected, false
	}

	if expected > int64(maxInsCount) {
		log.GetLogger().Infof("func %s expect reach max instance, so use max instance, "+
			"expect : %d, maxInstanceCount: %d", funcSvc, expected, maxInsCount)
		return int64(maxInsCount), true
	}

	return expected, true
}

func (handle *Handler) calExpectedScaleNums(curInsCount int, funcSvc string) (int64, error) {
	if curInsCount == 0 {
		log.GetLogger().Errorf("funcSvc: %s get curInsCount is 0", funcSvc)
		return 0, fmt.Errorf("funcSvc: %s get curInsCount is 0", funcSvc)
	}

	// 1. get this metric data from metric data
	resourceDataByPod := handle.resourceMetricCache.getResourceByFuncSvcID(funcSvc)
	resiliencyStrategy := createAndMakeResiliencyStrategy(funcSvc, curInsCount, resourceDataByPod)
	if resiliencyStrategy == nil {
		log.GetLogger().Errorf("failed to get avgData for func %s, may be no input data, "+
			"resourceDataByPod: %+v", funcSvc, resourceDataByPod)
		return 0, fmt.Errorf("failed to get avgData for func %s, may be no input data, "+
			"resourceDataByPod: %+v", funcSvc, resourceDataByPod)
	}

	expectedInsCount, actualScaleRes, err := resiliencyStrategy.getExpectedInsCount()
	if err != nil {
		log.GetLogger().Errorf("failed to cal expected scale nums, err: %s", err.Error())
		return expectedInsCount, err
	}

	log.GetLogger().Infof("Expect to scale %s to %d instances by resource %s,",
		funcSvc, expectedInsCount, actualScaleRes)

	return expectedInsCount, nil
}

func (handle *Handler) generateActions(funcSvc string, totalExpected, curInsCount int64,
	functionMetaInfo *config.FunctionMetaInfo) *Action {
	if totalExpected > curInsCount {
		needPull := totalExpected - curInsCount
		return &Action{
			Type:             ActionPull,
			NeedPull:         needPull,
			TotalExpected:    totalExpected,
			functionMetaInfo: functionMetaInfo,
			pods:             make([]string, 0),
		}
	} else if totalExpected < curInsCount {
		needReduce := curInsCount - totalExpected
		return &Action{
			Type:             ActionReduce,
			NeedReduce:       needReduce,
			TotalExpected:    totalExpected,
			functionMetaInfo: functionMetaInfo,
			pods:             make([]string, 0),
		}
	} else {
		log.GetLogger().Infof("curInsCount %d equals totalExpected %d for funcSvc: %s, so no need to scale",
			curInsCount, totalExpected, funcSvc)
		return nil
	}
}

// RefreshResourceMetricCache -
func (handle *Handler) RefreshResourceMetricCache(resourceMetricCache map[resourceusage.
	FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList) {
	handle.resourceMetricCache.updateResource(resourceMetricCache)
}
