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
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	bridgeConfig "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	"functioncore/pkg/state-manager/config"
)

var (
	// lastScaleTimeMap -
	lastScaleTimeMap = make(map[string]int64)
	// lock of last scale time map lock
	lastScaleTimeMapLock = sync.RWMutex{}
)

// Action describes action of pulling up reversed instance or reducing some instances
type Action struct {
	wg         sync.WaitGroup
	failedNums int32
	succeedNum int32
	pods       []string

	Type ActionType
	// reversed instance or pull
	NeedPull         int64
	NeedReduce       int64
	TotalExpected    int64
	functionMetaInfo *bridgeConfig.FunctionMetaInfo
}

// ActionType is type of action
type ActionType int

// enums of action type
const (
	ActionPull ActionType = iota
	ActionReduce
)

// Actions -
type Actions map[string]*Action

// initActions -
func initActions() Actions {
	return make(map[string]*Action)
}

func (actions Actions) addAction(funcSvcID string, action *Action) {
	if action != nil {
		actions[funcSvcID] = action
	}
}

// DoActions -
func (actions Actions) DoActions() {
	for functionSvcID, action := range actions {
		if action.Type == ActionPull {
			go action.ScaleUp(functionSvcID, action.NeedPull)
		} else if action.Type == ActionReduce {
			if !checkLastScaleTime(functionSvcID) {
				log.GetLogger().Infof("skip scale down, waiting cooling, funcSvcID: %s", functionSvcID)
				continue
			}
			go action.ScaleDown(functionSvcID, action.NeedReduce)
			setLastScaleTime(functionSvcID)
		} else {
			continue
		}
	}
}

func (action *Action) addPods(pods []string) {
	action.pods = pods
}

// ScaleUp -
func (action *Action) ScaleUp(functionSvcID string, scalingWorkersNum int64) (int, int) {
	startTime := time.Now()

	log.GetLogger().Infof("begin to pull up bridge instances with scalingWorkersNum %d for func: %s",
		scalingWorkersNum, functionSvcID)

	for i := 0; i < int(scalingWorkersNum); i++ {
		action.wg.Add(1)
		go action.pullUpFunc(functionSvcID)
	}

	action.wg.Wait()

	log.GetLogger().Infof("finished to scale up bridge instances with failed nums: %d succeed nums: %d "+
		"scalingWorkersNum is %d for func:  %s cost time: %s",
		atomic.LoadInt32(&action.failedNums), atomic.LoadInt32(&action.succeedNum),
		scalingWorkersNum, functionSvcID, time.Since(startTime).String())
	return int(atomic.LoadInt32(&action.failedNums)), int(atomic.LoadInt32(&action.succeedNum))
}

func (action *Action) pullUpFunc(functionSvcID string) {
	_, err := nodemgr.Instance().ScaleUpInstance(functionSvcID, action.functionMetaInfo)
	if err != nil {
		atomic.AddInt32(&action.failedNums, 1)
		log.GetLogger().Errorf("failed to pull up instance for func: %s because: %s", functionSvcID, err.Error())
	} else {
		atomic.AddInt32(&action.succeedNum, 1)
	}
	action.wg.Done()
}

// ScaleDown -
func (action *Action) ScaleDown(functionSvcID string, reduceWorkerNum int64) (int, int) {
	startTime := time.Now()

	needReduce := reduceWorkerNum

	for _, podName := range action.pods {
		if needReduce <= 0 {
			break
		}

		action.wg.Add(1)
		// pick up instance to scale down with random now
		go action.reduceFunc(functionSvcID, podName)
		needReduce--
	}

	action.wg.Wait()

	log.GetLogger().Infof("finished to scale down bridge instances with failed nums: %d succeed nums: %d "+
		"reduceWorkerNum is %d for func: %s cost time: %s",
		atomic.LoadInt32(&action.failedNums), atomic.LoadInt32(&action.succeedNum),
		reduceWorkerNum, functionSvcID, time.Since(startTime).String())
	return int(atomic.LoadInt32(&action.failedNums)), int(atomic.LoadInt32(&action.succeedNum))
}

func (action *Action) reduceFunc(functionSvcID, workerID string) {
	err := nodemgr.Instance().ScaleDownInstance(functionSvcID, workerID, action.functionMetaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to scale down function %s, workerID: %s,err: %s", functionSvcID,
			workerID, err.Error())
		atomic.AddInt32(&action.failedNums, 1)
	} else {
		atomic.AddInt32(&action.succeedNum, 1)
	}

	action.wg.Done()
}

// if last now diff scale time lower than downscaleStabilization, we need to ignore this scale down action
func checkLastScaleTime(functionSvcID string) bool {
	now := time.Now().Unix()
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config file for checkLastScaleTime")
		return false
	}
	lastScaleTimeMapLock.RLock()
	defer lastScaleTimeMapLock.RUnlock()
	lastScaleTime, ok := lastScaleTimeMap[functionSvcID]
	if !ok {
		return true
	}

	if lastScaleTime != 0 && (now-lastScaleTime <= cfg.ScaleData.DownscaleStabilization) {
		return false
	}

	return true
}

// set last scale time in map
func setLastScaleTime(functionSvcID string) {
	lastScaleTimeMapLock.Lock()
	defer lastScaleTimeMapLock.Unlock()
	lastScaleTimeMap[functionSvcID] = time.Now().Unix()
}

// ClearLastScaleTime -
func ClearLastScaleTime(functionSvcID string) {
	log.GetLogger().Debugf("clear last scale time of %s", functionSvcID)
	lastScaleTimeMapLock.Lock()
	defer lastScaleTimeMapLock.Unlock()
	delete(lastScaleTimeMap, functionSvcID)
}
