/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package requestdispatcher -
package requestdispatcher

import (
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/queue"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/strategy"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
)

const (
	// DefaultMircoServiceConcurrency - CAE default concurrency
	DefaultMircoServiceConcurrency = 100000
	caeScaleDownDuration           = "CAE_SCALE_DOWN_TIME_INTERVAL"
	minCaeScaleDownDuration        = 60
)

var (
	caeScaleDownTimeInterval int = getCaeScaleDownTimeInterval()
)

type microserviceScheduler struct {
	autoScaler          autoscaler.AutoScaler
	strategy            string
	index               int
	sortedSlices        []scheduleEntry
	funcKeyWithResource string
	currentAllRequests  int
	concurrency         int
	lastConcurrency     int
	keepaliveInterval   int
	scaleWindow         scaleWindow
	needIns             *queue.MonotonicQueue
	scaleNum            *queue.MonotonicQueue
	sync.Mutex
}

func newMicronServiceScheduler(funcKeyWithResource string, scaler autoscaler.AutoScaler,
	metadata servicediscovery.FunctionMetaInfo, stopCh <-chan struct{}) Scheduler {
	if stopCh == nil {
		return nil
	}
	if metadata.InstanceMetaData.ConcurrentNum == 0 {
		metadata.InstanceMetaData.ConcurrentNum = DefaultMircoServiceConcurrency
	}
	scheduler := &microserviceScheduler{
		autoScaler:          scaler,
		strategy:            common.FConfig.MicroServiceSchedulingPolicy,
		sortedSlices:        make([]scheduleEntry, 0),
		funcKeyWithResource: funcKeyWithResource,
		lastConcurrency:     metadata.InstanceMetaData.ConcurrentNum,
		concurrency:         metadata.InstanceMetaData.ConcurrentNum,
		scaleWindow:         scaleWindow{},
		needIns:             queue.NewMonotonicQueue(caeScaleDownTimeInterval),
		scaleNum:            queue.NewMonotonicQueue(caeScaleDownTimeInterval),
		keepaliveInterval:   caeScaleDownTimeInterval,
	}
	log.GetLogger().Infof("caeScaleDownTimeInterval %d", caeScaleDownTimeInterval)
	scaler.SetFuncMeta(metadata)
	go func() {
		if autoscaler.EnableMicroserviceAutoScaler {
			timeTicker := time.NewTicker(time.Second)
			for {
				select {
				case <-timeTicker.C:
					scheduler.checkScaleDown()
				case <-stopCh:
					timeTicker.Stop()
					return
				}
			}
		}
	}()
	log.GetLogger().Infof("new microServiceScheduler: %s, concurrency: %d.", funcKeyWithResource,
		metadata.InstanceMetaData.ConcurrentNum)
	return scheduler
}

func (mss *microserviceScheduler) SetKeepaliveTime(kaTime int) {}

// Schedule -
func (mss *microserviceScheduler) Schedule(request *types.TCPRequest, _ string) string {
	var usedInstance map[string]struct{}
	if request != nil {
		usedInstance = request.UsedInstance
	} else {
		usedInstance = make(map[string]struct{}, 1)
	}

	log.GetLogger().Debugf("MicroserviceScheduler funcKeyWithResource: %s, strategy: %s, concurrency: %d.",
		mss.funcKeyWithResource, mss.strategy, mss.concurrency)
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()
	switch mss.strategy {
	case common.RoundRobin:
		mss.index = strategy.RoundRobin(mss.index, mss.sortedSlices, usedInstance, defaultCheckFunction)
	case common.LeastConnections:
		mss.index = strategy.LeastConnections(mss.sortedSlices, usedInstance, connNums, defaultCheckFunction)
	default:
		mss.index = strategy.RoundRobin(mss.index, mss.sortedSlices, usedInstance, defaultCheckFunction)
	}
	if mss.index < 0 || mss.index >= len(mss.sortedSlices) {
		return ""
	}
	mss.sortedSlices[mss.index].currentRequests++
	mss.addAllRequestNums(1)
	return mss.sortedSlices[mss.index].workerID
}

// Update -
func (mss *microserviceScheduler) Update(metrics *workerevaluator.WorkerMetrics) {
	mss.Mutex.Lock()
	mss.updateSortedWorkers(metrics)
	usedWorkerNum := len(mss.sortedSlices)
	mss.Mutex.Unlock()
	mss.autoScaler.UpdateUsedWorkerNum(usedWorkerNum)
}

func (mss *microserviceScheduler) GetSchedulerType() string {
	return constants.BusinessTypeCAE
}

// PreSchedule mark workerID scheduled
func (mss *microserviceScheduler) PreSchedule(workerID string) {
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()
	for i, entry := range mss.sortedSlices {
		if entry.workerID == workerID {
			mss.sortedSlices[i].currentRequests++
			mss.addAllRequestNums(1)
			return
		}
	}
	log.GetLogger().Warnf("preSchedule workerID %s missed for func %s", workerID, mss.funcKeyWithResource)
}

func (mss *microserviceScheduler) updateSortedWorkers(metrics *workerevaluator.WorkerMetrics) {
	// unavailable worker
	if atomic.LoadUint32(&metrics.Enable) == 0 {
		for index := range mss.sortedSlices {
			if mss.sortedSlices[index].workerID == metrics.ID {
				log.GetLogger().Infof("Delete unavailable worker %s to sorted slice %s, currentRequests %d",
					metrics.ID, urnutils.AnonymizeTenantKey(metrics.FuncKeyWithResource), metrics.RecoveredRequestNum)
				mss.delAllRequestNums(mss.sortedSlices[index].currentRequests)
				mss.sortedSlices = append(mss.sortedSlices[:index], mss.sortedSlices[index+1:]...)
				break
			}
		}
		return
	}

	// available worker
	var found = false
	for index := range mss.sortedSlices {
		if mss.sortedSlices[index].workerID == metrics.ID {
			mss.calculateScheduleEntry(&mss.sortedSlices[index], metrics)
			found = true
		}
	}
	if !found {
		log.GetLogger().Infof("add new available worker %s to sorted slice %s, currentRequests %d",
			metrics.ID, urnutils.AnonymizeTenantKey(metrics.FuncKeyWithResource), metrics.RecoveredRequestNum)
		entry := scheduleEntry{
			workerID:        metrics.ID,
			concurrency:     mss.concurrency,
			currentRequests: metrics.RecoveredRequestNum,
			isLocalWorker:   metrics.IsLocalWorker,
			isMinInstance:   metrics.IsMinInstance,
			nodeIP:          metrics.NodeIP,
		}
		mss.addAllRequestNums(entry.currentRequests)
		mss.sortedSlices = append(mss.sortedSlices, entry)
	}
}

// SetFunctionMetadata update function meta data
func (mss *microserviceScheduler) SetFunctionMetadata(metadata servicediscovery.FunctionMetaInfo) {
	mss.Mutex.Lock()
	mss.setConcurrency(metadata.InstanceMetaData.ConcurrentNum)
	mss.Mutex.Unlock()
	mss.autoScaler.SetFuncMeta(metadata)
}

func (mss *microserviceScheduler) calculateScheduleEntry(old *scheduleEntry,
	metrics *workerevaluator.WorkerMetrics) *scheduleEntry {
	latenciesLen := len(metrics.Latencies)
	if latenciesLen != 0 {
		if old.currentRequests < latenciesLen {
			log.GetLogger().Warnf("currentRequests %d is small than latenciesLen %d, workerID %s",
				old.currentRequests, latenciesLen, old.workerID)
			mss.delAllRequestNums(old.currentRequests)
			old.currentRequests = 0
		} else {
			mss.delAllRequestNums(latenciesLen)
			old.currentRequests -= latenciesLen
		}
	}
	return old
}

func (mss *microserviceScheduler) delAllRequestNums(finishReqNum int) {
	requestsNum := mss.currentAllRequests - finishReqNum
	if requestsNum < 0 {
		log.GetLogger().Warnf("function: %s currentAllRequests: %d is small than finishReqNum: %d",
			urnutils.AnonymizeTenantKey(mss.funcKeyWithResource), mss.currentAllRequests, finishReqNum)
		requestsNum = 0
	}
	mss.currentAllRequests = requestsNum
}

func (mss *microserviceScheduler) setConcurrency(concurrency int) {
	if concurrency == 0 {
		concurrency = DefaultMircoServiceConcurrency
	}
	diff := concurrency - mss.lastConcurrency
	if diff == 0 {
		log.GetLogger().Infof("concurrency for %s is still %d", mss.funcKeyWithResource, concurrency)
		return
	}

	for index := range mss.sortedSlices {
		mss.sortedSlices[index].concurrency += diff
	}
	mss.concurrency += diff
	log.GetLogger().Infof("update concurrency from %d to %d for function %s, current average concurrency is %d",
		mss.lastConcurrency, concurrency, mss.funcKeyWithResource, mss.concurrency)

	mss.lastConcurrency = concurrency
}

// UpdateWorkerWillDeleteStatus -
func (mss *microserviceScheduler) UpdateWorkerWillDeleteStatus(workerID string, status bool) {
}

func connNums(entry scheduleEntry, usedInstance map[string]struct{}) int {
	if _, ok := usedInstance[entry.workerID]; ok {
		return math.MaxInt
	}
	return entry.currentRequests
}

func defaultCheckFunction(entry scheduleEntry, usedInstance map[string]struct{}) bool {
	if _, ok := usedInstance[entry.workerID]; ok {
		return false
	}
	isNodeAvailable := getNodeAvailable(entry.nodeIP, entry.workerID)
	if isNodeAvailable && (entry.concurrency-entry.currentRequests > 0) {
		return true
	}
	return false
}

func (mss *microserviceScheduler) addAllRequestNums(reqNum int) {
	mss.currentAllRequests = mss.currentAllRequests + reqNum
	if mss.currentAllRequests > mss.scaleWindow.maxRequests {
		mss.scaleWindow.maxRequests = mss.currentAllRequests
	}
}

func (mss *microserviceScheduler) sortWorkerIDs() {
	sort.Slice(mss.sortedSlices, func(i, j int) bool {
		return mss.sortedSlices[i].priority > mss.sortedSlices[j].priority
	})
}

func (mss *microserviceScheduler) checkScaleDown() {
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()

	if len(mss.sortedSlices) == 0 {
		mss.scaleNum.Push(0)
		mss.needIns.Push(0)
		return
	}
	scaleNum := mss.autoScaler.CalculateScaleDown(mss.scaleWindow.maxRequests, 0)
	mss.scaleWindow.maxRequests = mss.currentAllRequests
	if scaleNum > len(mss.sortedSlices) || scaleNum < 0 {
		mss.scaleNum.Push(0)
		mss.needIns.Push(len(mss.sortedSlices))
		log.GetLogger().Warnf("invalid scale down number %d, sorted slice len %d, function: %s",
			scaleNum, len(mss.sortedSlices), urnutils.AnonymizeTenantKey(mss.funcKeyWithResource))
		return
	}
	mss.scaleNum.Push(-scaleNum)
	mss.needIns.Push(len(mss.sortedSlices) - scaleNum)
	if scaleNum == 0 {
		return
	}

	minScaleDown := -mss.scaleNum.GetMax()
	maxNeedIns := mss.needIns.GetMax()
	if len(mss.sortedSlices)-maxNeedIns < minScaleDown {
		minScaleDown = len(mss.sortedSlices) - maxNeedIns
	}
	if minScaleDown > 0 && mss.scaleNum.Len() >= mss.keepaliveInterval {
		mss.sortWorkerIDs()
		index := len(mss.sortedSlices) - 1
		for ; index >= len(mss.sortedSlices)-minScaleDown; index-- {
			entry := mss.sortedSlices[index]
			// isMinInstance condition: reserved instance
			if entry.isMinInstance {
				break
			}
			if entry.currentRequests != 0 {
				log.GetLogger().Warnf("select scaled down worker %s, but request num on it is %d,"+
					" wait request finished", entry.workerID, entry.currentRequests)
				break
			}
			go mss.autoScaler.ScaleDown(entry.workerID)
		}
		mss.sortedSlices = mss.sortedSlices[0 : index+1]
	}
}

func getCaeScaleDownTimeInterval() int {
	scaleDownTimeInterval := os.Getenv(caeScaleDownDuration)
	scaleDownTime, err := strconv.Atoi(scaleDownTimeInterval)
	if err != nil {
		log.GetLogger().Warnf("failed to atoi, scaleDownTimeInterval: %s, "+
			"err: %s", scaleDownTimeInterval, err.Error())
		return int(internal.IdleTimeForCaeWorker / time.Second)
	}
	if scaleDownTime < minCaeScaleDownDuration {
		log.GetLogger().Warnf("scaleDownTime %d < 60, cae scaleDownTime retain the default value", scaleDownTime)
		return int(internal.IdleTimeForCaeWorker / time.Second)
	}
	log.GetLogger().Infof("current CaeScaleDownTimeInterval %d", scaleDownTime)
	return scaleDownTime
}
