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
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/queue"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/strategy"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxy/workersview"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

const (
	// DefaultMircoServiceConcurrency - CAE default concurrency
	DefaultMircoServiceConcurrency = 100000
	caeScaleDownDuration           = "CAE_SCALE_DOWN_TIME_INTERVAL"
	minCaeScaleDownDuration        = 60
	caeInsDeleteCheckInterval      = 500
	// cae request timeout 900s, gap 20s
	scaleDownTaskTimeout = 920
)

var (
	willDeletedInstance      = sync.Map{}
	caeScaleDownTimeInterval = getCaeScaleDownTimeInterval()
)

type microserviceScheduler struct {
	autoScaler            autoscaler.AutoScaler
	strategy              string
	index                 int
	sortedSlices          []scheduleEntry
	funcKeyWithResource   string
	currentAllRequests    int
	concurrency           int
	lastConcurrency       int
	keepaliveInterval     int
	circuitBreakerEnabled bool
	scaleWindow           scaleWindow
	needIns               *queue.MonotonicQueue
	scaleNum              *queue.MonotonicQueue
	rnd                   *rand.Rand
	dispatcher            *RequestDispatcher
	sync.Mutex
}

type scaleDownCheckTask struct {
	workerID string
	stopChan chan interface{}
	sync.Mutex
	closed    bool
	timestamp int64
}

func (mss *microserviceScheduler) SetDispatcher(dispatcher *RequestDispatcher) {
	log.GetLogger().Infof("prepare set dispatcher, funcKeyWithResource: %s", mss.funcKeyWithResource)
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()
	mss.dispatcher = dispatcher
	log.GetLogger().Infof("finish set dispatcher, funcKeyWithResource: %s", mss.funcKeyWithResource)
}

func init() {
	go clearWillDeleteInsRecord(signals.WaitForSignal())
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
		autoScaler:            scaler,
		strategy:              common.FConfig.MicroServiceSchedulingPolicy,
		sortedSlices:          make([]scheduleEntry, 0),
		funcKeyWithResource:   funcKeyWithResource,
		lastConcurrency:       metadata.InstanceMetaData.ConcurrentNum,
		concurrency:           metadata.InstanceMetaData.ConcurrentNum,
		scaleWindow:           scaleWindow{},
		needIns:               queue.NewMonotonicQueue(caeScaleDownTimeInterval),
		scaleNum:              queue.NewMonotonicQueue(caeScaleDownTimeInterval),
		keepaliveInterval:     caeScaleDownTimeInterval,
		rnd:                   rand.New(rand.NewSource(time.Now().UnixNano())),
		circuitBreakerEnabled: metadata.FuncMetaData.MicCircuitBreakerEnabled,
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
func (mss *microserviceScheduler) Schedule(tcpRequest *types.TCPRequest, traceID string) string {
	instanceID := tcpRequest.InstanceID
	usedInstance := tcpRequest.UsedInstance
	log.GetLogger().Debugf("MicroserviceScheduler funcKeyWithResource:%s, strategy:%s, concurrency:%d, "+
		"instanceID:%s, traceID:%s", mss.funcKeyWithResource, mss.strategy, mss.concurrency, instanceID, traceID)
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()
	circuitBreakerEnabled := mss.circuitBreakerEnabled
	if len(instanceID) > 0 {
		workerID := utils.ParseWorkerIDFromInstanceID(instanceID)
		for i := 0; i < len(mss.sortedSlices); i++ {
			if mss.sortedSlices[i].workerID == workerID && specifyInsCheckFunction(mss.sortedSlices[i], usedInstance) {
				mss.sortedSlices[i].currentRequests++
				mss.addAllRequestNums(1)
				return workerID
			}
		}
		return ""
	}
	switch mss.strategy {
	case common.RoundRobin:
		mss.index = strategy.RoundRobin(mss.index, mss.sortedSlices, usedInstance,
			circuitBreakerEnabled, defaultCheckFunction)
	case common.LeastConnections:
		mss.rnd.Shuffle(len(mss.sortedSlices), func(i, j int) {
			mss.sortedSlices[i], mss.sortedSlices[j] = mss.sortedSlices[j], mss.sortedSlices[i]
		})
		mss.index = strategy.LeastConnections(mss.sortedSlices, usedInstance, circuitBreakerEnabled,
			connNums, defaultCheckFunction)
	default:
		mss.index = strategy.RoundRobin(mss.index, mss.sortedSlices, usedInstance,
			circuitBreakerEnabled, defaultCheckFunction)
	}
	if mss.index < 0 && len(mss.sortedSlices) > 0 && circuitBreakerEnabled {
		return circuitBreakerEnabledWorkerID
	}
	if mss.index < 0 || mss.index >= len(mss.sortedSlices) {
		return ""
	}
	mss.sortedSlices[mss.index].currentRequests++
	mss.addAllRequestNums(1)
	return mss.sortedSlices[mss.index].workerID
}

// ScheduleAll get all instances
func (mss *microserviceScheduler) ScheduleAll() []string {
	log.GetLogger().Debugf("MicroserviceScheduleAll, funcKeyWithResource: %s, strategy: %s, concurrency: %d",
		mss.funcKeyWithResource, mss.strategy, mss.concurrency)
	mss.Mutex.Lock()
	defer mss.Mutex.Unlock()

	workerIDList := make([]string, len(mss.sortedSlices), len(mss.sortedSlices))
	for i, entry := range mss.sortedSlices {
		if entry.willDelete {
			continue
		}
		workerIDList[i] = entry.workerID
		mss.sortedSlices[i].currentRequests++
	}
	mss.addAllRequestNums(len(mss.sortedSlices))
	return workerIDList
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

func (mss *microserviceScheduler) processDisabledWorker(metrics *workerevaluator.WorkerMetrics) {
	for index := range mss.sortedSlices {
		if mss.sortedSlices[index].workerID == metrics.ID {
			log.GetLogger().Infof("Delete unavailable worker %s to sorted slice %s, currentRequests %d",
				metrics.ID, urnutils.AnonymizeTenantKey(metrics.FuncKeyWithResource), metrics.RecoveredRequestNum)
			scaleDownTask := &scaleDownCheckTask{
				timestamp: time.Now().Unix(),
				workerID:  metrics.ID,
				stopChan:  make(chan interface{}),
			}
			scaleDownCheckTaskIf, _ := willDeletedInstance.LoadOrStore(metrics.ID, scaleDownTask)
			scaleDownTask, ok := scaleDownCheckTaskIf.(*scaleDownCheckTask)
			if ok {
				scaleDownTask.Lock()
				if !scaleDownTask.closed {
					scaleDownTask.closed = true
					close(scaleDownTask.stopChan)
				}
				scaleDownTask.Unlock()
			}
			mss.autoScaler.ScaleDown(action.ScaleDownParam{
				WorkerID:      mss.sortedSlices[index].workerID,
				IgnoreApplier: true,
			})
			mss.delAllRequestNums(mss.sortedSlices[index].currentRequests)
			mss.sortedSlices = append(mss.sortedSlices[:index], mss.sortedSlices[index+1:]...)
			break
		}
	}
}

func (mss *microserviceScheduler) updateSortedWorkers(metrics *workerevaluator.WorkerMetrics) {
	// unavailable worker
	if atomic.LoadUint32(&metrics.Enable) == 0 {
		mss.processDisabledWorker(metrics)
		if len(mss.sortedSlices) == 0 {
			mss.stopDispatcherWhenNoInstance()
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
			workerID:            metrics.ID,
			funcKeyWithResource: mss.funcKeyWithResource,
			concurrency:         mss.concurrency,
			currentRequests:     metrics.RecoveredRequestNum,
			isLocalWorker:       metrics.IsLocalWorker,
			isMinInstance:       metrics.IsMinInstance,
			nodeIP:              metrics.NodeIP,
		}
		mss.addAllRequestNums(entry.currentRequests)
		mss.sortedSlices = append(mss.sortedSlices, entry)
	}
}

// SetFunctionMetadata update function meta data
func (mss *microserviceScheduler) SetFunctionMetadata(metadata servicediscovery.FunctionMetaInfo) {
	mss.Mutex.Lock()
	mss.setConcurrency(metadata.InstanceMetaData.ConcurrentNum)
	mss.setCircuitBreakerEnabled(metadata.FuncMetaData.MicCircuitBreakerEnabled)
	mss.Mutex.Unlock()
	log.GetLogger().Infof("prepare to set func meta, funcName: %s", metadata.FuncMetaData.FunctionVersionURN)
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

func (mss *microserviceScheduler) setCircuitBreakerEnabled(circuitBreakerEnabled bool) {
	log.GetLogger().Infof("update circuitBreakerEnabled from %t to %t for function %s",
		mss.circuitBreakerEnabled, circuitBreakerEnabled, mss.funcKeyWithResource)
	mss.circuitBreakerEnabled = circuitBreakerEnabled
}

// UpdateWorkerWillDeleteStatus -
func (mss *microserviceScheduler) UpdateWorkerWillDeleteStatus(info *willDeleteInfo) {
	mss.Lock()
	defer mss.Unlock()
	found := false
	for index := range mss.sortedSlices {
		if mss.sortedSlices[index].workerID == info.workerID {
			found = true
			log.GetLogger().Infof("%s willDelete status changed to %t", info.workerID, info.status)
			if info.status {
				mss.sortedSlices[index].willDelete = info.status
			}
			break
		}
	}
	if !found {
		log.GetLogger().Warnf("%s willDelete status changed to %t, but sortedSlices not found",
			info.workerID, info.status)
	}
	if info.status && info.ip == internal.SelfNodeIP {
		mss.ScaleDownTargetInstance(info.workerID)
	}
	if len(mss.sortedSlices) == 0 {
		mss.stopDispatcherWhenNoInstance()
	}
}

func (mss *microserviceScheduler) stopDispatcherWhenNoInstance() {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(mss.funcKeyWithResource)
	_, err := servicediscovery.GetRegistry().GetFunctionFuncMetaData(functionKey)
	if err == nil {
		log.GetLogger().Debugf("Function %s still exist, no need to stop dispatcher", mss.funcKeyWithResource)
		return
	}
	log.GetLogger().Infof("Function %s has no instances and metadata not found, need to stop dispatcher",
		mss.funcKeyWithResource)
	if mss.dispatcher != nil && mss.dispatcher.workerDispatcher != nil {
		mss.dispatcher.workerDispatcher.DeleteFunctionDispatcher(functionKey)
	}
}

func (mss *microserviceScheduler) ScaleDownTargetInstance(workerID string) {
	scaleDownCheckTaskIf, loaded := willDeletedInstance.LoadOrStore(workerID, &scaleDownCheckTask{
		timestamp: time.Now().Unix(),
		workerID:  workerID,
		stopChan:  make(chan interface{}),
	})
	if loaded {
		log.GetLogger().Warnf("WorkerInstance %s has been marked will delete, do nothing", workerID)
		return
	}
	scaleDownTask, ok := scaleDownCheckTaskIf.(*scaleDownCheckTask)
	if !ok {
		log.GetLogger().Warnf("scaleDownCheckTaskIf assert scaleDownCheckTask failed for %s", workerID)
		return
	}
	go mss.DoScaleDownTargetInstance(workerID, scaleDownTask.stopChan)
}

func clearWillDeleteInsRecord(stopCh <-chan struct{}) {
	ticker := time.NewTicker(scaleDownTaskTimeout * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			willDeletedInstance.Range(func(instanceID, scaleDownCheckTaskIf any) bool {
				scaleDownTask, ok := scaleDownCheckTaskIf.(*scaleDownCheckTask)
				if !ok {
					return true
				}
				if now-int64(scaleDownTaskTimeout) > scaleDownTask.timestamp {
					scaleDownTask.Lock()
					if !scaleDownTask.closed {
						scaleDownTask.closed = true
						close(scaleDownTask.stopChan)
					}
					scaleDownTask.Unlock()
					willDeletedInstance.Delete(instanceID)
				}
				return true
			})
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warn("WillDeleteInstance clean task exit")
				return
			}
		}
	}
}

func (mss *microserviceScheduler) DoScaleDownTargetInstance(workerID string, stopChan chan interface{}) {
	log.GetLogger().Infof("%s %s marked willDelete, check current request for delete periodically",
		mss.funcKeyWithResource, workerID)
	ticker := time.NewTicker(caeInsDeleteCheckInterval * time.Millisecond)
	timer := time.NewTimer(time.Second * scaleDownTaskTimeout)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			mss.Lock()
			found := false
			currentRequest := 0
			for index := range mss.sortedSlices {
				if mss.sortedSlices[index].workerID == workerID {
					currentRequest = mss.sortedSlices[index].currentRequests
					found = true
					break
				}
			}
			mss.Unlock()
			if !found || currentRequest != 0 {
				continue
			}
			mss.autoScaler.ScaleDown(action.ScaleDownParam{
				WorkerID:      workerID,
				IgnoreApplier: true,
			})
			return
		case <-timer.C:
			log.GetLogger().Warnf("scale down %s timeout, do it by force", workerID)
			mss.autoScaler.ScaleDown(action.ScaleDownParam{
				WorkerID:      workerID,
				IgnoreApplier: true,
			})
			return
		case <-stopChan:
			log.GetLogger().Warnf("scale down check task of %s %s has been canceled",
				mss.funcKeyWithResource, workerID)
			return
		}
	}
}

func connNums(entry scheduleEntry, usedInstance map[string]struct{}, circuitBreakerEnabled bool) int {
	if _, ok := usedInstance[entry.workerID]; ok {
		return math.MaxInt
	}
	if circuitBreakerEnabled && workerbroker.GetInstance().GetWorkerBrokerCPUOverloaded(entry.workerID) {
		return math.MaxInt
	}
	return entry.currentRequests
}

func defaultCheckFunction(entry scheduleEntry, usedInstance map[string]struct{},
	circuitBreakerEnabled bool) (bool, string, string) {
	obj := fmt.Sprintf("%s/%s", entry.funcKeyWithResource, entry.workerID)
	if entry.willDelete {
		return false, obj, "marked willDelete"
	}
	if _, ok := usedInstance[entry.workerID]; ok {
		return false, obj, "used instance"
	}
	if circuitBreakerEnabled && workerbroker.GetInstance().GetWorkerBrokerCPUOverloaded(entry.workerID) {
		return false, obj, "cpu overload"
	}
	isNodeAvailable := getNodeAvailable(entry.nodeIP, entry.workerID)
	if isNodeAvailable && (entry.concurrency-entry.currentRequests > 0) {
		return true, obj, "pass"
	}
	return false, obj, "reached max concurrency"
}

func specifyInsCheckFunction(entry scheduleEntry, usedInstance map[string]struct{}) bool {
	if _, ok := usedInstance[entry.workerID]; ok {
		log.GetLogger().Warnf("specify instance %s check not pass, it is used instance", entry.workerID)
		return false
	}
	isNodeAvailable := getNodeAvailable(entry.nodeIP, entry.workerID)
	if isNodeAvailable && (entry.concurrency-entry.currentRequests > 0) {
		return true
	}
	log.GetLogger().Warnf("specify instance %s check not pass, node available %t, current request %d, limit %d",
		entry.workerID, isNodeAvailable, entry.currentRequests, entry.concurrency)
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
			if !workersview.CheckOnlyOneInstanceLeft(mss.funcKeyWithResource, entry.workerID) {
				break
			}
			log.GetLogger().Infof("this pod is the only one on demand instance so can scale down, "+
				"funcKeyWithResource: %s, podName: %s", mss.funcKeyWithResource, urnutils.ExtractPodName(entry.workerID))
			go mss.autoScaler.ScaleDown(action.ScaleDownParam{WorkerID: entry.workerID})
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
