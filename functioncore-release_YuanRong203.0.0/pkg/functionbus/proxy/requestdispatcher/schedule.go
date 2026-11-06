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

// Package requestdispatcher contains request lb, autoscaler and so on
package requestdispatcher

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/queue"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/proxyview"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
)

const (
	minInstancePriorityRate           = 2000
	localWorkerPriorityRate           = 1000
	localWorkerWillDeletePriorityRate = 100
	remoteWorkerPriorityRate          = 0

	msToS = 1000

	defaultRequestLatencySampleNumThreshold = 50

	delayedTickWindow = 5

	circuitBreakerEnabledWorkerID = "NoInstanceWithCircuitBreakerEnabled"
)

// Scheduler -
type Scheduler interface {
	Schedule(tcpRequest *types.TCPRequest, traceID string) string
	ScheduleAll() []string
	Update(metrics *workerevaluator.WorkerMetrics)
	SetFunctionMetadata(metadata servicediscovery.FunctionMetaInfo)
	UpdateWorkerWillDeleteStatus(info *willDeleteInfo)
	GetSchedulerType() string
	PreSchedule(workerID string)
	SetKeepaliveTime(kaTime int)
	SetDispatcher(dispatcher *RequestDispatcher)
}

type scheduleEntry struct {
	workerID            string
	funcKeyWithResource string
	currentRequests     int
	concurrency         int
	isLocalWorker       bool
	isMinInstance       bool
	priority            int
	nodeIP              string
	willDelete          bool
	scaleDownCh         chan interface{}
}

type scaleWindow struct {
	tick        int
	scaleNum    int
	maxRequests int
}

type defaultScheduler struct {
	requestLatencyAggregation dataaggregation.AtomicAverage
	autoScaler                autoscaler.AutoScaler
	scaleWindow               scaleWindow
	sortedSlices              []scheduleEntry
	funcKeyWithResource       string
	lastConcurrency           int
	concurrency               int
	currentAllRequests        int
	dynamicConcurrency        bool
	minInstanceIdleModeSwitch bool
	businessType              string
	enableRemoteDebug         bool
	isBridge                  bool
	enableWebSocket           bool
	rnd                       *rand.Rand
	keepaliveInterval         int
	needIns                   *queue.MonotonicQueue
	scaleNum                  *queue.MonotonicQueue
	sync.Mutex
}

// Schedule -
func (ddp *defaultScheduler) Schedule(_ *types.TCPRequest, _ string) string {
	ddp.Mutex.Lock()
	defer ddp.Mutex.Unlock()
	if ddp.isBridge {
		ddp.rnd.Shuffle(len(ddp.sortedSlices), func(i, j int) {
			ddp.sortedSlices[i], ddp.sortedSlices[j] = ddp.sortedSlices[j], ddp.sortedSlices[i]
		})
	}
	// first in sortedSlices is the best choice except for the instance where the request is full
	for i := 0; i < len(ddp.sortedSlices); i++ {
		entry := ddp.sortedSlices[i]
		// proxy available and concurrency check
		isNodeAvailable := getNodeAvailable(entry.nodeIP, entry.workerID)
		if isNodeAvailable && (entry.concurrency-entry.currentRequests > 0) {
			ddp.sortedSlices[i].currentRequests++
			ddp.sortedSlices[i].computePriority(ddp.minInstanceIdleModeSwitch)
			ddp.sortWorkerIDs()
			ddp.addAllRequestNums(1)
			return entry.workerID
		}
	}
	return ""
}

// ScheduleAll not used
func (ddp *defaultScheduler) ScheduleAll() []string {
	return nil
}

// PreSchedule current do nothing
func (ddp *defaultScheduler) PreSchedule(workerID string) {
	return
}

func (ddp *defaultScheduler) GetSchedulerType() string {
	return constants.BusinessTypeFaaS
}

// SetKeepaliveTime -
func (ddp *defaultScheduler) SetKeepaliveTime(kaTime int) {
	ddp.Mutex.Lock()
	defer ddp.Mutex.Unlock()
	ddp.keepaliveInterval = kaTime
}

// SetDispatcher current do nothing
func (ddp *defaultScheduler) SetDispatcher(dispatcher *RequestDispatcher) {
	return
}

func (ddp *defaultScheduler) addAllRequestNums(reqNum int) {
	ddp.currentAllRequests = ddp.currentAllRequests + reqNum
	if ddp.currentAllRequests > ddp.scaleWindow.maxRequests {
		ddp.scaleWindow.maxRequests = ddp.currentAllRequests
	}
}

func (ddp *defaultScheduler) delAllRequestNums(finishReqNum int) {
	requestsNum := ddp.currentAllRequests - finishReqNum
	if requestsNum < 0 {
		log.GetLogger().Warnf("function: %s currentAllRequests: %d is small than finishReqNum: %d",
			urnutils.AnonymizeTenantKey(ddp.funcKeyWithResource), ddp.currentAllRequests, finishReqNum)
		requestsNum = 0
	}
	ddp.currentAllRequests = requestsNum
}

func newDefaultScheduler(funcKeyWithResource string, autoScaler autoscaler.AutoScaler,
	metadata servicediscovery.FunctionMetaInfo, stopCh <-chan struct{}) Scheduler {
	if stopCh == nil {
		return nil
	}
	autoScaler.SetFuncMeta(metadata)
	requestLatencySampleNumThreshold := defaultRequestLatencySampleNumThreshold
	if config := common.FConfig.AutoScalerConfig; config != nil && config.RequestLatencyAggregationThreshold != 0 {
		requestLatencySampleNumThreshold = config.RequestLatencyAggregationThreshold
	}
	scheduler := &defaultScheduler{
		requestLatencyAggregation: dataaggregation.NewAtomicAverage(int64(requestLatencySampleNumThreshold)),
		autoScaler:                autoScaler,
		sortedSlices:              make([]scheduleEntry, 0),
		funcKeyWithResource:       funcKeyWithResource,
		isBridge:                  metadata.FuncMetaData.IsBridgeFunction,
		rnd:                       rand.New(rand.NewSource(time.Now().UnixNano())),
		lastConcurrency:           metadata.InstanceMetaData.ConcurrentNum,
		concurrency:               metadata.InstanceMetaData.ConcurrentNum,
		minInstanceIdleModeSwitch: metadata.InstanceMetaData.IdleMode,
		dynamicConcurrency:        false,
		scaleWindow:               scaleWindow{},
		needIns:                   queue.NewMonotonicQueue(int(time.Minute / time.Second)),
		scaleNum:                  queue.NewMonotonicQueue(int(time.Minute / time.Second)),
		businessType:              metadata.FuncMetaData.BusinessType,
		enableRemoteDebug:         metadata.FuncMetaData.EnableRemoteDebug,
		enableWebSocket:           metadata.ExtendedMetaData.EnableWebsocket,
	}

	if !metadata.FuncMetaData.IsBridgeFunction {
		go func() {
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
		}()
	} else {
		log.GetLogger().Debugf("bridge function no need to check scale down, funcKeyWithResource %s", funcKeyWithResource)
	}
	return scheduler
}

func (ddp *defaultScheduler) resetScaleWindow() {
	ddp.scaleWindow.scaleNum = 0
	ddp.scaleWindow.tick = 0
	ddp.scaleWindow.maxRequests = ddp.currentAllRequests
}

// compute priority value of scheduling worker instance.
// Calculation formula is: priority = (currentRequests + PriorityRate
// PriorityRate equals the instance priorityRate plus the number of requests.
// The higher the number of requests, the higher the priority.
func (entry *scheduleEntry) computePriority(minInstanceIdleModeSwitch bool) {
	if entry.concurrency == 0 {
		log.GetLogger().Warnf("concurrency of workerID %s is 0", entry.workerID)
		entry.priority = 0
		return
	}
	if entry.isMinInstance {
		if minInstanceIdleModeSwitch {
			entry.priority = minInstancePriorityRate + entry.currentRequests
		} else {
			// load balance for min instance
			entry.priority = minInstancePriorityRate + entry.concurrency - entry.currentRequests
		}
		return
	}
	if entry.isLocalWorker {
		if entry.willDelete {
			entry.priority = localWorkerWillDeletePriorityRate + entry.currentRequests
		} else {
			entry.priority = localWorkerPriorityRate + entry.currentRequests
		}
		return
	}
	entry.priority = remoteWorkerPriorityRate + entry.currentRequests
}

func getNodeAvailable(nodeIP, workerID string) bool {
	if nodeIP == internal.SelfNodeIP {
		return true
	}
	proxyAvailableStatus := proxyview.GetProxyAvailableStatus(nodeIP)
	if !proxyAvailableStatus {
		log.GetLogger().Warnf("worker %s in node %s, proxy available status is unavailable",
			workerID, nodeIP)
	}
	return proxyAvailableStatus
}

// SetFunctionMetadata update function meta data
func (ddp *defaultScheduler) SetFunctionMetadata(metadata servicediscovery.FunctionMetaInfo) {
	ddp.Mutex.Lock()
	ddp.setConcurrency(metadata.InstanceMetaData.ConcurrentNum)
	// waiting for min instance update to recalculate priority is acceptable
	ddp.minInstanceIdleModeSwitch = metadata.InstanceMetaData.IdleMode
	ddp.businessType = metadata.FuncMetaData.BusinessType
	ddp.enableRemoteDebug = metadata.FuncMetaData.EnableRemoteDebug
	ddp.requestLatencyAggregation.Reset()
	ddp.Mutex.Unlock()
	log.GetLogger().Infof("prepare to set func meta, funcName: %s", metadata.FuncMetaData.FunctionVersionURN)
	ddp.autoScaler.SetFuncMeta(metadata)
}

func (ddp *defaultScheduler) setConcurrency(concurrency int) {
	diff := concurrency - ddp.lastConcurrency
	if diff == 0 {
		log.GetLogger().Infof("concurrency for %s is still %d", ddp.funcKeyWithResource, concurrency)
		return
	}

	for index := range ddp.sortedSlices {
		ddp.sortedSlices[index].concurrency += diff
	}
	ddp.concurrency += diff
	log.GetLogger().Infof("update concurrency from %d to %d for function %s, current average concurrency is %d",
		ddp.lastConcurrency, concurrency, ddp.funcKeyWithResource, ddp.concurrency)

	ddp.lastConcurrency = concurrency
}

func (ddp *defaultScheduler) calculateScheduleEntry(old *scheduleEntry,
	metrics *workerevaluator.WorkerMetrics) *scheduleEntry {
	latenciesLen := len(metrics.Latencies)
	if latenciesLen != 0 {
		if old.currentRequests < latenciesLen {
			log.GetLogger().Warnf("currentRequests %d is small than latenciesLen %d, workerID %s",
				old.currentRequests, latenciesLen, old.workerID)
			ddp.delAllRequestNums(old.currentRequests)
			old.currentRequests = 0
		} else {
			ddp.delAllRequestNums(latenciesLen)
			old.currentRequests -= latenciesLen
		}

		ddp.requestLatencyAggregation.AddSamples(metrics.Latencies)
	}
	return old
}

func (ddp *defaultScheduler) sortWorkerIDs() {
	sort.Slice(ddp.sortedSlices, func(i, j int) bool {
		return ddp.sortedSlices[i].priority > ddp.sortedSlices[j].priority
	})
}

func (ddp *defaultScheduler) updateSortedWorkers(metrics *workerevaluator.WorkerMetrics) {
	// unavailable worker
	if atomic.LoadUint32(&metrics.Enable) == 0 {
		for index := range ddp.sortedSlices {
			if ddp.sortedSlices[index].workerID == metrics.ID {
				ddp.delAllRequestNums(ddp.sortedSlices[index].currentRequests)
				ddp.sortedSlices = append(ddp.sortedSlices[:index], ddp.sortedSlices[index+1:]...)
				break
			}
		}
		return
	}

	// available worker
	var found = false
	for index := range ddp.sortedSlices {
		if ddp.sortedSlices[index].workerID == metrics.ID {
			ddp.calculateScheduleEntry(&ddp.sortedSlices[index], metrics)
			ddp.sortedSlices[index].computePriority(ddp.minInstanceIdleModeSwitch)
			found = true
		}
	}
	if !found {
		log.GetLogger().Infof("add new available worker %s to sorted slice %s, currentRequests %d",
			metrics.ID, urnutils.AnonymizeTenantKey(metrics.FuncKeyWithResource), metrics.RecoveredRequestNum)
		entry := scheduleEntry{
			workerID:            metrics.ID,
			funcKeyWithResource: ddp.funcKeyWithResource,
			concurrency:         ddp.concurrency,
			currentRequests:     metrics.RecoveredRequestNum,
			priority:            0,
			isLocalWorker:       metrics.IsLocalWorker,
			isMinInstance:       metrics.IsMinInstance,
			nodeIP:              metrics.NodeIP,
		}
		entry.computePriority(ddp.minInstanceIdleModeSwitch)
		ddp.addAllRequestNums(entry.currentRequests)
		ddp.sortedSlices = append(ddp.sortedSlices, entry)
	}
	ddp.sortWorkerIDs()
}

// UpdateWorkerWillDeleteStatus is to update worker will delete status
func (ddp *defaultScheduler) UpdateWorkerWillDeleteStatus(info *willDeleteInfo) {
	ddp.Lock()
	defer ddp.Unlock()
	for index := range ddp.sortedSlices {
		if ddp.sortedSlices[index].workerID == info.workerID {
			ddp.sortedSlices[index].willDelete = info.status
			ddp.sortedSlices[index].computePriority(ddp.minInstanceIdleModeSwitch)
			break
		}
	}
	// trigger sort workerIDs
	ddp.sortWorkerIDs()
}

func (ddp *defaultScheduler) deleteInSlices(workerID string, slices []scheduleEntry) []scheduleEntry {
	for index := range slices {
		if slices[index].workerID == workerID {
			slices = append(slices[:index], slices[index+1:]...)
			break
		}
	}
	return slices
}

func (ddp *defaultScheduler) sumCurrentRequests() int {
	requests := 0
	for index := range ddp.sortedSlices {
		requests += ddp.sortedSlices[index].currentRequests
	}

	if requests < 0 {
		log.GetLogger().Warnf("total request number(%d) is invalid", requests)
		for index := range ddp.sortedSlices {
			log.GetLogger().Debugf("workerID %s currentRequests is %d",
				requests, ddp.sortedSlices[index].workerID, ddp.sortedSlices[index].currentRequests)
		}
	}

	return requests
}

// Update update scheduler entry
func (ddp *defaultScheduler) Update(metrics *workerevaluator.WorkerMetrics) {
	log.GetLogger().Debugf("update metrics, worker(%s), latency len: %d", metrics.ID, len(metrics.Latencies))
	ddp.Mutex.Lock()
	ddp.updateSortedWorkers(metrics)
	usedWorkerNum := len(ddp.sortedSlices)
	ddp.Mutex.Unlock()
	// the number of instances needs to be updated immediately to prevent capacity expansion exceptions.
	ddp.autoScaler.UpdateUsedWorkerNum(usedWorkerNum)
}

// new design for checkScaleDown:
// introduce MonotonicQueue that can get maximum from last time-windows with O(1) time complexity
// every second, we:
// 1. calculate current scale down num with currentAllRequests, and push (-scaleNum) to ddp.scaleNum
// 2. use [len(ddp.sortedSlices) - scaleNum] as the number of instances we need, and push it to ddp.needIns
// 3. now we can get both minScaleNum and maxNeedIns in the last time-windows
// 4. compare minScaleNum and [len(ddp.sortedSlices)-maxNeedIns], use smaller one  to scale down instance
// see TestCheckScaleDown for test cases
func (ddp *defaultScheduler) checkScaleDown() {
	ddp.Mutex.Lock()
	defer ddp.Mutex.Unlock()

	if len(ddp.sortedSlices) == 0 {
		ddp.scaleNum.Push(0)
		ddp.needIns.Push(0)
		return
	}
	avgLatencyMs := int(ddp.requestLatencyAggregation.GetValue())
	scaleNum := ddp.autoScaler.CalculateScaleDown(ddp.currentAllRequests, avgLatencyMs)

	if scaleNum > len(ddp.sortedSlices) || scaleNum < 0 {
		ddp.scaleNum.Push(0)
		ddp.needIns.Push(len(ddp.sortedSlices))
		log.GetLogger().Warnf("invalid scale down number %d, sorted slice len %d, function: %s",
			scaleNum, len(ddp.sortedSlices), urnutils.AnonymizeTenantKey(ddp.funcKeyWithResource))
		return
	}
	ddp.scaleNum.Push(-scaleNum)
	ddp.needIns.Push(len(ddp.sortedSlices) - scaleNum)
	if scaleNum == 0 {
		return
	}

	interval := ddp.getKeepAliveTimeInternal()
	ddp.needIns.Resize(interval)
	ddp.scaleNum.Resize(interval)

	minScaleDown := -ddp.scaleNum.GetMax()
	maxNeedIns := ddp.needIns.GetMax()
	if len(ddp.sortedSlices)-maxNeedIns < minScaleDown {
		minScaleDown = len(ddp.sortedSlices) - maxNeedIns
	}
	if minScaleDown > 0 && ddp.scaleNum.Len() >= interval {
		ddp.sortWorkerIDs()
		index := len(ddp.sortedSlices) - 1
		for ; index >= len(ddp.sortedSlices)-minScaleDown; index-- {
			entry := ddp.sortedSlices[index]
			// isMinInstance condition: reserved instance
			// websocketType condition: websocket instance which scheduled by faas-scheduler
			// if businessType in function meta is WEBSOCKET, skip scale in check
			if entry.isMinInstance || commonUtils.IsWebSocketFunc(ddp.businessType, ddp.enableRemoteDebug,
				ddp.enableWebSocket) {
				break
			}
			if entry.currentRequests != 0 {
				log.GetLogger().Warnf("select scaled down worker %s, but request num on it is %d,"+
					" wait request finished", entry.workerID, entry.currentRequests)
				break
			}
			go ddp.autoScaler.ScaleDown(action.ScaleDownParam{WorkerID: entry.workerID})
		}
		ddp.sortedSlices = ddp.sortedSlices[0 : index+1]
	}
}

func (ddp *defaultScheduler) getKeepAliveTimeInternal() int {
	if common.FConfig.DynamicKeepAliveConfig.Enable && ddp.keepaliveInterval > 0 {
		return ddp.keepaliveInterval
	}
	keepAliveTimeForWorker := int(internal.IdleTimeForWorker / time.Second)
	return keepAliveTimeForWorker
}
