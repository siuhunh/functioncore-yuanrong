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

// Package autoscaler contains interface and implement of autoscaler. See scale algorithm in func calculateScale*
package autoscaler

import (
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/autoscaler/algorithm"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

const (
	caeAutoScalerEnableEnv = "CAE_AUTO_SCALER_ENABLE"

	// load balance workers will be watched from router ETCD in 1s
	backPressureBufTime = time.Second
	msToS               = 1000

	scaleLatencyWindowCapacity = 10

	// it means there are 25 times in 30s that req queue length is over then limit
	breakBalanceCalculateInHalfMinute = 25

	backPressureLimitNumber = 10

	defaultScaleTimeout = 15 * time.Minute
	// scaleDelay 33s(origin worker deploy) and 13s(vpc)
	scaleDelay = 46
)

var (
	// EnableMicroserviceAutoScaler -
	EnableMicroserviceAutoScaler = os.Getenv(caeAutoScalerEnableEnv) == "true"
)

// AutoScaler interface of AutoScaler
type AutoScaler interface {
	QueueLengthChanged(length, rps int, info common.TraceInfo, queuedTime time.Duration, option queue.BackPressuredOption)
	CalculateScaleDown(currentRequests int, requestLatencyMs int) int
	ScaleDown(scaleDownParam action.ScaleDownParam)
	SetFuncMeta(metadata servicediscovery.FunctionMetaInfo)
	UpdateUsedWorkerNum(num int)
	Reset()
	CheckUsedWorker() bool
}

// BackPressureInstance record instance scaled elsewhere and back-pressured in buffer time
type BackPressureInstance struct {
	position  types.Position
	timestamp time.Time
}

type defaultAutoScaler struct {
	*common.AutoScalerConfig
	requestQue              *queue.RequestQueue
	scaleLatencyAggregation dataaggregation.DataAggregation
	functionKey             string
	funcKeyWithResource     string
	scalingWorkersNum       int

	backPressureInstance BackPressureInstance
	// with status burst, no trigger for scale-down calculation
	burst                    bool
	lastScaleCalculationTime time.Time
	lastScalingTime          time.Time
	algorithm.ScaleInfo
	sync.Mutex
	firstBreakBalanceCalculateTime time.Time
	breakBalanceCalculateCount     int
	scaleTimeout                   time.Duration
}

type microserviceAutoScaler struct {
	scaler *defaultAutoScaler
}

func (micro *microserviceAutoScaler) CheckUsedWorker() bool {
	return false
}

func (micro *microserviceAutoScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
	if length == 0 || !EnableMicroserviceAutoScaler || micro.scaler == nil {
		return
	}
	now := time.Now()
	micro.scaler.Mutex.Lock()
	defer micro.scaler.Mutex.Unlock()
	if micro.scaler.checkBackPressure() {
		go micro.scaler.backPressureRequests(micro.scaler.backPressureInstance.position,
			false, option.ScaleErrorCode)
		return
	}
	if option.IsBackPressured && micro.scaler.lastScaleCalculationTime.IsZero() {
		micro.scaler.lastScaleCalculationTime = now
		return
	}
	micro.scaler.lastScaleCalculationTime = now
	if micro.scaler.scalingWorkersNum > 0 {
		return
	}
	micro.scaler.scalingWorkersNum = 1
	go micro.scaler.retryToScaleUp(info)
	log.GetLogger().Infof("queueLengthChanged for microservice function: %s length | %d | rps | %d | "+
		"scalingWorkersNum: %d traceID: %s", urnutils.AnonymizeTenantKey(micro.scaler.funcKeyWithResource),
		length, rps, micro.scaler.scalingWorkersNum, info.TraceID)
}

func (micro *microserviceAutoScaler) ScaleDown(scaleDownParam action.ScaleDownParam) {
	action.ScaleDown(scaleDownParam, micro.scaler.functionKey, false)
}

func (micro *microserviceAutoScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
	if micro.scaler != nil {
		timeout := getScaleTimeout(metadata)
		micro.scaler.scaleTimeout = timeout
		micro.scaler.CurrentConcurrencyPerWorker = metadata.InstanceMetaData.ConcurrentNum
		log.GetLogger().Debugf("update concurrency for function: %s, %d", urnutils.AnonymizeTenantKey(
			micro.scaler.funcKeyWithResource), metadata.InstanceMetaData.ConcurrentNum)
	}
}

func (micro *microserviceAutoScaler) UpdateUsedWorkerNum(num int) {
	micro.scaler.Mutex.Lock()
	micro.scaler.UsedWorkerNum = num
	micro.scaler.Mutex.Unlock()
}

func (micro *microserviceAutoScaler) Reset() {}

func (micro *microserviceAutoScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	micro.scaler.Mutex.Lock()
	defer micro.scaler.Mutex.Unlock()
	needWorkerNum := currentRequests
	var scaleNum int
	if needWorkerNum >= micro.scaler.UsedWorkerNum {
		scaleNum = 0
	} else {
		scaleNum = micro.scaler.UsedWorkerNum - needWorkerNum
	}
	return scaleNum
}

// InitAutoScalerConfig -
func InitAutoScalerConfig() *common.AutoScalerConfig {
	common.InitDefaultAutoScalerConfig()
	return common.FConfig.AutoScalerConfig
}

// NewDefaultAutoScaler -
func NewDefaultAutoScaler(funcKeyWithResource string, queue *queue.RequestQueue,
	metadata servicediscovery.FunctionMetaInfo) AutoScaler {
	return &defaultAutoScaler{
		scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
			time.Hour, 0, scaleLatencyWindowCapacity),
		AutoScalerConfig:    InitAutoScalerConfig(),
		functionKey:         utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource),
		funcKeyWithResource: funcKeyWithResource,
		scalingWorkersNum:   0,
		requestQue:          queue,
		scaleTimeout:        getScaleTimeout(metadata),
	}
}

// NewMicroServiceAutoScaler -
func NewMicroServiceAutoScaler(funcKeyWithResource string, queue *queue.RequestQueue,
	metadata servicediscovery.FunctionMetaInfo) AutoScaler {
	return &microserviceAutoScaler{
		scaler: &defaultAutoScaler{
			AutoScalerConfig: InitAutoScalerConfig(),
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			functionKey:         utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource),
			funcKeyWithResource: funcKeyWithResource,
			requestQue:          queue,
			scaleTimeout:        getScaleTimeout(metadata),
			scalingWorkersNum:   0,
		},
	}
}

func getScaleTimeout(data servicediscovery.FunctionMetaInfo) time.Duration {
	if data.FuncMetaData.Timeout == 0 || data.FuncMetaData.IsCustomContainer {
		return defaultScaleTimeout
	}
	return time.Duration(scaleDelay+data.FuncMetaData.Timeout+data.ExtendedMetaData.Initializer.InitializerTimeout) *
		time.Second
}

// SetFuncMeta support dynamic concurrency
func (das *defaultAutoScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
	atomic.StoreInt64((*int64)(&das.scaleTimeout), 1)
	timeout := getScaleTimeout(metadata)
	das.Mutex.Lock()
	das.CurrentConcurrencyPerWorker = metadata.InstanceMetaData.ConcurrentNum
	das.scaleTimeout = timeout
	das.Mutex.Unlock()
	log.GetLogger().Infof("update concurrency for function: %s, %d",
		urnutils.AnonymizeTenantKey(das.funcKeyWithResource), metadata.InstanceMetaData.ConcurrentNum)
}

func (das *defaultAutoScaler) getScaleLatencyMs(t time.Time) int {
	scaleLatencyMs := int(das.scaleLatencyAggregation.GetValue(t))
	if scaleLatencyMs < das.MaxEvaluateScaleLatencyMs && scaleLatencyMs != 0 {
		return scaleLatencyMs
	}
	return das.MaxEvaluateScaleLatencyMs
}

// calculateScaleUpNum contains scale up algorithm
func (das *defaultAutoScaler) calculateScaleUpNum(hasNew bool) int {
	// don't trigger scaling up twice for no new requests except for all local worker exit
	if !hasNew && das.UsedWorkerNum != 0 {
		return 0
	}

	return algorithm.CalculateScaleUp(das.AutoScalerConfig, das.ScaleInfo)
}

// Reset resets calculationTime of autoScaler
func (das *defaultAutoScaler) Reset() {
	das.Mutex.Lock()
	das.lastScaleCalculationTime = time.Time{}
	das.Mutex.Unlock()
}

// CheckUsedWorker check if used worker instance
func (das *defaultAutoScaler) CheckUsedWorker() bool {
	das.Mutex.Lock()
	check := das.UsedWorkerNum != 0
	das.Mutex.Unlock()
	return check
}

// QueueLengthChanged observe pending requests to trigger scaling up
func (das *defaultAutoScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
	now := time.Now()
	das.Mutex.Lock()
	defer das.Mutex.Unlock()
	das.PendingRequests = length
	das.RPS = rps
	if length == 0 {
		das.burst = false
		return
	}
	if das.checkBackPressure() {
		go das.backPressureRequests(das.backPressureInstance.position, false, option.ScaleErrorCode)
		return
	}
	if option.IsBackPressured && das.lastScaleCalculationTime.IsZero() {
		das.lastScaleCalculationTime = now
		return
	}
	// for 1 -> n: scale calculation is not triggered at once
	if now.Sub(das.lastScaleCalculationTime) < time.Duration(das.ScaleCalculationIntervalMs)*time.Millisecond {
		return
	}
	das.lastScaleCalculationTime = now

	das.ScalingLatencyMs = das.getScaleLatencyMs(now)
	needScalingNum := das.calculateScaleUpNum(true)
	if needScalingNum > 0 {
		das.burst = true
	}
	incrNeedScalingNum, successBreakBalance := 0, false
	if needScalingNum > das.scalingWorkersNum {
		incrNeedScalingNum = needScalingNum - das.scalingWorkersNum
	} else if needScalingNum == 0 && das.scalingWorkersNum == 0 &&
		now.Sub(das.lastScalingTime) > time.Duration(das.AutoScalerConfig.MaxEvaluatedLatencyMs)*time.Millisecond &&
		das.needBreakBalanceScaleUp(queuedTime, now) {
		// long time no scaling and no scheduled
		incrNeedScalingNum = 1
		successBreakBalance = true
	}
	das.dealBreakBalanceData(incrNeedScalingNum, length, now)
	das.scalingWorkersNum += incrNeedScalingNum
	for i := 0; i < incrNeedScalingNum; i++ {
		go das.retryToScaleUp(info)
	}
	log.GetLogger().Infof("queLenChange | %s | len %d | rps %d | scalingWorker %d | incrNeedScaling %d | "+
		"avgLatency %d | workerTPS %d | scalingLatency %d | breakBalance %t | breakBalanceCalculateCount %d | %s",
		urnutils.FuncKeyWithResToShort(das.funcKeyWithResource), length, rps, das.scalingWorkersNum, incrNeedScalingNum,
		das.RequestLatencyMs, das.WorkerTPS, das.ScalingLatencyMs, successBreakBalance, das.breakBalanceCalculateCount,
		info.TraceID)
}

func (das *defaultAutoScaler) dealBreakBalanceData(incrNeedScalingNum, length int, now time.Time) {
	if incrNeedScalingNum > 0 {
		das.refreshBreakBalanceCalculate(now)
	} else {
		das.breakBalanceCalculate(length, now)
	}
}

// rule1: if queueTime wait over MaxEvaluatedLatencyMs, we scale up one pod
// rule2: queueLength over limit for a while, means we can deal all request, but our deal rate is not quick
//
//	for this situation, we need scale up one pod to break the balance
func (das *defaultAutoScaler) needBreakBalanceScaleUp(queuedTime time.Duration, now time.Time) bool {
	halfMinuteAgo, err := time.ParseDuration("-30s")
	if err != nil {
		log.GetLogger().Errorf("calculate break balance err for function %s when get halfMinuteAgo time, %s",
			urnutils.AnonymizeTenantKey(das.funcKeyWithResource), err.Error())
		return false
	}
	needBreak := now.Add(halfMinuteAgo).After(das.firstBreakBalanceCalculateTime) &&
		das.breakBalanceCalculateCount >= breakBalanceCalculateInHalfMinute

	log.GetLogger().Debugf("check need break balance for function %s, needBreak is %v, "+
		"breakBalanceCalculateCount is %d",
		urnutils.AnonymizeTenantKey(das.funcKeyWithResource), needBreak, das.breakBalanceCalculateCount)

	if queuedTime > time.Duration(das.AutoScalerConfig.MaxEvaluatedLatencyMs)*time.Millisecond || needBreak {
		return true
	}
	return false
}

func (das *defaultAutoScaler) refreshBreakBalanceCalculate(now time.Time) {
	log.GetLogger().Debugf("refresh break balance calculate for function %s",
		urnutils.AnonymizeTenantKey(das.funcKeyWithResource))
	das.firstBreakBalanceCalculateTime = now
	das.breakBalanceCalculateCount = 0
}

func (das *defaultAutoScaler) breakBalanceCalculate(queueLength int, now time.Time) {
	halfMinuteAgo, err := time.ParseDuration("-30s")
	if err != nil {
		log.GetLogger().Errorf("break balance calculate err for function %s when get halfMinuteAgo time, %s",
			urnutils.AnonymizeTenantKey(das.funcKeyWithResource), err.Error())
		return
	}

	inHalfMinute := now.Add(halfMinuteAgo).Before(das.firstBreakBalanceCalculateTime)

	if inHalfMinute {
		if queueLength > das.AutoScalerConfig.BreakBalanceQueueLengthLimit {
			das.breakBalanceCalculateCount++
		}
		log.GetLogger().Debugf("calculate break balance for function %s, queueLength is %d, "+
			"breakBalanceCalculateCount is %d",
			urnutils.AnonymizeTenantKey(das.funcKeyWithResource), queueLength, das.breakBalanceCalculateCount)
	} else {
		das.refreshBreakBalanceCalculate(now)
	}
}

func (das *defaultAutoScaler) retryToScaleUp(info common.TraceInfo) {
	currentWorkerNum, scalingWorkerNum := 0, 0
	for i := 0; i < common.DefaultRetryTimes; i++ {
		das.Lock()
		currentWorkerNum = das.UsedWorkerNum
		scalingWorkerNum = das.scalingWorkersNum
		das.Unlock()

		instance := das.scaleUp(info, currentWorkerNum, scalingWorkerNum, false)
		if instance == nil {
			break
		}
		err := das.workerScaledUp(instance)
		if err == nil {
			break
		}
	}
	das.Lock()
	das.scalingWorkersNum--
	das.lastScalingTime = time.Now()
	das.Unlock()
}

func (das *defaultAutoScaler) handleInnerUserErr(response *types.TCPResponse) {
	needResponseErr := das.UsedWorkerNum == 0
	das.Mutex.Unlock()
	if needResponseErr {
		das.responseError(response)
	}
}

func (das *defaultAutoScaler) makeScaleUpParam(info common.TraceInfo,
	currentWorkerNum, scalingWorkersNum int, isRetry bool) (common.ScaleUpParam, bool) {
	insResource := utils.GetResourceFromFuncKeyWithResource(das.funcKeyWithResource)
	if insResource == nil {
		log.GetLogger().Warnf("failed to scale up function, can't get instance resource from "+
			"funcKeyWithResource %s", urnutils.AnonymizeTenantKey(das.funcKeyWithResource))
		return common.ScaleUpParam{}, false
	}
	// scalingWorkersNum contains itself scale up request
	// OwnedWorkerNum = currentWorkerNum + scalingWorkersNum - 1 means not contain itself
	scaleUpParam := common.ScaleUpParam{InsResource: insResource, TraceID: info.TraceID, FunctionKey: das.functionKey,
		OwnedWorkerNum:   currentWorkerNum + scalingWorkersNum - 1,
		CurrentWorkerNum: currentWorkerNum, ScalingWorkersNum: scalingWorkersNum,
		Timeout: das.scaleTimeout, TraceParent: info.TraceParent}
	if isRetry {
		scaleUpParam.OwnedWorkerNum = -1
	}
	return scaleUpParam, true
}

func (das *defaultAutoScaler) scaleUp(info common.TraceInfo,
	currentWorkerNum, scalingWorkersNum int, isRetry bool) *types.Instance {
	log.GetLogger().Infof("scaleUpFunc | %s | curWorker %d | scalingWorker %d | %s | ready %t",
		urnutils.FuncKeyWithResToShort(das.funcKeyWithResource), currentWorkerNum, scalingWorkersNum, info.TraceID, isRetry)

	scaleUpParam, ok := das.makeScaleUpParam(info, currentWorkerNum, scalingWorkersNum, isRetry)
	if !ok {
		return nil
	}
	scaleStartTime := time.Now()
	instance, errCode, errMsg := action.WrapScaleUpAdaptor(scaleUpParam)
	response := buildScaleResp(errCode, errMsg)
	das.requestQue.SetError(response)
	das.Mutex.Lock()
	if errCode != 0 {
		log.GetLogger().Warnf("failed to scale up function: %s scaling number: %d current worker num: %d "+
			"error: %s errCode: %d traceID: %s", urnutils.AnonymizeTenantKey(das.funcKeyWithResource), das.scalingWorkersNum,
			das.UsedWorkerNum, errMsg, errCode, info.TraceID)

		// if error code is no such function, response the error
		if errCode == snerror.FuncMetaNotFound {
			das.Mutex.Unlock()
			das.responseError(response)
			return nil
		}

		if errCode == snerror.InnerRuntimeInitTimeoutCode {
			// unlock in handleInnerUserErr
			das.handleInnerUserErr(response)
			return instance
		}

		// no response error and wait for retrying scaling or local worker turned idle until request queued timeout
		// HINT: two scaling results may be ReachMaxInstancesCode and success, but the later may come behind the former
		if das.UsedWorkerNum != 0 || errCode == snerror.GettingPodErrorCode || errCode == snerror.CancelGeneralizePod ||
			((errCode == snerror.ReachMaxInstancesCode || errCode == snerror.ReachMaxOnDemandInstancesPerTenant) &&
				das.scalingWorkersNum != 0) {
			das.Mutex.Unlock()
			return nil
		}
		das.Mutex.Unlock()
		das.responseError(response)
		return nil
	}

	log.GetLogger().Infof("scale up function successfully, funcKeyWithResource: %s, deployedNode: %s, "+
		"scaling number: %d, traceID: %s", urnutils.AnonymizeTenantKey(das.funcKeyWithResource),
		instance.DeployedNode, das.scalingWorkersNum, info.TraceID)

	scaleFinishTime := time.Now()
	das.scaleLatencyAggregation.AddSample(int64(scaleFinishTime.Sub(scaleStartTime)/time.Millisecond), scaleFinishTime)

	das.Mutex.Unlock()

	return instance
}

func buildScaleResp(errCode uint32, errMsg string) *types.TCPResponse {
	if errCode == 0 {
		return nil
	}
	if errCode == snerror.InnerRuntimeInitTimeoutCode {
		errCode = snerror.RuntimeInitTimeoutCode
	}
	var logs, summary string
	logs, summary, errMsg = snerror.DecodeFromErrMSG(errCode, errMsg)
	return &types.TCPResponse{
		ErrorCode:    errCode,
		ErrorMessage: errMsg,
		Logs:         logs,
		Summary:      summary,
	}
}

func (das *defaultAutoScaler) responseError(response *types.TCPResponse) {
	log.GetLogger().Warnf("failed to scale up for %s and response error, errCode: %d, errMsg: %s",
		urnutils.AnonymizeTenantKey(das.funcKeyWithResource), response.ErrorCode, response.ErrorMessage)

	for {
		forwardRequests := das.requestQue.Pop(das.CurrentConcurrencyPerWorker)
		if len(forwardRequests) == 0 {
			break
		}
		for index := range forwardRequests {
			forwardRequests[index].Response <- response
		}
	}
}

func (das *defaultAutoScaler) workerScaledUp(instance *types.Instance) error {
	workerAddr := instance.IP + ":" + instance.Port
	if instance.DeployedIP == internal.SelfNodeIP {
		// invoke local worker to handle requests
		brokerConfig := &workerbroker.BrokerConfig{
			FuncKeyWithResource: das.funcKeyWithResource,
			WorkerID:            instance.InstanceID,
			WorkerAddr:          workerAddr,
			IsMinInstance:       instance.IsReserved,
			IsDirectFunc:        instance.IsDirectFunc,
			HasInitializer:      instance.HasInitializer,
			BusinessType:        instance.BusinessType,
			PodUID:              instance.PodUID,
			ContainerIDs:        instance.ContainerIDs,
			Resource:            instance.Resource,
			ViewResource:        instance.ViewResource,
		}
		if err := workerbroker.GetInstance().CreateWorkerBroker(brokerConfig); err != nil {
			log.GetLogger().Errorf("failed to create workerBroker, instance: %s", instance.InstanceID)
			// force to delete question worker
			action.ScaleDown(action.ScaleDownParam{WorkerID: instance.InstanceID}, das.functionKey, true)
			return err
		}
		return nil
	}

	return das.createRemoteWorkerBroker(instance)
}

func (das *defaultAutoScaler) createRemoteWorkerBroker(instance *types.Instance) error {
	// no need to notify to create reserved instance
	if !instance.IsReserved {
		workerAddr := instance.IP + ":" + instance.Port
		// notify remote proxy to create worker
		if err := common.NotifyRemote(instance.DeployedIP, &common.FunctionProperty{
			TenantID:   common.BusProxyTenantID,
			InstanceID: instance.InstanceID,
		}, &common.NotifyEvent{
			NotifyType:          common.ChangeState,
			Enable:              true,
			FuncKeyWithResource: das.funcKeyWithResource,
			WorkerAddress:       workerAddr,
			WorkerOwnerIP:       instance.OwnerIP,
			IsDirectFunc:        instance.IsDirectFunc,
			HasInitializer:      instance.HasInitializer,
			BusinessType:        instance.BusinessType,
			PodUID:              instance.PodUID,
			ContainerIDs:        instance.ContainerIDs,
			Resource:            instance.Resource,
			ViewResource:        instance.ViewResource,
		}); err != nil {
			log.GetLogger().Errorf("failed to notify remote %s %s for %s with error: %s", instance.DeployedIP,
				instance.InstanceID, urnutils.AnonymizeTenantKey(das.funcKeyWithResource), err.Error())
			// force to delete question worker
			das.ScaleDown(action.ScaleDownParam{WorkerID: instance.InstanceID})
			// return error to send scale up request to worker manager with owned worker num -1
			// worker manager will scale up a normal instance
			return err
		}
	}

	if instance.OwnerIP != internal.SelfNodeIP {
		position := types.Position{
			InstanceID: instance.InstanceID,
			NodeID:     instance.OwnerIP,
		}
		// record instance and then back pressure requests from queue in case that requests trigger scaling before
		das.addToBackPressureInstance(instance)
		das.backPressureRequests(position, true, 0)
	} else {
		// ownerIP is localIP and deployIP is not localIP, it's remote worker
		workerbroker.GetInstance().CreateRemoteBroker(das.funcKeyWithResource, instance.InstanceID,
			instance.DeployedIP, "")
	}

	return nil
}

func (das *defaultAutoScaler) addToBackPressureInstance(instance *types.Instance) {
	das.Lock()
	position := &das.backPressureInstance.position
	(*position).NodeID = instance.OwnerIP
	(*position).InstanceID = instance.InstanceID
	das.backPressureInstance.timestamp = time.Now()
	das.Unlock()
}

// checkBackPressure note: called in data mutex
func (das *defaultAutoScaler) checkBackPressure() bool {
	return das.scalingWorkersNum == 0 &&
		time.Since(das.backPressureInstance.timestamp) < backPressureBufTime
}

func (das *defaultAutoScaler) backPressureRequests(position types.Position, createRemote bool, code uint32) {
	// scalingWorkers and the back pressure worker share all the requests
	requests := das.requestQue.PopN(das.scalingWorkersNum+1, das.CurrentConcurrencyPerWorker)

	if num := len(requests); num == 0 {
		log.GetLogger().Warnf("request queue is empty, scale more than expected: %s",
			das.backPressureInstance.position.InstanceID)
		return
	}
	var count int
	for index := range requests {
		if !createRemote && requests[index].Request != nil &&
			requests[index].Request.BackPressureNums > backPressureLimitNumber {
			if code == snerror.ReachMaxInstancesCode ||
				code == snerror.ReachMaxOnDemandInstancesPerTenant {
				das.requestQue.Push(requests[index])
				log.GetLogger().Infof("back pressure number reach limit: %d ,func: %s,"+
					"scaleErrorCode: %d, traceID:%s", backPressureLimitNumber,
					urnutils.AnonymizeTenantKey(das.funcKeyWithResource), code, requests[index].TraceID)
				continue
			}
			requests[index].Response <- &types.TCPResponse{ErrorCode: constants.BackpressureCode}
			log.GetLogger().Infof("backPressureReq | noSchedule | %s | %s",
				urnutils.FuncKeyWithResToShort(das.funcKeyWithResource), requests[index].TraceID)
			continue
		}
		requests[index].Position = position
		requests[index].Response <- &types.TCPResponse{ErrorCode: constants.BackpressureCode,
			InstanceID: position.InstanceID, NodeID: position.NodeID}
		log.GetLogger().Infof("backPressureReq | schedule | %s | position %v | %s",
			urnutils.FuncKeyWithResToShort(das.funcKeyWithResource), position, requests[index].TraceID)
		count++
	}
	if count > 0 {
		log.GetLogger().Infof("back pressure request num: %d, func: %s, position: %v", count,
			urnutils.AnonymizeTenantKey(das.funcKeyWithResource), position)
	}
}

// UpdateUsedWorkerNum the number of workers in use is updated in real time to prevent capacity expansion exceptions.
func (das *defaultAutoScaler) UpdateUsedWorkerNum(num int) {
	das.Mutex.Lock()
	das.UsedWorkerNum = num
	das.Mutex.Unlock()
}

// CalculateScaleDown contains scale down algorithm
func (das *defaultAutoScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	das.Mutex.Lock()
	defer das.Mutex.Unlock()
	das.CurrentRequests = currentRequests
	das.RequestLatencyMs = requestLatencyMs
	if requestLatencyMs > 0 {
		// if user code running cost same time, if workerTPS round down (6.7 => 6)
		// it will be a problem about the request come as 6\7\6\7\6, but workerTPS calculated is 6
		// so if two instance exist, it will never scale down a part of one
		// because bus rule is: in one minute all calculate need scale down
		// but in this situation only half calculate need scale down, so it can't scale down, it's not right
		das.WorkerTPS = int(math.Ceil(float64(das.CurrentConcurrencyPerWorker*msToS) / float64(requestLatencyMs)))
	}
	log.GetLogger().Debugf("calculate scale down number, currentConcurrencyPerWorker: %d workerTPS: %d "+
		"avgLatency: %d usedWorkerNum: %d burst: %t, function: %s currentRequests: %d",
		das.CurrentConcurrencyPerWorker, das.WorkerTPS,
		das.RequestLatencyMs, das.UsedWorkerNum, das.burst,
		urnutils.AnonymizeTenantKey(das.funcKeyWithResource), das.CurrentRequests)

	if das.burst {
		return 0
	}

	return algorithm.CalculateScaleDown(das.AutoScalerConfig, das.ScaleInfo)
}

// ScaleDown -
func (das *defaultAutoScaler) ScaleDown(scaleDownParam action.ScaleDownParam) {
	action.ScaleDown(scaleDownParam, das.functionKey, false)
}
