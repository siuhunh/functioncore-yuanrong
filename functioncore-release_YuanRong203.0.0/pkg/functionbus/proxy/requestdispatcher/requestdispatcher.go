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
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxy/workersview"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

const defaultConcurrency = 100

// RequestDispatcher -
type RequestDispatcher struct {
	requestQueue        *queue.RequestQueue
	funcKeyWithResource string
	scheduler           Scheduler
	scaler              autoscaler.AutoScaler
	stopChan            chan struct{}
	concurrency         uint32 // atomic
	stopOnce            sync.Once
	workerDispatcher    *WorkerDispatcher
}

var errScheduleFailed = errors.New("schedule failed")

var queueType = parseQueueType()

func parseQueueType() string {
	q := os.Getenv("QueueType")
	if q == "" {
		log.GetLogger().Warnf("ENV of QueueType is empty")
	}
	return q
}

// CreateRequestDispatcher -
func CreateRequestDispatcher(funcKeyWithResource string,
	metadata servicediscovery.FunctionMetaInfo, isCAEFunc bool) *RequestDispatcher {
	stopCh := make(chan struct{})
	requestQueue := queue.NewRequestQueue(queueType, internal.RequestQueueTimeout, stopCh)
	var scheduler Scheduler
	var scaler autoscaler.AutoScaler
	if metadata.FuncMetaData.BusinessType == constants.BusinessTypeCAE || isCAEFunc {
		metadata.InstanceMetaData.ConcurrentNum = DefaultMircoServiceConcurrency
		scaler = autoscaler.NewMicroServiceAutoScaler(funcKeyWithResource, requestQueue, metadata)
		scheduler = newMicronServiceScheduler(funcKeyWithResource, scaler, metadata, stopCh)
	} else {
		metadata.InstanceMetaData.ConcurrentNum = sanitizeConcurrency(metadata.InstanceMetaData.ConcurrentNum)
		scaler = autoscaler.NewDefaultAutoScaler(funcKeyWithResource, requestQueue, metadata)
		scheduler = newDefaultScheduler(funcKeyWithResource, scaler, metadata, stopCh)
	}
	rd := &RequestDispatcher{
		funcKeyWithResource: funcKeyWithResource,
		scaler:              scaler,
		requestQueue:        requestQueue,
		scheduler:           scheduler,
		stopChan:            stopCh,
		concurrency:         uint32(metadata.InstanceMetaData.ConcurrentNum),
	}
	if metadata.FuncMetaData.BusinessType == constants.BusinessTypeCAE || isCAEFunc {
		scheduler.SetDispatcher(rd)
	}
	return rd
}

// SetSchedulerKeepaliveTime -
func (rd *RequestDispatcher) SetSchedulerKeepaliveTime(kaTime int) {
	rd.scheduler.SetKeepaliveTime(kaTime)
}

// GetRequestQueueLen returns the request queue length
func (rd *RequestDispatcher) GetRequestQueueLen() int {
	if rd.requestQueue == nil {
		return 0
	}
	return rd.requestQueue.Len()
}

// UpdateWorkerWillDeleteStatus trigger modification of worker priority
func (rd *RequestDispatcher) UpdateWorkerWillDeleteStatus(info *willDeleteInfo) {
	if rd.scheduler == nil {
		log.GetLogger().Warnf("RequestDispatcher scheduler is nil for %s %t", info.workerID, info.status)
		return
	}
	rd.scheduler.UpdateWorkerWillDeleteStatus(info)
}

// OnObserved observe workerEvaluator
func (rd *RequestDispatcher) OnObserved(content interface{}, recovering bool) {
	workerMetric, ok := content.(*workerevaluator.WorkerMetrics)
	if !ok {
		log.GetLogger().Warnf("failed to convert content to WorkerMetrics")
		return
	}
	rd.scheduler.Update(workerMetric)
	// no trigger scheduling when recovering
	if !recovering {
		// local workers update, remote=false
		rd.requestQueue.ScheduleWorkerChanged(false)
	}
}

// Start -
func (rd *RequestDispatcher) Start() {
	workerevaluator.GetInstance().Observe(rd.funcKeyWithResource, rd)
	workersview.GetWorkersView().ObserveWorkers(rd.funcKeyWithResource, rd.requestQueue.ScheduleWorkerChanged)
	rd.requestQueue.RegisterHandler(rd.schedule)
	rd.requestQueue.ObserveLength(rd.scaler)
}

// Stop -
func (rd *RequestDispatcher) Stop() {
	requests := rd.requestQueue.Pop(rd.requestQueue.Len())
	responseFuncMetaUpdate(requests)
	workerevaluator.GetInstance().UnObserve(rd.funcKeyWithResource)
	workersview.GetWorkersView().UnobserveWorkers(rd.funcKeyWithResource)
	rd.stopOnce.Do(func() {
		close(rd.stopChan)
	})
	log.GetLogger().Debugf("request dispatcher exited funcKeyWithResource:%s",
		urnutils.AnonymizeTenantKey(rd.funcKeyWithResource))
}

// SetInstanceMetadata -
func (rd *RequestDispatcher) SetInstanceMetadata(metadata servicediscovery.FunctionMetaInfo) {
	metadata.InstanceMetaData.ConcurrentNum = sanitizeConcurrency(metadata.InstanceMetaData.ConcurrentNum)
	atomic.StoreUint32(&rd.concurrency, uint32(metadata.InstanceMetaData.ConcurrentNum))
	if rd.scaler != nil {
		log.GetLogger().Infof("prepare to reset, funcName: %s", metadata.FuncMetaData.FunctionVersionURN)
		rd.scaler.Reset()
		log.GetLogger().Infof("finish reset, funcName: %s", metadata.FuncMetaData.FunctionVersionURN)
	}
	rd.scheduler.SetFunctionMetadata(metadata)
}

// StopWhenTerminate stops the request dispatcher when Functiontask starts to update
func (rd *RequestDispatcher) StopWhenTerminate() {
	requests := rd.requestQueue.Pop(rd.requestQueue.Len())
	responseFunctionTaskUpdating(requests)
	workerevaluator.GetInstance().UnObserve(rd.funcKeyWithResource)
	workersview.GetWorkersView().UnobserveWorkers(rd.funcKeyWithResource)
	close(rd.stopChan)
	log.GetLogger().Debugf("request dispatcher exited funcKeyWithResource:%s",
		urnutils.AnonymizeTenantKey(rd.funcKeyWithResource))
}

func sanitizeConcurrency(n int) int {
	if n <= 0 || n > math.MaxUint32 {
		n = defaultConcurrency
	}
	return n
}

func responseFuncMetaUpdate(requests []*types.DispatcherRequest) {
	for i := range requests {
		requests[i].Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.UserFuncIsUpdatedCode,
			ErrorMessage: proxyerrors.UserFuncIsUpdatedErr,
		}
	}
}

func responseFunctionTaskUpdating(requests []*types.DispatcherRequest) {
	for i := range requests {
		requests[i].Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.IsUpgradingCode,
			ErrorMessage: proxyerrors.IsUpgradingErr,
		}
	}
}

func (rd *RequestDispatcher) schedule(request *types.DispatcherRequest) error {
	if request.Canceled {
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.RequestCanceledCode,
			ErrorMessage: proxyerrors.RequestCanceledError,
		}
		cancel.GetInstance().DeleteRequest(request.FutureID)
		return nil
	}

	if request.Request.IsInvokeAll {
		if rd.scheduler.GetSchedulerType() != constants.BusinessTypeCAE {
			log.GetLogger().Warnf("invokeAll request, businessType is not CAE, traceID: %s", request.TraceID)
			request.Response <- &types.TCPResponse{}
			return nil
		}
		// cae invokeAll request: schedule on local and remote workers
		log.GetLogger().Debugf("invokeAll request, businessType is CAE, traceID: %s", request.TraceID)
		rd.dispatchAll(request)
		rd.forwardAll(request)
		request.Response <- &types.TCPResponse{}
		return nil
	}

	dispatchErr := errors.New("no error")
	forwardErr := errors.New("no error")
	// if called is cae, we want ignored the rule of slf node prefer
	if (!request.Request.DirectRequest && request.Position.InstanceID != common.ProxyReqPrefix) &&
		(rd.scheduler.GetSchedulerType() == constants.BusinessTypeCAE && len(request.Request.InstanceID) == 0) {
		log.GetLogger().Debugf("this request call cae func so only call forward, functionKey: %s, traceID: %s",
			request.FunctionKey, request.TraceID)
	} else {
		// schedule on local workers or remote workers
		if dispatchErr = rd.dispatch(request); dispatchErr == nil {
			return nil
		}
	}

	// call request from other function, no managed worker, need to forward
	if !request.Request.DirectRequest && request.Position.InstanceID != common.ProxyReqPrefix {
		if forwardErr = rd.forward(request); forwardErr == nil {
			return nil
		}
	}

	log.GetLogger().Warnf("schedule failed, dispatchErr %s, forwardErr %s, functionKey:%s, traceID:%s",
		dispatchErr.Error(), forwardErr.Error(), urnutils.AnonymizeTenantKey(request.FunctionKey), request.TraceID)
	return errScheduleFailed
}

// Request new request coming
func (rd *RequestDispatcher) Request(request *types.DispatcherRequest) error {
	log.GetLogger().Debugf("start to dispatch request funcKeyWithResource: %s, traceID: %s, isInvokeAll: %t",
		urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID, request.Request.IsInvokeAll)
	return rd.requestQueue.Push(request)
}

// PreSchedule mark workerID is scheduled
func (rd *RequestDispatcher) PreSchedule(workerID string) {
	rd.scheduler.PreSchedule(workerID)
}

func (rd *RequestDispatcher) dispatch(request *types.DispatcherRequest) error {
	workerID := rd.scheduler.Schedule(request.Request, request.TraceID)
	if len(request.Request.InstanceID) > 0 && len(workerID) == 0 {
		return fmt.Errorf("schedule current node worker %s failed", request.Request.InstanceID)
	}
	if workerID == circuitBreakerEnabledWorkerID && rd.scheduler.GetSchedulerType() == constants.BusinessTypeCAE {
		log.GetLogger().Warnf("MicroService schedule request failed with circuitBreakerEnabled, "+
			"funcKeyWithResource: %s, traceID: %s", rd.funcKeyWithResource, request.TraceID)
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.MicroserviceCircuitBreakErrCode,
			ErrorMessage: proxyerrors.MicroserviceCircuitBreakErrMsg,
		}
		return nil
	}
	if !autoscaler.EnableMicroserviceAutoScaler && workerID == "" &&
		rd.scheduler.GetSchedulerType() == constants.BusinessTypeCAE && request.Request.DirectRequest {
		// failed fast
		log.GetLogger().Warnf("MicroService schedule request on local workers failed, "+
			"funcKeyWithResource: %s, traceID: %s", rd.funcKeyWithResource, request.TraceID)
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.CAEScheduleRequestErrCode,
			ErrorMessage: proxyerrors.CAEScheduleRequestErrMsg,
		}
		return nil
	}
	if workerID == "" {
		log.GetLogger().Debugf("schedule request on local workers failed, funcKeyWithResource: %s, traceID: %s",
			urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
		return fmt.Errorf("schedule request on local workers failed")
	}
	if request.Request.UsedInstance != nil {
		request.Request.UsedInstance[workerID] = struct{}{}
	}
	request.Request.Concurrency = atomic.LoadUint32(&rd.concurrency)
	log.GetLogger().Infof("scheduleReq | %s | %s | %s", urnutils.ExtractPodName(workerID),
		urnutils.FuncKeyWithResToShort(rd.funcKeyWithResource), request.TraceID)
	// submit task
	workerbroker.GetInstance().Process(workerID, request)
	return nil
}

func (rd *RequestDispatcher) dispatchAll(request *types.DispatcherRequest) {
	localWorkerIDList := rd.scheduler.ScheduleAll()
	if len(localWorkerIDList) == 0 {
		log.GetLogger().Debugf("cae invokeAll request, no local workers, funcKeyWithResource: %s, traceID: %s",
			urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
		return
	}
	request.Request.Concurrency = atomic.LoadUint32(&rd.concurrency)

	log.GetLogger().Infof("dispatchAll, localWorkerIDList: %v, traceID: %s", localWorkerIDList, request.TraceID)
	for _, workerID := range localWorkerIDList {
		copiedRequest := copyRequest(request)
		go func(workerID string, request *types.DispatcherRequest) {
			log.GetLogger().ForDebugf("schedule cae invokeAll request on local workerID: %s, funcKeyWithResource: %s, "+
				"traceID: %s", workerID, urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
			workerbroker.GetInstance().Process(workerID, request)
			resp := <-request.Response
			log.GetLogger().ForDebugf("finish cae invokeAll request on local worker, workerID: %s, resp errorCode: %d, "+
				"funcKeyWithResource: %s, traceID: %s",
				workerID, resp.ErrorCode, urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
		}(workerID, copiedRequest)
	}
}

func (rd *RequestDispatcher) forward(request *types.DispatcherRequest) error {
	if rd.scheduler.GetSchedulerType() == constants.BusinessTypeCAE {
		return rd.forwardCAE(request)
	}
	funcKeyWithResource := utils.GetFuncKeyWithResource(request.FunctionKey,
		types.InstanceResource{CPU: request.Request.CPU, Memory: request.Request.Memory})
	remoteNode := workersview.ChooseRemoteNode(funcKeyWithResource, request.Request.UsedInstance)
	if remoteNode != nil {
		remoteNodeStr, ok := remoteNode.(string)
		if !ok {
			log.GetLogger().Errorf("revert interface to string failed, funcKeyWithResource:%s",
				urnutils.AnonymizeTenantKey(funcKeyWithResource))
			return errScheduleFailed
		}
		if request.Request.UsedInstance != nil {
			request.Request.UsedInstance[remoteNodeStr] = struct{}{}
		}
		log.GetLogger().ForDebugf("forward request to %s, funcKeyWithResource: %s, traceID: %s", remoteNodeStr,
			urnutils.AnonymizeTenantKey(funcKeyWithResource), request.TraceID)
		tenantID, funcName, funcVer := utils.ParseFuncKey(request.FunctionKey)
		dstProperty := &common.FunctionProperty{
			TenantID:        tenantID,
			FunctionName:    funcName,
			FunctionVersion: funcVer,
			InstanceID:      common.ProxyReqPrefix,
		}
		go func() {
			request.Response <- common.SendCallReq(remoteNodeStr, request.TraceID, request.Request, dstProperty)
		}()
		return nil
	}
	return errScheduleFailed
}

func (rd *RequestDispatcher) forwardCAE(request *types.DispatcherRequest) error {
	funcKeyWithResource := utils.GetFuncKeyWithResource(request.FunctionKey,
		types.InstanceResource{CPU: request.Request.CPU, Memory: request.Request.Memory})
	var instance, nodeIP string
	log.GetLogger().Debugf("RequestDispatcher forwardCAE funcKeyWithResource:%s, targetInstanceID:%s, traceID:%s",
		funcKeyWithResource, request.Request.InstanceID, request.TraceID)
	if len(request.Request.InstanceID) == 0 {
		instance, nodeIP = workersview.ChooseRemoteInstanceSkipUsed(funcKeyWithResource, request.Request.UsedInstance)
	} else {
		instance, nodeIP = workersview.ChooseSpecificRemoteInstance(funcKeyWithResource, request.Request.InstanceID)
		if len(request.Request.InstanceID) > 0 && len(instance) == 0 {
			return fmt.Errorf("schedule other node worker %s failed", request.Request.InstanceID)
		}
	}
	if instance != "" {
		defer func() {
			if request.Request.UsedInstance != nil && nodeIP != internal.SelfNodeIP {
				request.Request.UsedInstance[instance] = struct{}{}
			}
		}()
		log.GetLogger().Infof("forward request to %s on %s, funcKeyWithResource: %s, traceID: %s", instance, nodeIP,
			urnutils.AnonymizeTenantKey(funcKeyWithResource), request.TraceID)
		dstProperty := constructForwardDstProperty(request.FunctionKey, instance)

		if nodeIP == internal.SelfNodeIP {
			// if call self node
			return rd.dispatch(request)
		}

		go func() {
			request.Response <- common.SendCallReq(nodeIP, request.TraceID, request.Request, dstProperty)
		}()

		return nil
	}
	if autoscaler.EnableMicroserviceAutoScaler {
		return errScheduleFailed
	}
	request.Response <- &types.TCPResponse{
		ErrorCode:    proxyerrors.CAEScheduleRequestErrCode,
		ErrorMessage: proxyerrors.CAEScheduleRequestErrMsg,
	}
	return nil
}

func (rd *RequestDispatcher) forwardAll(request *types.DispatcherRequest) {
	remoteInstanceMap := workersview.GetAllRemoteInstances(rd.funcKeyWithResource)
	if len(remoteInstanceMap) == 0 {
		log.GetLogger().Debugf("cae invokeAll request, no remote workers, funcKeyWithResource: %s, traceID: %s",
			urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
		return
	}

	log.GetLogger().Infof("forwardAll, remoteInstanceMap: %v, traceID: %s", remoteInstanceMap, request.TraceID)
	for pod, nodeIP := range remoteInstanceMap {
		copiedRequest := copyRequest(request)
		go func(pod, nodeIP string, request *types.DispatcherRequest) {
			log.GetLogger().ForDebugf("forward cae invokeAll request to %s on %s, funcKeyWithResource: %s, traceID: %s",
				pod, nodeIP, urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
			dstProperty := constructForwardDstProperty(request.FunctionKey, pod)
			resp := common.SendCallReq(nodeIP, request.TraceID, request.Request, dstProperty)
			log.GetLogger().ForDebugf("finish cae invokeAll request on remote worker, workerID: %s, nodeIP: %s, "+
				"resp errorCode: %d, funcKeyWithResource: %s, traceID: %s",
				pod, nodeIP, resp.ErrorCode, urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), request.TraceID)
		}(pod, nodeIP, copiedRequest)
	}
}

func copyRequest(request *types.DispatcherRequest) *types.DispatcherRequest {
	return &types.DispatcherRequest{
		Request: &types.TCPRequest{
			LogType:             request.Request.LogType,
			RawData:             request.Request.RawData,
			RequestID:           request.Request.RequestID,
			UserData:            request.Request.UserData,
			Extra:               request.Request.Extra,
			WorkerID:            request.Request.WorkerID,
			Priority:            request.Request.Priority,
			DirectRequest:       request.Request.DirectRequest,
			CanBackpressure:     request.Request.CanBackpressure,
			IsStateFul:          request.Request.IsStateFul,
			CPU:                 request.Request.CPU,
			Memory:              request.Request.Memory,
			Concurrency:         request.Request.Concurrency,
			TraceCtx:            request.Request.TraceCtx,
			RetryFlag:           request.Request.RetryFlag,
			PersistResponse:     request.Request.PersistResponse,
			StateID:             request.Request.StateID,
			WorkflowID:          request.Request.WorkflowID,
			WorkflowStateID:     request.Request.WorkflowStateID,
			WorkflowRunID:       request.Request.WorkflowRunID,
			BackpressureNodeID:  request.Request.BackpressureNodeID,
			BackPressureNums:    request.Request.BackPressureNums,
			BusinessType:        request.Request.BusinessType,
			UsedInstance:        request.Request.UsedInstance,
			IsInitialConcurrent: request.Request.IsInitialConcurrent,
			IsInvokeAll:         request.Request.IsInvokeAll,
			MetricsErrSaved:     request.Request.MetricsErrSaved,
		},
		Response:    make(chan *types.TCPResponse, 1),
		Position:    request.Position,
		FunctionKey: request.FunctionKey,
		TraceID:     request.TraceID,
		NodeLabel:   request.NodeLabel,
		FutureID:    request.FutureID,
		Canceled:    request.Canceled,
		WorkerID:    request.WorkerID,
	}
}

func constructForwardDstProperty(funcKey, instance string) *common.FunctionProperty {
	tenantID, funcName, funcVer := utils.ParseFuncKey(funcKey)
	return &common.FunctionProperty{
		TenantID:        tenantID,
		FunctionName:    funcName,
		FunctionVersion: funcVer,
		InstanceID:      instance,
	}
}
