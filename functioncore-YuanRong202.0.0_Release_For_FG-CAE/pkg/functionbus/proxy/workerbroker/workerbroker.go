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

// Package workerbroker contains communication with workers
package workerbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/invokemetric"
	"functioncore/pkg/functionbus/proxy/caemetrics"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/resourceusage"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

type workerBroker struct {
	dstProperty         *common.FunctionProperty
	bus                 bus.CommunicationBus
	runtimeConn         atomic.Value
	funcKeyWithResource string
	workerID            string
	workerAddr          string
	ownerIP             string
	readyChan           chan interface{}
	waitOnce            sync.Once
	timeout             time.Duration
	initTimeout         time.Duration
	isMinInstance       bool
	isDirectFunction    bool
	hasInitializer      bool
	isInitialized       bool
	runtimeConnUpdate   <-chan tcpbus.RuntimeReport
	stopCh              chan struct{}
	initErrResponse     *types.TCPResponse

	requestCount    int
	paused          bool
	blockForRecover bool
	lastReqFinish   time.Time
	directLock      *sync.RWMutex
	recoverBlock    *sync.Cond
	directTimeout   time.Duration

	reportCache *sync.Map
}

const (
	// defaultRequestTimeout 900s(sync invocation timeout) and 300s(initialization timeout)
	defaultRequestTimeout    = 1200
	defaultInitializeTimeout = 300
	commonInitializeDelay    = 1 * time.Second

	pauseCheckTime          = 500 * time.Millisecond
	workerIdleTime          = 3 * time.Second
	pauseRecoverTimeout     = 3 * time.Second
	recoverRuntimeTimeout   = 5 * time.Second
	waitWorkerReportTimeout = 3 * time.Second
	checkRuntimeTimeout     = 3 * time.Second

	microToMilliSecond = 1000
)

// GetRequestTimeout -
func GetRequestTimeout(funcKeyWithResource string) time.Duration {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	commonDelay := atomic.LoadInt64(&common.FConfig.MaxWorkerDelay)
	if err != nil {
		return time.Duration(defaultRequestTimeout+commonDelay) * time.Second
	}
	funcTimeout := data.FuncMetaData.Timeout
	if funcTimeout > constants.GetSyncRequestTimeout() {
		funcTimeout = constants.GetSyncRequestTimeout()
	}

	return time.Duration(funcTimeout+data.ExtendedMetaData.Initializer.InitializerTimeout+commonDelay) * time.Second
}

// GetInitializerTimeout -
func GetInitializerTimeout(funcKeyWithResource string) time.Duration {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		return time.Duration(defaultInitializeTimeout) * time.Second
	}
	return time.Duration(data.ExtendedMetaData.Initializer.InitializerTimeout) * time.Second
}

// GetDirectTimeout get request timeout for direct function
func GetDirectTimeout(funcKeyWithResource string) time.Duration {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		return time.Duration(constants.GetSyncRequestTimeout()) * time.Second
	}
	funcTimeout := data.FuncMetaData.Timeout
	if funcTimeout > constants.GetSyncRequestTimeout() {
		funcTimeout = constants.GetSyncRequestTimeout()
	}

	return time.Duration(funcTimeout) * time.Second
}

func newWorkerBroker(config *BrokerConfig) *workerBroker {
	tenantID, functionName, functionVersion := utils.ParseFuncKey(config.FuncKeyWithResource)
	dstProperty := &common.FunctionProperty{
		TenantID:        tenantID,
		FunctionName:    functionName,
		FunctionVersion: functionVersion,
		InstanceID:      config.WorkerID,
	}

	lock := new(sync.RWMutex)
	wb := &workerBroker{
		funcKeyWithResource: config.FuncKeyWithResource,
		workerID:            config.WorkerID,
		dstProperty:         dstProperty,
		workerAddr:          config.WorkerAddr,
		ownerIP:             config.RemoteIP,
		readyChan:           make(chan interface{}, 1),
		timeout:             GetRequestTimeout(config.FuncKeyWithResource),
		initTimeout:         GetInitializerTimeout(config.FuncKeyWithResource),
		isMinInstance:       config.IsMinInstance,
		stopCh:              make(chan struct{}),
		isDirectFunction:    config.IsDirectFunc,
		directLock:          lock,
		recoverBlock:        sync.NewCond(lock),
	}
	if config.IsDirectFunc {
		wb.directTimeout = GetDirectTimeout(config.FuncKeyWithResource)
	}
	return wb
}

func linkWithWorker(workerAddr string, workerID string, reportCache *sync.Map) (bus.CommunicationBus, error) {
	tcpBusOpt := &tcpbus.Option{CreateClient: true, ServerAddr: workerAddr, Remote: true, ReportCache: reportCache}
	tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		return nil, err
	}
	tcpBus.SetWorkerID(workerID)
	return tcpBus, nil
}

func (wb *workerBroker) updateRuntimeConn() {
	if !wb.isDirectFunction {
		return
	}
	for {
		select {
		case report, ok := <-wb.runtimeConnUpdate:
			if !ok {
				log.GetLogger().Warnf("update conn of %s closed", wb.workerID)
				return
			}
			log.GetLogger().Infof("runtime conn updated for %s", wb.workerID)
			tcpBus := tcpbus.NewTCPBusAsServer(report.Conn, *wb.dstProperty)
			tcpBus.SetWorkerID(wb.workerID)
			wb.runtimeConn.Store(tcpBus)
			wb.directLock.Lock()
			wb.isInitialized = report.Stage == common.RuntimeStageInitialized
			wb.directLock.Unlock()
			if wb.isMinInstance && wb.hasInitializer && !wb.isInitialized {
				go wb.doInitialize(uuid.New().String())
			}
		case <-wb.stopCh:
			log.GetLogger().Infof("update runtime conn stopped of %s", wb.workerID)
			return
		}
	}
}

func (wb *workerBroker) backgroundPause() {
	if !wb.isDirectFunction || wb.isMinInstance {
		return
	}
	wb.directLock.Lock()
	wb.lastReqFinish = time.Now()
	wb.directLock.Unlock()
	ticker := time.NewTicker(pauseCheckTime)
	defer ticker.Stop()
	reportTicker := time.NewTicker(time.Minute)
	defer reportTicker.Stop()

	for {
		select {
		case <-ticker.C:
			wb.directLock.Lock()
			if wb.paused || wb.requestCount > 0 || time.Since(wb.lastReqFinish) < workerIdleTime ||
				(wb.hasInitializer && !wb.isInitialized) {
				wb.directLock.Unlock()
				continue
			}
			err := wb.pause()
			if err != nil {
				wb.directLock.Unlock()
				log.GetLogger().Warnf("failed to pause pod %s, err: %s", wb.workerID, err.Error())
				continue
			}
			wb.paused = true
			wb.directLock.Unlock()
		case <-wb.stopCh:
			return
		case <-reportTicker.C:
			now := time.Now()
			wb.reportCache.Range(func(key, value any) bool {
				v, ok := value.(common.WorkerReportNotify)
				if !ok {
					return true
				}
				if now.Sub(v.ReportTime) >= time.Minute {
					wb.reportCache.Delete(key)
				}
				return true
			})
		}
	}
}

func (wb *workerBroker) doDirectRequest(request *types.DispatcherRequest, tcpBus *tcpbus.TCPBus) {
	startTime := time.Now()

	if !wb.runtimeConn.Load().(*tcpbus.TCPBus).CheckRuntimeConnect() {
		wb.recoverBlock.L.Lock()
		if wb.blockForRecover {
			wb.recoverBlock.Wait()
		}
		wb.recoverBlock.L.Unlock()
	}

	wb.directLock.RLock()
	shouldCheckInit := wb.hasInitializer && !wb.isInitialized
	initResponse := wb.initErrResponse
	wb.directLock.RUnlock()
	if shouldCheckInit {
		if !wb.isMinInstance {
			initResponse = wb.doInitialize(request.TraceID)
		}
		if checkResponseHasError(initResponse) {
			wb.setResponse(request, initResponse, startTime, time.Since(startTime))
			return
		}
	}
	wb.directLock.Lock()
	wb.requestCount++
	paused := wb.paused
	wb.directLock.Unlock()
	if paused {
		err := wb.recoverRuntime(false)
		if err != nil {
			costTime := time.Since(startTime)
			log.GetLogger().Errorf("failed to recover pod %s, traceID %s, err: %s", wb.workerID,
				request.TraceID, err.Error())
			wb.errResponse(request, costTime)
			return
		}
		wb.directLock.Lock()
		wb.paused = false
		wb.directLock.Unlock()
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), wb.directTimeout+time.Second)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, request.TraceID)
	log.GetLogger().Debugf("send request to worker, function:%s, traceID:%s, instanceID:%s",
		request.FunctionKey, request.TraceID, tcpBus.Property.InstanceID)
	workerevaluator.GetInstance().RecordRequestStart(wb.workerID)
	var dst = *wb.dstProperty
	dst.FutureID = request.FutureID

	response, err := wb.runtimeConn.Load().(*tcpbus.TCPBus).Call(ctx, &dst, request.Request, false)
	costTime := time.Since(startTime)
	wb.directLock.Lock()
	wb.requestCount--
	wb.lastReqFinish = time.Now()
	wb.directLock.Unlock()
	if err != nil {
		wb.errResponse(request, costTime)
		return
	}
	if response.ErrorCode == proxyerrors.ClientExitErrCode || response.ErrorCode == proxyerrors.SendReqErrCode ||
		response.ErrorCode == proxyerrors.TimeoutErrCode {
		report, checkErr := wb.checkRuntime(request.TraceID, response.ErrorCode != proxyerrors.TimeoutErrCode)
		if response.ErrorCode == proxyerrors.ClientExitErrCode || response.ErrorCode == proxyerrors.SendReqErrCode {
			go wb.blockAndRecoverRuntime()
		}
		if checkErr != nil {
			wb.errResponse(request, costTime)
			return
		}
		if response.ErrorCode == proxyerrors.TimeoutErrCode {
			response.ErrorCode = snerror.RuntimeInvokeTimeoutCode
			response.ErrorMessage = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), wb.directTimeout)
			// remove request delay to be accurate in summary
			costTime -= time.Second
		} else if report.ErrorCode != 0 {
			response.ErrorCode = report.ErrorCode
			response.ErrorMessage = report.ErrorMessage
		}
		response.Logs = report.Logs
		response.Summary = amendRuntimeSummary(report.Summary, costTime)
		wb.setResponse(request, response, startTime, costTime)
		return
	}

	if request.Request.LogType == constants.RuntimeLogOptTail {
		reportIf, _ := wb.reportCache.LoadOrStore(request.TraceID, common.WorkerReportNotify{
			ReportTime: time.Now(),
			Ch:         make(chan common.WorkerReport, 1),
		})
		defer wb.reportCache.Delete(request.TraceID)

		if report, ok := reportIf.(common.WorkerReportNotify); ok {
			select {
			case r := <-report.Ch:
				response.Logs = r.Logs
			case <-time.After(waitWorkerReportTimeout):
				log.GetLogger().Warnf("failed to wait worker report of %s %s", wb.workerID, request.TraceID)
			}
		}
	}

	wb.setResponse(request, response, startTime, costTime)
}

func (wb *workerBroker) cancel(dst *common.CancelProperty, requestID uint64, traceID string) *types.TCPResponse {
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)
	res, err := tcpBus.Cancel(ctx, dst, &types.TCPRequest{RequestID: requestID})
	if err != nil {
		log.GetLogger().Errorf("workerBroker cancel failed, err: %s", err.Error())
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	return res
}

func (wb *workerBroker) recover(requestID uint64, traceID string, flag bool) *types.TCPResponse {
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	ctx, cancel := utils.ContextWithTimeout(context.Background(),
		common.DefaultRecoverTimeout, constants.HeaderTraceID, traceID)
	defer cancel()
	res, err := tcpBus.Recover(ctx, &types.TCPRequest{RequestID: requestID, WorkerID: wb.workerID, IsStateFul: flag})
	if err != nil {
		log.GetLogger().Errorf("workerBroker recover failed, err: %s", err.Error())
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	if !tcpBus.CheckWorkerID(res.RawData) {
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	return res
}

func (wb *workerBroker) blockAndRecoverRuntime() {
	wb.recoverBlock.L.Lock()
	if !wb.blockForRecover {
		wb.blockForRecover = true
	} else {
		wb.recoverBlock.L.Unlock()
		return
	}
	wb.recoverBlock.L.Unlock()
	wb.recoverRuntime(true)
}

func (wb *workerBroker) recoverRuntime(restartRuntime bool) error {
	log.GetLogger().Infof("start to recover runtime for pod %s", wb.workerID)
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return errors.New("failed to assert tcp bus type")
	}
	ctx, cancel := context.WithTimeout(context.Background(), recoverRuntimeTimeout)
	defer func() {
		cancel()
		wb.recoverBlock.L.Lock()
		wb.blockForRecover = false
		wb.recoverBlock.L.Unlock()
		wb.recoverBlock.Broadcast()
	}()
	rsp, err := tcpBus.RecoverRuntime(ctx, restartRuntime)
	if err != nil {
		log.GetLogger().Errorf("failed to recover runtime for pod %s, err: %s", wb.workerID, err.Error())
		return err
	}
	if rsp.ErrorCode != proxyerrors.StateOKCode {
		log.GetLogger().Errorf("recover runtime for pod %s failed, err: %s", wb.workerID, rsp.ErrorMessage)
		return errors.New(rsp.ErrorMessage)
	}
	log.GetLogger().Infof("succeed to recover runtime for pod %s", wb.workerID)
	return nil
}

func (wb *workerBroker) checkRuntime(traceID string, shouldWait bool) (*common.WorkerReport, error) {
	log.GetLogger().Infof("start to check runtime for worker %s traceID %s", wb.workerID, traceID)
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return nil, errors.New("failed to assert tcp bus type")
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), checkRuntimeTimeout)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, traceID)
	rsp, err := tcpBus.CheckRuntime(ctx, traceID, shouldWait)
	if err != nil {
		log.GetLogger().Errorf("failed to check runtime for worker %s, err: %s", wb.workerID, err.Error())
		return nil, err
	}
	return &common.WorkerReport{
		ErrorCode:    rsp.ErrorCode,
		ErrorMessage: rsp.ErrorMessage,
		Logs:         rsp.Logs,
		Summary:      rsp.Summary,
	}, nil
}

func (wb *workerBroker) pause() error {
	log.GetLogger().Infof("start to pause pod %s", wb.workerID)
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return errors.New("failed to assert tcp bus type")
	}
	ctx, cancel := context.WithTimeout(context.Background(), pauseRecoverTimeout)
	defer cancel()
	_, err := tcpBus.Pause(ctx)
	if err != nil {
		log.GetLogger().Errorf("failed to pause pod %s, err: %s", wb.workerID, err.Error())
		return err
	}
	return nil
}

func (wb *workerBroker) process(request *types.DispatcherRequest) {
	if request == nil {
		log.GetLogger().Errorf("args error, request is nil")
		return
	}
	pool := GetInstance().brokerTaskPool

	// for first call, wait until bus with worker is ready
	wb.waitOnce.Do(func() { <-wb.readyChan })
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return
	}
	log.GetLogger().Debugf("send request to task pool, function: %s, traceID: %s, instanceID: %s",
		request.FunctionKey, request.TraceID, tcpBus.Property.InstanceID)
	flag := false
	if request.Request != nil {
		flag = request.Request.IsSessionCreateReq
	}
	if request.Canceled {
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.RequestCanceledCode,
			ErrorMessage: proxyerrors.RequestCanceledError,
		}
		cancel.GetInstance().DeleteRequest(request.FutureID)
		workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, 0, flag)
		return
	}
	cancel.GetInstance().UpdateRequest(request.FutureID, wb.workerID)
	pool.Submit(func() {
		if !wb.isDirectFunction {
			wb.doRequest(request, tcpBus)
		} else {
			wb.doDirectRequest(request, tcpBus)
		}
	})
}

func (wb *workerBroker) errResponse(request *types.DispatcherRequest, costTime time.Duration) {
	errResponse := &types.TCPResponse{
		ErrorCode:    proxyerrors.InternalErrCode,
		ErrorMessage: proxyerrors.InternalErr,
	}
	log.GetLogger().Errorf("failed to send request to worker, function: %s, traceID: %s, instanceID: %s",
		request.FunctionKey, request.TraceID, wb.workerID)
	request.Response <- errResponse
	if !request.Request.DirectRequest {
		invokemetric.SaveMetric(request.FunctionKey, costTime.Milliseconds(), proxyerrors.InternalErrCode)
	}
	workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, costTime, request.Request.IsSessionCreateReq)
	return
}

func (wb *workerBroker) setResponse(request *types.DispatcherRequest, response *types.TCPResponse,
	startTime time.Time, costTime time.Duration) {
	if response.ErrorCode == proxyerrors.StateAcceptedCode {
		log.GetLogger().Infof("receive request-accepted response, traceID: %s", request.TraceID)
		cancel.GetInstance().RecordAsyncRequests(wb.workerID, request.TraceID)
	} else {
		workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, costTime, request.Request.IsSessionCreateReq)
	}

	// worker is exiting, don't schedule on this worker anymore
	if !wb.isDirectFunction && (response.ErrorCode == proxyerrors.ClientExitErrCode ||
		response.ErrorCode == proxyerrors.WorkerExitErrCode) {
		funcKeyWithResource := utils.GetFuncKeyWithResource(request.FunctionKey,
			types.InstanceResource{CPU: request.Request.CPU, Memory: request.Request.Memory})
		workerevaluator.GetInstance().DeleteWorkerMetrics(wb.workerID, funcKeyWithResource)
	}

	log.GetLogger().Debugf("send request to worker successfully, function:%s, traceID:%s, instanceID:%s, cost:%v",
		request.FunctionKey, request.TraceID, wb.workerID, costTime)
	response.CallNode = internal.SelfNodeIP
	response.CallInstanceID = wb.workerID
	response.ActualCallStartTime = startTime
	response.ActualCallCost = costTime
	request.Response <- response
	if !request.Request.DirectRequest {
		invokemetric.SaveMetric(request.FunctionKey, costTime.Milliseconds(), response.ErrorCode)
	}
}

func (wb *workerBroker) doRequest(request *types.DispatcherRequest, tcpBus *tcpbus.TCPBus) {
	startTime := time.Now()
	we := workerevaluator.GetInstance()
	ctx, cancelFunc := context.WithTimeout(context.Background(), wb.timeout)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, request.TraceID)
	log.GetLogger().Debugf("send request to worker, function:%s, traceID:%s, instanceID:%s",
		request.FunctionKey, request.TraceID, tcpBus.Property.InstanceID)
	we.RecordRequestStart(wb.workerID)
	var dst = *wb.dstProperty
	dst.FutureID = request.FutureID

	response, err := wb.bus.Call(ctx, &dst, request.Request, false)
	costTime := time.Since(startTime)
	if err != nil {
		wb.errResponse(request, costTime)
		return
	}

	wb.setResponse(request, response, startTime, costTime)
}

func (wb *workerBroker) doInitialize(traceID string) *types.TCPResponse {
	wb.recoverBlock.L.Lock()
	if wb.blockForRecover {
		wb.recoverBlock.Wait()
	}
	wb.recoverBlock.L.Unlock()
	log.GetLogger().Infof("start to initialize worker %s traceID %s", wb.workerID, traceID)
	runtimeConn, ok := wb.runtimeConn.Load().(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to do initialize, no runtime connection available")
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: fmt.Sprint("runtime connection not found"),
		}
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), wb.initTimeout+commonInitializeDelay)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, traceID)
	initError := snerror.New(proxyerrors.StateOKCode, "init success")
	wb.directLock.Lock()
	if wb.isInitialized {
		wb.directLock.Unlock()
		return &types.TCPResponse{
			ErrorCode:    uint32(initError.Code()),
			ErrorMessage: initError.Error(),
		}
	}
	startTime := time.Now()
	response, err := runtimeConn.Init(ctx)
	costTime := time.Since(startTime)
	if err != nil {
		response = &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: err.Error(),
		}
	} else if response.ErrorCode == proxyerrors.TimeoutErrCode {
		response.ErrorCode = snerror.RuntimeInitTimeoutCode
		response.ErrorMessage = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), wb.initTimeout)
		report, err := wb.checkRuntime(traceID, false)
		if err != nil {
			response.ErrorCode = proxyerrors.InternalErrCode
			response.ErrorMessage = err.Error()
		}
		if report.ErrorCode != proxyerrors.StateOKCode {
			response.ErrorCode = report.ErrorCode
			response.ErrorMessage = report.ErrorMessage
		}
		response.Logs = report.Logs
		// remove request delay to be accurate in summary
		response.Summary = amendRuntimeSummary(report.Summary, costTime-commonInitializeDelay)
	}
	if !checkResponseHasError(response) {
		wb.isInitialized = true
	} else if response.ErrorCode != proxyerrors.InternalErrCode {
		wb.initErrResponse = response
		wb.blockForRecover = true
		go wb.recoverRuntime(true)
	}
	wb.directLock.Unlock()
	return response
}

func (wb *workerBroker) handleCancel(ctx context.Context, dst *common.CancelProperty, req *types.TCPRequest) error {
	traceID := utils.ContextValueString(ctx, constants.HeaderTraceID)
	reqID := req.RequestID
	resp := cancel.GetInstance().HandleCancel(ctx, traceID, dst)
	resp.RequestID = reqID
	log.GetLogger().Debugf("cancel got errCode %d traceID %s", resp.ErrorCode, traceID)
	wb.bus.SetCancelResult(resp)
	return nil
}

func (wb *workerBroker) handleRequest(ctx context.Context, dst *common.FunctionProperty,
	req *types.TCPRequest) error {
	traceID := utils.ContextValueString(ctx, constants.HeaderTraceID)
	reqID := req.RequestID
	req.StateID = dst.StateID
	resp := requestHandler(traceID, req, dst)
	resp.RequestID = reqID
	log.GetLogger().Debugf("request got errCode %d traceID %s requestID: %d", resp.ErrorCode,
		traceID, resp.RequestID)
	wb.bus.SetResult(resp)
	return nil
}

func (wb *workerBroker) reportRuntimeMetrics(runtimeMetrics common.RuntimeMetricsProperty) error {
	log.GetLogger().Infof("current MetricsName: %s, funcURN: %s",
		runtimeMetrics.MetricsName, runtimeMetrics.FunctionURN)
	if runtimeMetrics.MetricsName == constants.MetricsUsedResourceTimeWindow ||
		runtimeMetrics.MetricsName == constants.ReservedInstanceMetricsTimeWindow {
		if err := workerevaluator.Client.ReportUsedResourceTimeWindows(runtimeMetrics,
			wb.funcKeyWithResource); err != nil {
			log.GetLogger().Errorf("failed to report usedResourceTimeWindows, "+
				"workerID: %s, functionKey %s, %s", wb.workerID,
				urnutils.AnonymizeTenantKey(wb.funcKeyWithResource), err.Error())
			return err
		}
	}
	return nil
}

func (wb *workerBroker) storeResourceUsage(resourceUsage common.ResourceUsage) {
	resourceusage.GetNodeResourceUsageInstance().Store(resourceUsage)
}

func (wb *workerBroker) storeCaeMetrics(metrics common.CaeMetrics) {
	caemetrics.GetCaeMetricsHandler().Store(metrics)
}

func amendRuntimeSummary(summaryData string, duration time.Duration) string {
	summary := &snerror.SummaryInfo{}
	err := json.Unmarshal([]byte(summaryData), summary)
	if err != nil {
		log.GetLogger().Errorf("failed to amend runtime summary %s error %s", summaryData, err.Error())
		return summaryData
	}
	summary.Duration = float64(duration.Microseconds() / microToMilliSecond)
	summary.BillingDuration = int(duration.Microseconds() / microToMilliSecond)
	return summary.MarshalJSONString()
}

func checkResponseHasError(response *types.TCPResponse) bool {
	if response == nil {
		return false
	}
	return response.ErrorCode != proxyerrors.StateOKCode
}
