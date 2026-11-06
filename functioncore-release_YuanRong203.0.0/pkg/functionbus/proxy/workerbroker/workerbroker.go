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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/invokemetric"
	"functioncore/pkg/functionbus/proxy/bridgemetrics"
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
	isMinInstance       bool
	isDirectFunction    bool
	hasInitializer      bool
	isInitialized       bool
	runtimeConnUpdate   <-chan tcpbus.RuntimeReport
	stopCh              chan struct{}
	initErrResponse     *types.TCPResponse

	requestCount      int
	paused            bool
	blockForRecover   bool
	lastReqFinish     time.Time
	directLock        *sync.RWMutex
	reInitLock        *sync.Mutex
	recoverBlock      *sync.Cond
	longRunFunc       bool
	directInitTimeout time.Duration
	invokeTimeout     time.Duration

	asyncDirectRequestMap *sync.Map
	reportCache           *sync.Map

	summaryRecorder *SummaryRecorder
	isConcurrent    bool

	verticalAutoscalingInfo atomic.Value
	recoverWaitNewConnCh    chan struct{}

	isCPUOverloaded                 int32
	startTime                       time.Time
	cpuUsageCircuitBreakerThreshold float64
	circuitBreakerInitialPeriod     int64
}

type verticalAutoscalingInfo struct {
	businessType       string
	workerContainerID  string
	runtimeContainerID string
	podUID             string
	// runtime resource config
	runtimeCPULimit      int64
	runtimeCPURequest    int64
	runtimeMemoryLimit   int64
	runtimeMemoryRequest int64
	// worker resource config
	workerCPULimit      int64
	workerCPURequest    int64
	workerMemoryLimit   int64
	workerMemoryRequest int64
	// runtime pool resource config
	runtimePoolCPULimit      int64
	runtimePoolCPURequest    int64
	runtimePoolMemoryLimit   int64
	runtimePoolMemoryRequest int64
	// worker pool resource config
	workerPoolCPULimit      int64
	workerPoolCPURequest    int64
	workerPoolMemoryLimit   int64
	workerPoolMemoryRequest int64
}

var (
	initRedisOnce               sync.Once
	clusterID                   = os.Getenv("CLUSTER_ID")
	circuitBreakerThreshold     = os.Getenv("CIRCUIT_BREAKER_THRESHOLD")
	circuitBreakerInitialPeriod = os.Getenv("CIRCUIT_BREAKER_INITIAL_PERIOD")
	curCPUUsageThreshold        float64
	initialPeriod               int64
)

const (
	// defaultRequestTimeout 900s(sync invocation timeout) and 300s(initialization timeout)
	defaultRequestTimeout    = 1200
	defaultInitializeTimeout = 300
	commonInitializeDelay    = 1 * time.Second

	pauseCheckTime               = 500 * time.Millisecond
	workerIdleTime               = 3 * time.Second
	pauseRecoverTimeout          = 3 * time.Second
	recoverRuntimeTimeout        = 5 * time.Second
	waitWorkerReportTimeout      = 3 * time.Second
	checkRuntimeTimeout          = 3 * time.Second
	waitNewConnReadyTimeout      = 10 * time.Second
	redisRetryTimes              = 3
	listLimit                    = 3
	microToMilliSecond           = 1000
	metricsKeyTTL                = 60 * time.Minute
	redisRetryInterval           = 100 * time.Millisecond
	bridgeMetricsKey             = "/sn/metrics/bridgeFuncMetrics"
	multipleDefaultConcurrentNum = 10
	mbToBytes                    = 1 << 20

	defaultCPUUsage          = 0.8
	defaultInstanceStartTime = 10
	base                     = 10
	bitSize                  = 64
	minToSecond              = 60

	// example: CPUUsage: 2.0782493007556675 CPULimit:2200, make data consistent to calculate CPU utilization.
	usedLimitRate = 1000
)

type asyncRequest struct {
	traceID     string
	instanceID  string
	functionKey string
	deadline    time.Time
	startTime   time.Time
	headers     map[string]string
}

type asyncResponse struct {
	statusCode int
	instanceID string
	body       []byte
	header     map[string]string
}

func init() {
	curCPUUsageThreshold, initialPeriod = getCPUUsageCircuitInfo()
	log.GetLogger().Infof("current curCPUUsageThreshold %f, initialPeriod %d", curCPUUsageThreshold, initialPeriod)
}

// GetRequestTimeout -
func GetRequestTimeout(funcKeyWithResource string) time.Duration {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetDirectFunctionMetadata(funcKey)
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

// GetConcurrentNum -
func GetConcurrentNum(funcKeyWithResource string) int {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata for %s, err: %s", funcKey, err.Error())
		return 0
	}
	return data.InstanceMetaData.ConcurrentNum
}

// GetCircuitBreakerEnabled -
func GetCircuitBreakerEnabled(funcKeyWithResource string) bool {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata for %s, err: %s", funcKey, err.Error())
		return false
	}
	return data.FuncMetaData.MicCircuitBreakerEnabled
}

// GetSummaryFunctionData is used to obtain runtime gpu memory and urn for billing.
func GetSummaryFunctionData(funcKeyWithResource string) (int64, bool, string, error) {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata for %s, err: %s", funcKey, err.Error())
		return 0, false, "", err
	}
	return data.ResourceMetaData.GpuMemory, data.ResourceMetaData.EnableTmpExpansion,
		data.FuncMetaData.FunctionVersionURN, err
}

// GetDirectTimeout get request timeout for direct function
func GetDirectTimeout(funcKeyWithResource string) (time.Duration, time.Duration, bool) {
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	data, err := servicediscovery.GetRegistry().GetDirectFunctionMetadata(funcKey)
	syncRequestTimeout := constants.GetSyncRequestTimeout()
	if err != nil {
		return time.Duration(syncRequestTimeout) * time.Second, defaultInitializeTimeout * time.Second, true
	}
	var flag bool
	invokeTimeout := data.FuncMetaData.Timeout
	if invokeTimeout > syncRequestTimeout {
		flag = true
	}
	initTimeout := data.ExtendedMetaData.Initializer.InitializerTimeout
	if initTimeout == 0 {
		initTimeout = defaultInitializeTimeout
	}
	return time.Duration(invokeTimeout) * time.Second, time.Duration(initTimeout) * time.Second, flag
}

func getCPUUsageCircuitInfo() (float64, int64) {
	var curCPUUsageThreshold float64 = defaultCPUUsage
	var initialPeriod int64 = defaultInstanceStartTime

	cpuUsage, ok := isStringValidFloatInRange(circuitBreakerThreshold)
	if ok {
		curCPUUsageThreshold = cpuUsage
	}

	res, err := strconv.ParseInt(circuitBreakerInitialPeriod, base, bitSize)
	if err == nil && res > 0 {
		initialPeriod = res
	}
	return curCPUUsageThreshold, initialPeriod
}

func isStringValidFloatInRange(s string) (float64, bool) {
	data, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}

	if data > 0 && data <= 1 {
		return data, true
	}
	return data, false
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
		funcKeyWithResource:             config.FuncKeyWithResource,
		workerID:                        config.WorkerID,
		dstProperty:                     dstProperty,
		workerAddr:                      config.WorkerAddr,
		ownerIP:                         config.RemoteIP,
		readyChan:                       make(chan interface{}, 1),
		timeout:                         GetRequestTimeout(config.FuncKeyWithResource),
		isMinInstance:                   config.IsMinInstance,
		stopCh:                          make(chan struct{}),
		isDirectFunction:                config.IsDirectFunc,
		directLock:                      lock,
		recoverBlock:                    sync.NewCond(lock),
		reInitLock:                      new(sync.Mutex),
		startTime:                       time.Now(),
		cpuUsageCircuitBreakerThreshold: curCPUUsageThreshold,
		circuitBreakerInitialPeriod:     initialPeriod,
	}
	if config.IsDirectFunc {
		wb.invokeTimeout, wb.directInitTimeout, wb.longRunFunc = GetDirectTimeout(config.FuncKeyWithResource)
	}
	if config.Resource.Worker.CPULimit != 0 {
		storeVerticalAutoscalingInfo(config, wb)
	}
	return wb
}

func storeVerticalAutoscalingInfo(config *BrokerConfig, wb *workerBroker) {
	wb.verticalAutoscalingInfo.Store(&verticalAutoscalingInfo{
		businessType:             config.BusinessType,
		workerContainerID:        config.ContainerIDs.Worker,
		runtimeContainerID:       config.ContainerIDs.Runtime,
		podUID:                   config.PodUID,
		runtimeCPULimit:          config.Resource.Runtime.CPULimit,
		runtimeCPURequest:        config.Resource.Runtime.CPURequest,
		runtimeMemoryLimit:       config.Resource.Runtime.MemoryLimit * mbToBytes,
		runtimeMemoryRequest:     config.Resource.Runtime.MemoryRequest * mbToBytes,
		workerCPURequest:         config.Resource.Worker.CPURequest,
		workerCPULimit:           config.Resource.Worker.CPULimit,
		workerMemoryRequest:      config.Resource.Worker.MemoryRequest * mbToBytes,
		workerMemoryLimit:        config.Resource.Worker.MemoryLimit * mbToBytes,
		runtimePoolCPULimit:      config.ViewResource.Runtime.CPULimit,
		runtimePoolCPURequest:    config.ViewResource.Runtime.CPURequest,
		runtimePoolMemoryLimit:   config.ViewResource.Runtime.MemoryLimit,
		runtimePoolMemoryRequest: config.ViewResource.Runtime.MemoryRequest,
		workerPoolCPURequest:     config.ViewResource.Worker.CPURequest,
		workerPoolCPULimit:       config.ViewResource.Worker.CPULimit,
		workerPoolMemoryRequest:  config.ViewResource.Worker.MemoryRequest,
		workerPoolMemoryLimit:    config.ViewResource.Worker.MemoryLimit,
	})
}

func linkWithWorker(workerAddr string, workerID string,
	isDirect bool, reportCache *sync.Map) (bus.CommunicationBus, error) {
	tcpBusOpt := &tcpbus.Option{CreateClient: true, ServerAddr: workerAddr, Remote: true, ReportCache: reportCache}
	tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		return nil, err
	}
	tcpBus.SetWorkerID(workerID)
	if isDirect {
		tcpBus.SetResetFlag(true)
	}
	return tcpBus, nil
}

func (wb *workerBroker) getCPUOverloaded() bool {
	return atomic.LoadInt32(&wb.isCPUOverloaded) == 1
}

func (wb *workerBroker) getVerticalAutoscalingInfo() *verticalAutoscalingInfo {
	infoIf := wb.verticalAutoscalingInfo.Load()
	if infoIf == nil {
		return nil
	}
	info, ok := infoIf.(*verticalAutoscalingInfo)
	if !ok {
		return nil
	}
	return info
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
			if wb.summaryRecorder != nil {
				report.Conn.RegisterBillingCallBack(wb.summaryRecorder.StartBilling, wb.summaryRecorder.StopBilling)
			}
			report.Conn.RegisterDirectAsyncRequestHandler(wb.ProcessAsyncResult)
			tcpBus := tcpbus.NewTCPBusAsServer(report.Conn, *wb.dstProperty)
			tcpBus.SetWorkerID(wb.workerID)
			wb.runtimeConn.Store(tcpBus)
			if wb.recoverWaitNewConnCh != nil {
				close(wb.recoverWaitNewConnCh)
			}
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
			if err == nil {
				wb.paused = true
				wb.directLock.Unlock()
				continue
			}
			// in some edge case, the worker paused runtime successfully, but bus not received the response,
			// thus bus won't send recover and all user requests will timeout.
			// so here we assume the pause is success in such case.
			if strings.Contains(err.Error(), proxyerrors.TimeoutErr) {
				wb.paused = true
			}
			wb.directLock.Unlock()
			log.GetLogger().Warnf("failed to pause pod %s, err: %s", wb.workerID, err.Error())
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

func (wb *workerBroker) watchSyncRequestTimeout() {
	if !wb.isDirectFunction || !wb.longRunFunc || wb.asyncDirectRequestMap == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			wb.asyncDirectRequestMap.Range(func(key, value any) bool {
				traceID, ok := key.(string)
				if !ok {
					return true
				}
				request, ok := value.(asyncRequest)
				if !ok {
					return true
				}
				if timeout := request.deadline.Sub(time.Now()); timeout <= 0 {
					msg := &protocol.Message{}
					msg.Header.ErrCode = proxyerrors.TimeoutErrCode
					wb.ProcessAsyncResult(msg, traceID, false)
				}
				return true
			})
		case <-wb.stopCh:
			return
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
	wb.AddRequestCount(1)
	if paused {
		err := wb.recoverRuntime(false)
		if err != nil {
			costTime := time.Since(startTime)
			log.GetLogger().Errorf("failed to recover pod %s, traceID %s, err: %s", wb.workerID,
				request.TraceID, err.Error())
			wb.errResponse(request, costTime)
			wb.AddRequestCount(-1)
			return
		}
		wb.directLock.Lock()
		wb.paused = false
		wb.directLock.Unlock()
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), wb.invokeTimeout)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, request.TraceID)
	if wb.longRunFunc {
		ctx = utils.ContextWithValueString(ctx, constants.HeaderAsync, "true")
		wb.storeAsyncDirectRequestMap(request)
	}
	log.GetLogger().Debugf("send request to worker, function:%s, traceID:%s, instanceID:%s",
		request.FunctionKey, request.TraceID, tcpBus.Property.InstanceID)
	workerevaluator.GetInstance().RecordRequestStart(wb.workerID)
	var dst = *wb.dstProperty
	dst.FutureID = request.FutureID
	response, err := wb.runtimeConn.Load().(*tcpbus.TCPBus).Call(ctx, &dst, request.Request, false)
	costTime := time.Since(startTime)
	if response == nil || response.ErrorCode != snerror.RequestAcceptedCode {
		wb.asyncDirectRequestMap.Delete(request.TraceID)
		wb.directLock.Lock()
		wb.requestCount--
		wb.lastReqFinish = time.Now()
		wb.directLock.Unlock()
		defer wb.AddRequestCount(-1)
	}
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
			response.ErrorMessage = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), wb.invokeTimeout)
		} else if report != nil && report.ErrorCode != proxyerrors.StateOKCode {
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

func (wb *workerBroker) storeAsyncDirectRequestMap(request *types.DispatcherRequest) {
	asyRequest := asyncRequest{}
	asyRequest.startTime = time.Now()
	asyRequest.traceID = request.TraceID
	asyRequest.instanceID = wb.workerID
	asyRequest.deadline = asyRequest.startTime.Add(wb.invokeTimeout)
	asyRequest.headers = request.Request.Extra
	asyRequest.functionKey = request.FunctionKey
	wb.asyncDirectRequestMap.Store(request.TraceID, asyRequest)
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
	res, err := tcpBus.Recover(ctx, &types.TCPRequest{RequestID: requestID, WorkerID: wb.workerID, IsStateFul: flag,
		IsInitialConcurrent: wb.isConcurrent})
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
	if restartRuntime {
		wb.recoverWaitNewConnCh = make(chan struct{})
		go wb.waitRecoverNewConnReady()
	}
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if !ok {
		log.GetLogger().Errorf("failed to assert tcp bus type")
		return errors.New("failed to assert tcp bus type")
	}
	ctx, cancel := context.WithTimeout(context.Background(), recoverRuntimeTimeout)
	defer func() {
		cancel()
		if !restartRuntime {
			wb.recoverBlockBroadcast()
		}
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
	log.GetLogger().ForDebugf("send request to task pool, function: %s, traceID: %s, instanceID: %s",
		request.FunctionKey, request.TraceID, tcpBus.Property.InstanceID)
	if request.Canceled {
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.RequestCanceledCode,
			ErrorMessage: proxyerrors.RequestCanceledError,
		}
		cancel.GetInstance().DeleteRequest(request.FutureID)
		workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, 0)
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
		ErrorCode:       proxyerrors.InternalErrCode,
		ErrorMessage:    proxyerrors.InternalErr,
		MetricsErrSaved: true,
	}
	log.GetLogger().Errorf("failed to send request to worker, function: %s, traceID: %s, instanceID: %s",
		request.FunctionKey, request.TraceID, wb.workerID)
	request.Response <- errResponse
	if !request.Request.DirectRequest && !request.Request.MetricsErrSaved {
		invokemetric.SaveMetric(request.FunctionKey, costTime.Milliseconds(), errResponse)
	}
	workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, costTime)
	return
}

func (wb *workerBroker) setResponse(request *types.DispatcherRequest, response *types.TCPResponse,
	startTime time.Time, costTime time.Duration) {
	if response.ErrorCode == proxyerrors.StateAcceptedCode {
		log.GetLogger().Infof("receive request-accepted response, traceID: %s", request.TraceID)
		cancel.GetInstance().RecordAsyncRequests(wb.workerID, request.TraceID)
	} else {
		workerevaluator.GetInstance().RecordRequestEnd(wb.workerID, costTime)
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
	if response.ErrorCode != 0 && !snerror.IsUserErr(response.ErrorCode) {
		response.MetricsErrSaved = true
	}
	request.Response <- response
	if !request.Request.DirectRequest {
		if response.ErrorCode != 0 && request.Request.MetricsErrSaved {
			return
		}
		invokemetric.SaveMetric(request.FunctionKey, costTime.Milliseconds(), response)
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
	wb.reInitLock.Lock()
	defer wb.reInitLock.Unlock()
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), wb.directInitTimeout)
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
	concurrentNum := GetConcurrentNum(wb.funcKeyWithResource)
	startTime := time.Now()
	response, err := runtimeConn.Init(ctx, concurrentNum)
	costTime := time.Since(startTime)
	if err != nil {
		response = &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: err.Error(),
		}
	} else if response.ErrorCode == proxyerrors.ClientExitErrCode || response.ErrorCode == proxyerrors.SendReqErrCode ||
		response.ErrorCode == proxyerrors.TimeoutErrCode {
		report, checkErr := wb.checkRuntime(traceID, response.ErrorCode != proxyerrors.TimeoutErrCode)
		if checkErr != nil {
			response.ErrorCode = proxyerrors.InternalErrCode
			response.ErrorMessage = checkErr.Error()
		}
		if response.ErrorCode == proxyerrors.TimeoutErrCode {
			response.ErrorCode = snerror.RuntimeInitTimeoutCode
			response.ErrorMessage = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), wb.directInitTimeout)
		} else if report != nil && report.ErrorCode != proxyerrors.StateOKCode {
			response.ErrorCode = report.ErrorCode
			response.ErrorMessage = report.ErrorMessage
		}
		response.Logs = report.Logs
		// remove request delay to be accurate in summary
		response.Summary = amendRuntimeSummary(report.Summary, costTime)
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
	workerID := workerIDPrefix + resourceUsage.PodName
	if wb.workerID == workerID {
		if resourceUsage.CPUUsage*usedLimitRate/float64(resourceUsage.CPULimit) > wb.cpuUsageCircuitBreakerThreshold {
			if time.Since(wb.startTime) > time.Duration(wb.circuitBreakerInitialPeriod)*time.Minute ||
				time.Now().Unix()-resourceUsage.StartTime > wb.circuitBreakerInitialPeriod*minToSecond {
				atomic.StoreInt32(&wb.isCPUOverloaded, 1)
			}
		} else {
			atomic.StoreInt32(&wb.isCPUOverloaded, 0)
		}
	}
}

func (wb *workerBroker) storeCaeMetrics(metrics common.CaeMetrics) {
	caemetrics.GetCaeMetricsHandler().Store(metrics)
}

func (wb *workerBroker) storeBridgeMetrics(metrics common.BridgeFuncMetrics) {
	if metrics.ForBridgeManager {
		bridgemetrics.GetBridgeMetricsHandler().Store(metrics)
	} else {
		wb.reportBridgeMetricsForMonitor(metrics)
	}
}

func (wb *workerBroker) reportBridgeMetricsForMonitor(metrics common.BridgeFuncMetrics) {
	initRedisOnce.Do(func() {
		if redisclient.GetRedisCmd() == nil {
			client := workerevaluator.Client.GetRedisClient()
			redisclient.SetRedisCmd(client)
		}
	})
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Warnf("failed to report bridgeFunc metrics, redis client is nil")
		return
	}
	metrics.ClusterID = clusterID
	metricsValue, err := json.Marshal(metrics)
	if err != nil {
		log.GetLogger().Warnf("failed to report bridgeFunc metrics, error:%s", err.Error())
		return
	}
	for i := 0; i < redisRetryTimes; i++ {
		err = redisclient.ZAddToRedis(bridgeMetricsKey, string(metricsValue), metricsKeyTTL)
		if err == nil {
			log.GetLogger().Debugf("succeed to report bridgeFunc metrics key: %s, value:%s",
				bridgeMetricsKey, string(metricsValue))
			return
		}
		log.GetLogger().Warnf("failed to report key: %s, err: %s, retry time %d", bridgeMetricsKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
}

func (wb *workerBroker) AddRequestCount(count int64) {
	tcpBus, ok := wb.bus.(*tcpbus.TCPBus)
	if ok {
		tcpBus.AddDirectRequestCount(count)
	}
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

// RecoverAsyncRequest -
func (wb *workerBroker) RecoverAsyncRequest(report tcpbus.RuntimeReport) {
	if !wb.longRunFunc || len(report.RequestMap) == 0 {
		return
	}
	funcKey := utils.GetFuncKeyFromFuncKeyWithResource(wb.funcKeyWithResource)
	var num int64
	for requestID, request := range report.RequestMap {
		if request == nil {
			continue
		}
		asyRequest := asyncRequest{}
		asyRequest.functionKey = funcKey
		asyRequest.instanceID = wb.workerID
		asyRequest.traceID = requestID
		if request.StartTime != 0 {
			asyRequest.startTime = time.Unix(request.StartTime, 0)
			asyRequest.deadline = asyRequest.startTime.Add(wb.invokeTimeout)
		} else {
			asyRequest.startTime = time.Now()
			asyRequest.deadline = asyRequest.startTime.Add(wb.invokeTimeout)
		}
		asyRequest.headers = make(map[string]string)
		asyRequest.headers[constants.HeaderWorkflowID] = request.GetWorkflowID()
		asyRequest.headers[constants.HeaderWorkflowRunID] = request.GetWorkflowRunID()
		asyRequest.headers[constants.HeaderWorkflowStateID] = request.GetWorkflowStateID()
		asyRequest.headers[constants.HeaderInvokeSource] = request.GetInvokeSource()
		asyRequest.headers[constants.HeaderOriginVersionTag] = request.GetOriginVersionTag()
		wb.asyncDirectRequestMap.Store(requestID, asyRequest)
		workerevaluator.GetInstance().RecordRequestStart(wb.workerID)
		log.GetLogger().Infof("succeed to recover async request %s for instance %s", requestID, wb.workerID)
		num++
	}
	if num != 0 {
		wb.directLock.Lock()
		wb.requestCount = wb.requestCount + int(num)
		wb.directLock.Unlock()
		wb.AddRequestCount(num)
		if wb.isConcurrent && wb.summaryRecorder != nil {
			wb.summaryRecorder.StartBilling(constants.InvokeStage, multipleDefaultConcurrentNum, uint64(num))
		}
	}
}

// ProcessAsyncResult process async result
func (wb *workerBroker) ProcessAsyncResult(msg *protocol.Message, requestID string, clientExited bool) {
	if clientExited {
		wb.processClientExitedAsyncResult()
		return
	}
	if msg == nil {
		return
	}
	requestID = resolveRequestID(msg, requestID)
	if requestID == "" {
		return
	}
	val, ok := wb.asyncDirectRequestMap.Load(requestID)
	if !ok {
		log.GetLogger().Warnf("failed to find async request, traceID: %s", requestID)
		val = asyncRequest{traceID: requestID, headers: make(map[string]string)}
	}
	request, ok := val.(asyncRequest)
	if !ok {
		log.GetLogger().Warnf("failed to load asyncRequest, traceID: %s", requestID)
		return
	}
	if wb.isConcurrent && wb.summaryRecorder != nil {
		wb.summaryRecorder.StopBilling(constants.InvokeStage, multipleDefaultConcurrentNum)
	}
	var response asyncResponse
	if msg.Header.ErrCode != proxyerrors.StateOKCode {
		response = wb.initAsyncErrResponse(msg, request)
	} else {
		message := &pb.Message{}
		if err := proto.Unmarshal(msg.Payload, message); err != nil {
			log.GetLogger().Warnf("bus call response unmarshal, traceID: %s, error: %s", requestID, err.Error())
			return
		}
		response = wb.initAsyncResponse(message, request)
	}
	defer wb.processAsyncRequestEnd(requestID)
	workerevaluator.ProcessAsyncResult(response.statusCode, response.header,
		response.body, request.traceID, response.instanceID)
}

func resolveRequestID(msg *protocol.Message, requestID string) string {
	if requestID != "" {
		return requestID
	}
	if msg.Header.ErrCode != proxyerrors.StateOKCode {
		log.GetLogger().Warnf("invalid requestID with error code %s", msg.Header.ErrCode)
		return ""
	}
	message := &pb.Message{}
	if err := proto.Unmarshal(msg.Payload, message); err != nil {
		log.GetLogger().Warnf("bus call response unmarshal, packageID: %s, error: %s", msg.Header.PacketID, err.Error())
		return ""
	}
	switch message.Type {
	case pb.Message_POSIX_CALL_RESPONSE:
		requestID = message.PosixCallResult.RequestID
	default:
	}
	return requestID
}

// processClientExitedAsyncResult process async result when runtime process exited
func (wb *workerBroker) processClientExitedAsyncResult() {
	if wb.asyncDirectRequestMap == nil {
		return
	}
	wb.asyncDirectRequestMap.Range(func(key, value any) bool {
		traceID, ok := key.(string)
		if !ok {
			return true
		}
		request, ok := value.(asyncRequest)
		if !ok {
			return true
		}
		log.GetLogger().Infof("start to process client exited async result, traceID: %s", traceID)
		msg := &protocol.Message{}
		msg.Header.ErrCode = proxyerrors.ClientExitErrCode
		msg.Payload = []byte(proxyerrors.ClientExitErr)
		if wb.isConcurrent && wb.summaryRecorder != nil {
			wb.summaryRecorder.StopBilling(constants.InvokeStage, multipleDefaultConcurrentNum)
		}
		response := wb.initAsyncErrResponse(msg, request)
		workerevaluator.ProcessAsyncResult(response.statusCode, response.header,
			response.body, request.traceID, response.instanceID)
		wb.processAsyncRequestEnd(traceID)
		return true
	})
}

func (wb *workerBroker) initAsyncResponse(message *pb.Message, request asyncRequest) asyncResponse {
	if message == nil {
		return asyncResponse{}
	}
	response := asyncResponse{header: wb.getAsyncResponseHeaderMap(request)}
	switch message.Type {
	case pb.Message_POSIX_CALL_RESPONSE:
		log.GetLogger().Infof("start to sync abandoned response by posix call result, "+
			"workerID: %s, traceID: %s", request.instanceID, request.traceID)
		result := message.PosixCallResult
		if result == nil {
			return response
		}
		if len(result.RawData) > internal.MaxStoreResponseSize {
			log.GetLogger().Warnf("function response size: %d, truncate and reserve size: %d",
				len(result.RawData), internal.MaxStoreResponseSize)
			buffer := bytes.NewBuffer(result.RawData)
			buffer.Truncate(internal.MaxStoreResponseSize)
			response.body = buffer.Bytes()
		} else {
			response.body = result.RawData
		}

		if result.ErrorCode == proxyerrors.StateOKCode {
			response.statusCode = http.StatusOK
		} else {
			response.statusCode = http.StatusInternalServerError
			response.body = internal.ConvertErrorToHTTPResponseBody(result.ErrorCode, result.ErrorMessage)
		}
		response.header[constants.HeaderInvokeSummary] = result.Summary
		response.instanceID = request.instanceID
		if !wb.isConcurrent && !wb.isMinInstance {
			summary := &snerror.SummaryInfo{}
			err := json.Unmarshal([]byte(result.Summary), summary)
			if err != nil {
				log.GetLogger().Warnf("failed to amend runtime summary %s error %s", result.Summary, err.Error())
			}
			response.header[constants.HeaderBillingDuration] = strconv.Itoa(summary.BillingDuration)
		} else {
			response.header[constants.HeaderBillingDuration] = ""
		}
		return response
	// The type field is not returned in the old version. This is for smooth upgrade.
	default:
		log.GetLogger().Infof("sync abandoned response for traceID: %s", request.traceID)
		callResponse := message.CallResponse
		if callResponse == nil {
			return response
		}
		// according to log size, no log in storage; response size is limited too.
		if len(callResponse.RawData) > internal.MaxStoreResponseSize {
			log.GetLogger().Warnf("function response size: %d, truncate and reserve size: %d",
				len(callResponse.RawData), internal.MaxStoreResponseSize)
			buffer := bytes.NewBuffer(callResponse.RawData)
			buffer.Truncate(internal.MaxStoreResponseSize)
			response.body = buffer.Bytes()
		} else {
			response.body = callResponse.RawData
		}

		if callResponse.ErrorCode == proxyerrors.StateOKCode {
			response.statusCode = http.StatusOK
		} else {
			response.statusCode = http.StatusInternalServerError
			response.body = internal.ConvertErrorToHTTPResponseBody(callResponse.ErrorCode, callResponse.ErrorMessage)
		}
		response.header[constants.HeaderInvokeSummary] = callResponse.Summary
		response.instanceID = request.instanceID
		if !wb.isConcurrent && !wb.isMinInstance {
			summary := &snerror.SummaryInfo{}
			err := json.Unmarshal([]byte(callResponse.Summary), summary)
			if err != nil {
				log.GetLogger().Warnf("failed to amend runtime summary %s error %s", callResponse.Summary, err.Error())
			}
			response.header[constants.HeaderBillingDuration] = strconv.Itoa(summary.BillingDuration)
		} else {
			response.header[constants.HeaderBillingDuration] = ""
		}
	}
	return response
}

func (wb *workerBroker) initAsyncErrResponse(msg *protocol.Message, request asyncRequest) asyncResponse {
	costTime := time.Since(request.startTime)
	if costTime > wb.invokeTimeout {
		costTime = wb.invokeTimeout
	}
	response := asyncResponse{header: wb.getAsyncResponseHeaderMap(request)}
	errCode := msg.Header.ErrCode
	errMessage := string(msg.Payload)
	if msg.Header.ErrCode == proxyerrors.ClientExitErrCode || msg.Header.ErrCode == proxyerrors.SendReqErrCode ||
		msg.Header.ErrCode == proxyerrors.TimeoutErrCode {
		report, checkErr := wb.checkRuntime(request.traceID, msg.Header.ErrCode != proxyerrors.TimeoutErrCode)
		if checkErr == nil && report != nil {
			response.header[constants.HeaderInvokeSummary] = amendRuntimeSummary(report.Summary, costTime)
		}
		if msg.Header.ErrCode == proxyerrors.TimeoutErrCode {
			errCode = snerror.RuntimeInvokeTimeoutCode
			errMessage = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), wb.invokeTimeout)
		} else if report != nil && report.ErrorCode != proxyerrors.StateOKCode {
			errCode = report.ErrorCode
			errMessage = report.ErrorMessage
		}
	}
	response.body = internal.ConvertErrorToHTTPResponseBody(errCode, errMessage)
	response.statusCode = http.StatusInternalServerError
	response.instanceID = request.instanceID
	// For concurrent function calls, calculate billing duration from use-resource-windows
	if !wb.isConcurrent && !wb.isMinInstance {
		response.header[constants.HeaderBillingDuration] =
			strconv.Itoa(int(costTime.Microseconds() / microToMilliSecond))
	} else {
		response.header[constants.HeaderBillingDuration] = ""
	}
	return response
}

func (wb *workerBroker) getAsyncResponseHeaderMap(request asyncRequest) map[string]string {
	header := make(map[string]string)
	if request.headers != nil {
		header[constants.HeaderWorkflowID] = request.headers[constants.HeaderWorkflowID]
		header[constants.HeaderWorkflowRunID] = request.headers[constants.HeaderWorkflowRunID]
		header[constants.HeaderWorkflowStateID] = request.headers[constants.HeaderWorkflowStateID]
		header[constants.HeaderInvokeSource] = request.headers[constants.HeaderInvokeSource]
		header[constants.HeaderOriginVersionTag] = request.headers[constants.HeaderOriginVersionTag]
	}
	header[constants.HeaderInstanceID] = request.instanceID
	header[constants.HeaderExecuteNode] = internal.SelfNodeIP
	_, memory := utils.GetCPUMemoryFromInstanceID(request.instanceID)
	tenantID, fullFuncName, version := utils.ParseFuncKey(request.functionKey)
	serviceName, funcName, err := urnutils.GetFunctionNameAndServiceName(fullFuncName)
	if err != nil {
		log.GetLogger().Warnf("failed to parse function name and service name: %s", fullFuncName)
	}
	// "projectID@package@functionName@version@memsize"
	header[constants.HeaderFuncURN] = fmt.Sprintf("%s@%s@%s@%s@%s", tenantID, serviceName,
		funcName, version, memory)
	return header
}

func (wb *workerBroker) processAsyncRequestEnd(requestID string) {
	wb.asyncDirectRequestMap.Delete(requestID)
	wb.directLock.Lock()
	wb.requestCount--
	wb.lastReqFinish = time.Now()
	wb.directLock.Unlock()
	wb.AddRequestCount(-1)
}

func (wb *workerBroker) waitRecoverNewConnReady() {
	select {
	case <-wb.recoverWaitNewConnCh:
		log.GetLogger().Infof("new conn is ready by recover runtime for pod %s", wb.workerID)
	case <-time.After(waitNewConnReadyTimeout):
		log.GetLogger().Warnf("failed to wait new conn is ready by recover runtime for pod %s", wb.workerID)
	}
	wb.recoverBlockBroadcast()
}

func (wb *workerBroker) recoverBlockBroadcast() {
	wb.recoverBlock.L.Lock()
	wb.blockForRecover = false
	wb.recoverBlock.L.Unlock()
	wb.recoverBlock.Broadcast()
}
