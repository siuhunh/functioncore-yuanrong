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

// Package workerevaluator contains worker metrics and supports observers
package workerevaluator

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

const (
	abandonedResponseLatencies = -1
	asyncRespHeaderSize        = 5
)

// WorkerEvaluator - data for load balance and notify observer
type WorkerEvaluator struct {
	timeWindowNum int
	metrics       sync.Map
	observers     sync.Map
}

var (
	instance             *WorkerEvaluator
	timeWindowsThreshold = 5
	timeWindowsLength    = time.Minute
)

// CreateInstance create singleton of WorkerEvaluator
func CreateInstance(stopCh <-chan struct{}) {
	instance = &WorkerEvaluator{
		observers: sync.Map{},
		metrics:   sync.Map{},
	}
	go instance.periodCollectMetrics(stopCh)
}

// GetInstance get singleton of WorkerEvaluator
func GetInstance() *WorkerEvaluator {
	return instance
}

func (we *WorkerEvaluator) periodCollectMetrics(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	// modify default configs
	if metricsTimeWindowConfigs := common.FConfig.WorkerMetricsTimeWindow; metricsTimeWindowConfigs != nil {
		if metricsTimeWindowConfigs.NumberThreshold != 0 {
			log.GetLogger().Infof("set %d as reporting metrics time window threshold",
				metricsTimeWindowConfigs.NumberThreshold)
			timeWindowsThreshold = metricsTimeWindowConfigs.NumberThreshold
		}
		if metricsTimeWindowConfigs.TimeWindowLength != 0 {
			log.GetLogger().Infof("set %d as metrics time window length",
				metricsTimeWindowConfigs.TimeWindowLength)
			timeWindowsLength = time.Duration(metricsTimeWindowConfigs.TimeWindowLength) * time.Second
		}
	}

	collectTimeWindowTicker := time.NewTicker(timeWindowsLength)
	log.GetLogger().Infof("start to collect and report worker metrics periodically")
	for {
		select {
		case <-collectTimeWindowTicker.C:
			we.collectMetrics()
		case <-stopCh:
			return
		}
	}
}

func (we *WorkerEvaluator) collectMetrics() {
	secondsTimestamp := time.Now().Unix()
	we.timeWindowNum++

	var workerList list.List
	we.metrics.Range(func(k, v interface{}) bool {
		workerList.PushBack(v)
		return true
	})

	// collect all worker views and report
	var reportMetrics []workermanager.WorkerView
	var needReport = false
	if we.timeWindowNum == timeWindowsThreshold {
		needReport = true
		we.timeWindowNum = 0
	}
	for e := workerList.Front(); e != nil; e = e.Next() {
		if metrics, ok := e.Value.(*WorkerMetrics); ok {
			metrics.twView.collectSingleTimeWindow(secondsTimestamp)
			if !needReport {
				continue
			}
			tenantID, funcName, funcVersion :=
				utils.ParseFuncKey(utils.GetFuncKeyFromFuncKeyWithResource(metrics.FuncKeyWithResource))
			if isStatefulFunc(metrics) {
				continue
			}
			if isMicroServiceFunc(metrics) {
				continue
			}
			if isDisableReportFunc(funcName) {
				continue
			}
			if isWebSocketFunc(metrics) {
				continue
			}
			reportMetrics = append(reportMetrics, workermanager.WorkerView{
				InstanceID:  metrics.ID,
				FuncName:    funcName,
				FuncVersion: funcVersion,
				TenantID:    tenantID,
				BusinessID:  common.InternalScaleBusinessID,
				Metrics:     metrics.twView.getTimeWindows(),
			})
		}
	}
	if len(reportMetrics) != 0 {
		view := &workermanager.View{
			Type:   workermanager.WorkerMetric,
			Object: reportMetrics,
		}
		go we.reportWorkerMetrics(view)
	}
}

const reportMetricsTimeout = 5 * time.Second

func isDisableReportFunc(funcName string) bool {
	if common.FConfig.AutoScalerConfig != nil {
		for _, name := range common.FConfig.AutoScalerConfig.DisAbleReportFunctionList {
			if name == funcName {
				log.GetLogger().Infof("skip report function %s metrics to worker-manager", funcName)
				return true
			}
		}
	}
	return false
}

func (we *WorkerEvaluator) reportWorkerMetrics(workerMetric *workermanager.View) {
	data, err := json.Marshal(workerMetric)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal worker views: %s", err.Error())
		return
	}
	requestURI := wmhealth.GetWorkerManagerBaseURL() + "/worker-manager/v1/functions/worker/metrics"
	ctx, cancel := context.WithTimeout(context.Background(), reportMetricsTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Warnf("failed to new request for workers view, err %s", err.Error())
		return
	}

	utils.FillInWorkerManagerRequestHeaders(request)

	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Warnf("failed to report workers view, err %s", err.Error())
		return
	}
	defer resp.Body.Close()
}

// GetWorkerNum returns the number of workers
func (we *WorkerEvaluator) GetWorkerNum() int {
	instanceNum := 0
	we.metrics.Range(func(k, v interface{}) bool {
		instanceNum++
		return true
	})
	return instanceNum
}

// AddWorker add local or remote worker record and publish
func (we *WorkerEvaluator) AddWorker(workerID string, opts ...WorkerMetricsOpt) {
	metrics := we.getOrAddWorkerMetrics(workerID, opts...)
	metrics.sendEnableChan(true)
}

// SyncAbandoned sync abandoned response
func SyncAbandoned(response *pb.CallResponse) {
	traceID := response.Labels[constants.HeaderRequestID]
	if response.ErrorCode == proxyerrors.StateAcceptedCode {
		log.GetLogger().Warnf("got request accepted response on worker: %s", response.InstanceID)
		cancel.GetInstance().RecordAsyncRequests(response.InstanceID, traceID)
		return
	}

	log.GetLogger().Infof("sync abandoned response for traceID: %s", traceID)
	GetInstance().RecordRequestEnd(response.InstanceID, abandonedResponseLatencies, false)
	cancel.GetInstance().DeleteAbandonedRequest(traceID)

	httpResp := &asyncResponse{
		Headers: make(map[string]string, asyncRespHeaderSize),
	}
	// according to log size, no log in storage; response size is limited too.
	if len(response.RawData) > internal.MaxStoreResponseSize {
		log.GetLogger().Warnf("function response size: %d, truncate and reserve size: %d",
			len(response.RawData), internal.MaxStoreResponseSize)
		buffer := bytes.NewBuffer(response.RawData)
		buffer.Truncate(internal.MaxStoreResponseSize)
		httpResp.Body = buffer.Bytes()
	} else {
		httpResp.Body = response.RawData
	}

	httpResp.Headers[constants.HeaderInvokeSummary] = response.Summary
	httpResp.Headers[constants.HeaderInstanceID] = response.InstanceID
	httpResp.Headers[constants.HeaderInvokeSource] = response.Labels[constants.HeaderInvokeSource]
	httpResp.Headers[constants.HeaderBillingDuration] = response.Labels[constants.HeaderBillingDuration]
	httpResp.Headers[constants.HeaderWorkflowID] = response.Labels[constants.HeaderWorkflowID]
	httpResp.Headers[constants.HeaderWorkflowRunID] = response.Labels[constants.HeaderWorkflowRunID]
	httpResp.Headers[constants.HeaderWorkflowStateID] = response.Labels[constants.HeaderWorkflowStateID]
	httpResp.Headers[constants.HeaderOriginVersionTag] = response.Labels[constants.HeaderOriginVersionTag]
	_, memory := utils.GetCPUMemoryFromInstanceID(response.InstanceID)
	tenantID, fullFuncName, version := utils.ParseFuncKey(response.FunctionKey)
	serviceName, funcName, err := urnutils.GetFunctionNameAndServiceName(fullFuncName)
	if err != nil {
		log.GetLogger().Warnf("failed to parse function name and service name: %s", fullFuncName)
	}
	// "projectID@package@functionName@version@memsize"
	httpResp.Headers[constants.HeaderFuncURN] = fmt.Sprintf("%s@%s@%s@%s@%s", tenantID, serviceName,
		funcName, version, memory)

	if response.ErrorCode == proxyerrors.StateOKCode {
		httpResp.StatusCode = http.StatusOK
		// according to body size, no response now, get it from response RawData in future
	} else {
		httpResp.StatusCode = http.StatusInternalServerError
		httpResp.Body = internal.ConvertErrorToHTTPResponseBody(response.ErrorCode, response.ErrorMessage)
	}

	if err = Client.reportAsyncResponse(traceID, httpResp); err != nil {
		log.GetLogger().Warnf("failed to report async response, traceID: %s, err: %s",
			traceID, err.Error())
	}
}

func isStatefulFunc(metrics *WorkerMetrics) bool {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(metrics.FuncKeyWithResource)
	funcMetaData, err := servicediscovery.GetRegistry().GetFunctionFuncMetaData(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata, funcKeyWithResource %s, err %s",
			metrics.FuncKeyWithResource, err.Error())
		return true
	}
	// stateful function no need to report metrics
	return funcMetaData.IsStatefulFunction
}

func isMicroServiceFunc(metrics *WorkerMetrics) bool {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(metrics.FuncKeyWithResource)
	funcMetaData, err := servicediscovery.GetRegistry().GetFunctionFuncMetaData(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata, funcKeyWithResource %s, err %s",
			metrics.FuncKeyWithResource, err.Error())
		return true
	}
	if funcMetaData.BusinessType == constants.BusinessTypeCAE {
		return true
	}
	return false
}

func isWebSocketFunc(metrics *WorkerMetrics) bool {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(metrics.FuncKeyWithResource)
	funcMetaData, err := servicediscovery.GetRegistry().GetFunctionFuncMetaData(functionKey)
	if err != nil {
		log.GetLogger().Warnf("failed to get metadata, funcKeyWithResource %s, err %s",
			metrics.FuncKeyWithResource, err.Error())
		return true
	}
	return commonUtils.IsWebSocketFunc(funcMetaData.BusinessType, funcMetaData.EnableRemoteDebug)
}
