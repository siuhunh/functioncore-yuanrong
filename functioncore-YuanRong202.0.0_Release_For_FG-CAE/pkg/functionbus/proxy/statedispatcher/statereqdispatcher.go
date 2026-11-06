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

// Package statedispatcher
package statedispatcher

import (
	"errors"
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

var errScheduleFailed = errors.New("schedule failed")
var stateQueueMetadataChannel chan common.StatefulFuncMetaData
var once sync.Once

const (
	instanceMetricsChanLen    = 10
	stateQueueMetadataChanLen = 1000
	forwardWorkerID           = "forward-worker"
)

// RequestDispatcher -
type RequestDispatcher struct {
	scheduler           Scheduler
	scaler              autoscaler.StateAutoScaler
	stopChan            chan struct{}
	insChan             chan string
	funcKeyWithResource string
	concurrency         uint32
}

// GetStateQueueMetadataChannel -
func GetStateQueueMetadataChannel() chan common.StatefulFuncMetaData {
	once.Do(func() {
		stateQueueMetadataChannel = make(chan common.StatefulFuncMetaData, stateQueueMetadataChanLen)
	})
	return stateQueueMetadataChannel
}

// CreateRequestDispatcher -
func CreateRequestDispatcher(funcKeyWithResource string, isNeedInstanceIdleCheck bool,
	metadata servicediscovery.FunctionMetaInfo) *RequestDispatcher {
	stopCh := make(chan struct{})
	scaler := autoscaler.NewStateAutoScaler(funcKeyWithResource, metadata)
	insChan := make(chan string, instanceMetricsChanLen)
	rd := &RequestDispatcher{
		scheduler:           createStateSchedule(scaler, stopCh, insChan, isNeedInstanceIdleCheck),
		scaler:              scaler,
		stopChan:            stopCh,
		insChan:             insChan,
		funcKeyWithResource: funcKeyWithResource,
		concurrency:         uint32(metadata.InstanceMetaData.ConcurrentNum),
	}
	return rd
}

// ReceiveInsDeleteEvent -
func (rd *RequestDispatcher) ReceiveInsDeleteEvent(stateID string) {
	rd.insChan <- stateID
}

// Start -
func (rd *RequestDispatcher) Start() {
	// synchronize the current worker information to the dispatcher.
	workerevaluator.GetInstance().Observe(rd.funcKeyWithResource, rd)
}

// Stop -
func (rd *RequestDispatcher) Stop() {
	workerevaluator.GetInstance().UnObserve(rd.funcKeyWithResource)
	close(rd.stopChan)
	log.GetLogger().Debugf("request dispatcher exited funcKeyWithResource:%s",
		urnutils.AnonymizeTenantKey(rd.funcKeyWithResource))
}

// OnObserved observe workerEvaluator
func (rd *RequestDispatcher) OnObserved(content interface{}, recovering bool) {
	workerMetric, ok := content.(*workerevaluator.WorkerMetrics)
	if !ok {
		log.GetLogger().Warnf("failed to convert content to WorkerMetrics")
		return
	}
	rd.scheduler.Update(workerMetric)
}

// Request -
func (rd *RequestDispatcher) Request(req *types.DispatcherRequest) error {
	log.GetLogger().Debugf("start to dispatch request funcKeyWithResource: %s, stateID: %s, traceID: %s",
		urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), req.Request.StateID, req.TraceID)
	req.Request.Concurrency = atomic.LoadUint32(&rd.concurrency)
	if req.Canceled {
		req.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.RequestCanceledCode,
			ErrorMessage: proxyerrors.RequestCanceledError,
		}
		cancel.GetInstance().DeleteRequest(req.FutureID)
		return nil
	}
	workerID, err := rd.scheduler.Schedule(req)
	if err != nil {
		return err
	}
	if workerID == "" {
		log.GetLogger().Debugf("schedule request on worker failed, funcKeyWithResource: %s, stateID: %s, "+
			"traceID: %s", urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), req.Request.StateID, req.TraceID)
		return errScheduleFailed
	}
	if workerID == forwardWorkerID {
		log.GetLogger().Debugf("workerID is forward-worker, not need to schedule request on localBus, "+
			"stateID: %s, traceID: %s", req.Request.StateID, req.TraceID)
		return nil
	}
	log.GetLogger().Infof("schedule request on workerID: %s, funcKeyWithResource: %s, stateID: %s, traceID: %s",
		workerID, urnutils.AnonymizeTenantKey(rd.funcKeyWithResource), req.Request.StateID, req.TraceID)
	if req.Request.IsStateFul {
		common.WorkerMap.SetVal(workerID)
	}
	// submit task
	workerbroker.GetInstance().Process(workerID, req)
	return nil
}

// ReceiveStateQueueMetadataPutEvent -
func (rd *RequestDispatcher) ReceiveStateQueueMetadataPutEvent(stateID string) {
	tenantID, complexFuncName, _ := utils.ParseFuncKey(rd.funcKeyWithResource)

	c := &urnutils.ComplexFuncName{}
	c.ParseFrom(complexFuncName)
	serviceID := c.GetSvcIDWithPrefix()
	funcName := c.FuncName
	GetStateQueueMetadataChannel() <- common.StatefulFuncMetaData{
		TenantID:     tenantID,
		ServiceID:    serviceID,
		StateID:      stateID,
		FunctionName: funcName,
	}
}
