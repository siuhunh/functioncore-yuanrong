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
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
)

type broker interface {
	cancel(dst *common.CancelProperty, requestID uint64, traceID string) *types.TCPResponse
	recover(requestID uint64, traceID string, flag bool) *types.TCPResponse
	process(request *types.DispatcherRequest)
}

type remoteBroker struct {
	dstProperty         *common.FunctionProperty
	funcKeyWithResource string
	workerID            string
	nodeIP              string
	isInitialConcurrent bool
}

func (rb *remoteBroker) recover(requestID uint64, traceID string, flag bool) *types.TCPResponse {
	return common.SendRecoverReq(rb.nodeIP, traceID,
		&types.TCPRequest{RequestID: requestID, WorkerID: rb.workerID, IsStateFul: flag,
			IsInitialConcurrent: rb.isInitialConcurrent})
}

func (rb *remoteBroker) cancel(dst *common.CancelProperty, requestID uint64, traceID string) *types.TCPResponse {
	return common.SendCancelReq(rb.nodeIP, traceID,
		&types.TCPRequest{RequestID: requestID, WorkerID: rb.workerID}, dst)
}

func (rb *remoteBroker) process(request *types.DispatcherRequest) {
	if request == nil {
		log.GetLogger().Errorf("args error, request is nil")
		return
	}
	pool := GetInstance().brokerTaskPool
	pool.Submit(func() {
		workerevaluator.GetInstance().RecordRequestStart(rb.workerID)
		// Even if the request is not canceled, the other proxy will handle the cancel of the request. Delete it
		// anyway.
		cancel.GetInstance().DeleteRequest(request.FutureID)
		if request.Canceled {
			request.Response <- &types.TCPResponse{
				ErrorCode:    proxyerrors.RequestCanceledCode,
				ErrorMessage: proxyerrors.RequestCanceledError,
			}
			workerevaluator.GetInstance().RecordRequestEnd(rb.workerID, 0)
			return
		}
		var dst = *rb.dstProperty
		dst.FutureID = request.FutureID
		startTime := time.Now()
		resp := common.SendCallReq(rb.nodeIP, request.TraceID, request.Request, &dst)
		cost := time.Since(startTime)
		if resp.ErrorCode == proxyerrors.StateAcceptedCode {
			log.GetLogger().Infof("receive request-accepted response, traceID: %s", request.TraceID)
		} else {
			workerevaluator.GetInstance().RecordRequestEnd(rb.workerID, cost)
		}
		resp.CallNode = rb.nodeIP
		resp.CallInstanceID = rb.workerID
		resp.ActualCallStartTime = startTime
		resp.ActualCallCost = cost
		request.Response <- resp
	})
}
