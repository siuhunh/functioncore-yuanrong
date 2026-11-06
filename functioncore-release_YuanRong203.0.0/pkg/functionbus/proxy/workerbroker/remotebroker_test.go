/*
 * Copyright (c) 2023-2024 Huawei Technologies Co., Ltd
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

package workerbroker

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestRecover(t *testing.T) {
	callCount := 0
	common.SendRecoverReq = func(nodeID, traceID string, req *types.TCPRequest) *types.TCPResponse {
		callCount++
		return nil
	}
	rb := &remoteBroker{}
	rb.recover(0, "", false)
	assert.Equal(t, 1, callCount)
}

func TestCancel(t *testing.T) {
	callCount := 0
	common.SendCancelReq = func(nodeID, traceID string, req *types.TCPRequest, dst *common.CancelProperty) *types.TCPResponse {
		callCount++
		return nil
	}
	rb := &remoteBroker{}
	rb.cancel(nil, 0, "")
	assert.Equal(t, 1, callCount)
}

func TestProcess(t *testing.T) {
	common.SendCallReq = func(nodeID, traceID string, callReq *types.TCPRequest, dst *common.FunctionProperty) *types.TCPResponse {
		return &types.TCPResponse{
			ErrorCode: proxyerrors.StateOKCode,
		}
	}
	rb := &remoteBroker{
		dstProperty: &common.FunctionProperty{},
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestStart, func(_ *workerevaluator.WorkerEvaluator, _ string) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestEnd, func(_ *workerevaluator.WorkerEvaluator, _ string, _ time.Duration) {
			return
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	req := &types.DispatcherRequest{
		Response: make(chan *types.TCPResponse, 1),
	}
	rb.process(req)
	rsp := <-req.Response
	assert.Equal(t, proxyerrors.StateOKCode, int(rsp.ErrorCode))
	req.Canceled = true
	rb.process(req)
	rsp = <-req.Response
	assert.Equal(t, proxyerrors.RequestCanceledCode, rsp.ErrorCode)
}
