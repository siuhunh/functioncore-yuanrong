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

// Package terminate implements terminate api.
package terminate

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// Terminate the state by stateID.
func (req *Request) Terminate(taskCtx taskcontext.Context) *Response {
	if isTerminate, resp := IsRelease(taskCtx, req); !isTerminate {
		return resp
	}

	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  common.TerminateFuncName,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
		ID:        req.FutureID,
	})
	log.GetLogger().Infof("terminate, traceID: %s, stateID: %s, futureID: %s, funcName: %s",
		taskCtx.TraceID(), req.GetStateID(), req.GetFutureID(), req.GetFuncName())

	r := common.NewTerminateRequest(taskCtx.ReqIDList(), sncFuture.ID)
	terminateErr := taskCtx.StateQueueMap().Request(&metadata.Metadata{
		TenantID:     taskCtx.TenantID(),
		ServiceID:    taskCtx.ServiceID(),
		StateID:      req.StateID,
		FunctionName: req.FuncName,
	}, r, false)

	success := taskCtx.FutureStore().SetIfNotExist(sncFuture)
	if !success {
		return &Response{
			Code:     common.EOK,
			FutureID: sncFuture.ID,
		}
	}
	go util.ReturnResponseToTask(taskCtx, r, sncFuture)

	if terminateErr != nil {
		log.GetLogger().Errorf("terminate, traceID: %s, stateID: %s, futureID: %s, funcName: %s, code: %d, err: %s",
			taskCtx.TraceID(), req.GetStateID(), req.GetFutureID(), req.GetFuncName(), terminateErr.Code(),
			terminateErr.Error())
		return &Response{
			Code:    terminateErr.Code(),
			Message: terminateErr.Error(),
		}
	}
	return &Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
	}
}

// IsRelease send request to bridgeManager to get a flag as result to determine whether to terminate
func IsRelease(taskCtx taskcontext.Context, req *RequestPb) (bool, *Response) {
	if len(req.PodName) != 0 {
		clientInstance := bridgetypes.ClientInstance{
			PodName:       req.PodName,
			AssignStateID: req.StateID,
			FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID:       taskCtx.TenantID(),
				ServiceID:      taskCtx.ServiceID(),
				BridgeFuncName: req.FuncName,
			},
		}
		isTerminate, err := util.Release(taskCtx, req.FuncName, clientInstance)
		if err != nil {
			log.GetLogger().Debugf("Release, traceID: %s, stateID: %s, futureID: %s, funcName: %s, code: %d, err: %s",
				taskCtx.TraceID(), req.GetStateID(), req.GetFutureID(), req.GetFuncName(), err.Code(), err.Error())
			return false, &Response{
				Code:    err.Code(),
				Message: err.Error(),
			}
		}
		log.GetLogger().Debugf("terminate, traceID: %s, stateID: %s, futureID: %s, funcName: %s, isTerminate: %t",
			taskCtx.TraceID(), req.GetStateID(), req.GetFutureID(), req.FuncName, isTerminate)
		if !isTerminate {
			return false, &Response{
				Code: common.EOK,
			}
		}
	}
	return true, nil
}
