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

package getfuture

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// Caller gets the value of the future in caller function task.
func (req *Request) Caller(taskCtx taskcontext.Context, deadlockChecker deadlock.Checker) (*Response, string) {
	log.GetLogger().Debugf("caller, serviceID: %s, stateID: %s, traceID: %s, futureID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())
	resp, logs := req.getFutureFromStore(taskCtx, deadlockChecker)
	return resp, logs
}

func (req *Request) getFutureFromStore(taskCtx taskcontext.Context, checker deadlock.Checker) (*Response, string) {
	sncFuture := taskCtx.FutureStore().Get(req.FutureID)
	if sncFuture == nil {
		log.GetLogger().Warnf("efs.get, serviceID: %s, stateID: %s, traceID: %s, futureID: %s", taskCtx.ServiceID(),
			req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())
		sncFuture = taskCtx.EFS().GetRequest(taskCtx.TenantID(), taskCtx.ServiceID(),
			req.FutureID, taskCtx.ReqIDList())
		if sncFuture == nil {
			log.GetLogger().Errorf("unknownFutureID, serviceID: %s, stateID: %s, traceID: %s, futureID: %s",
				taskCtx.ServiceID(), req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())
			return &Response{
				Code:    common.EInvalidParam,
				Message: "unknown future id: " + req.FutureID,
			}, ""
		}
	}
	return req.get(taskCtx, sncFuture, checker)
}

func (req *Request) get(taskCtx taskcontext.Context, sncFuture *snfuture.Future,
	checker deadlock.Checker) (*Response, string) {
	if req.StateID == "" || !config.Conf.ResolveDeadlock {
		resp := sncFuture.GetResponse()
		if resp.Message == common.TaskGetFutureForwardMsg {
			return &Response{
				Code:    snerror.StatefulFunctionReloading,
				Message: snerror.ErrText(snerror.StatefulFunctionReloading),
			}, resp.Log
		}
		return &Response{
			Code:        resp.Code,
			Message:     resp.Message,
			Result:      resp.Result,
			State:       "",
			DcCallStack: resp.DcCallStack,
		}, resp.Log
	}
	return req.getResolveDeadLock(taskCtx, sncFuture, checker)
}

func (req *Request) getResolveDeadLock(taskCtx taskcontext.Context, sncFuture *snfuture.Future,
	checker deadlock.Checker) (*Response, string) {
	resp := sncFuture.PeekResponse()
	updateState := false
	state := ""
	if resp == nil {
		startResp := checker.Start(&deadlock.StartRequest{
			TenantID:         taskCtx.TenantID(),
			ServiceID:        taskCtx.ServiceID(),
			StateID:          req.StateID,
			State:            req.State,
			FutureReqIDLists: deadlock.ConvertLists2Struct([]common.ReqIDList{sncFuture.ReqIDList}),
		})
		if startResp.Code != common.EOK {
			return &Response{
				Code:    startResp.Code,
				Message: startResp.Message,
			}, ""
		}

		resp = sncFuture.GetResponse()

		if startResp.DeadlockID != "" {
			endResp := checker.End(&deadlock.EndRequest{
				TenantID:   taskCtx.TenantID(),
				ServiceID:  taskCtx.ServiceID(),
				StateID:    req.StateID,
				DeadlockID: startResp.DeadlockID,
			})
			if endResp.Code != common.EOK {
				return &Response{
					Code:    endResp.Code,
					Message: endResp.Message,
				}, ""
			}
			updateState = true
			state = endResp.State
		}
	}
	return &Response{
		Code:        resp.Code,
		Message:     resp.Message,
		DcCallStack: resp.DcCallStack,
		Result:      resp.Result,
		State:       state,
		UpdateState: updateState,
	}, resp.Log
}
