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

package adaptor

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (a *Adaptor) calleeGetFuture(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetGetTaskReq()
	log.GetLogger().ForDebugf("calleeGetFuture, serviceID: %s, stateID: %s, traceID: %s, futureID: %s",
		taskCtx.ServiceID(), req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())

	resp := req.Callee(taskCtx)
	taskCtx.SetResult(resp)
}

func (a *Adaptor) getFuture(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetGetReq()
	log.GetLogger().ForDebugf("getFuture, serviceID: %s, stateID: %s, traceID: %s, futureID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())

	if taskCtx.Source() == common.SourceRuntimeToLocalFunctionTask {
		deadlockChecker := a.getDeadlockChecker(taskCtx, req.StateID, false)
		resp, funcLog := req.Caller(taskCtx, deadlockChecker)
		taskCtx.SetResultWithLog(resp, funcLog)
		taskCtx.FutureStore().Delete(req.FutureID)
		return
	}
	deadlockChecker := a.getDeadlockChecker(taskCtx, req.StateID, true)
	resp, funcLog := req.Get(taskCtx, deadlockChecker)
	taskCtx.SetResultWithLog(resp, funcLog)
}
