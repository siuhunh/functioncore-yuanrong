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
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (a *Adaptor) onComplete(taskCtx taskcontext.Context, args *FuncArgs) {
	req := new(invoke.OnCompleteRequest)
	req.OnCompleteRequestPb = *args.GetOnCompleteReq()
	taskCtx.SetReqIDList(
		common.AppendReqID(taskCtx.ReqIDList(), req.FuncName, req.Version, req.CountOfFuncName),
	)

	requiredFuture := taskCtx.FutureStore().Get(req.FutureID)
	if requiredFuture == nil {
		log.GetLogger().Warnf("unknown futureID, futureID: %s, traceID: %s, stateID: %s", req.GetFutureID(),
			taskCtx.TraceID(), req.GetStateID())
		taskCtx.SetResult(&invoke.Response{
			Code:    common.EInvalidParam,
			Message: "unknown future id: " + req.FutureID})
		return
	}
	requiredFuture.AddRefCount()
	switch taskCtx.Source() {
	case common.SourceRuntimeToCentralFunctionTask:
		resp := invoke.OnComplete(taskCtx, req, requiredFuture)
		taskCtx.SetResult(resp)
	case common.SourceRuntimeToLocalFunctionTask:
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
	default:
		log.GetLogger().Warnf("invalid source: %d", taskCtx.Source())
		return
	}
	taskCtx.FutureStore().Delete(req.FutureID)
}
