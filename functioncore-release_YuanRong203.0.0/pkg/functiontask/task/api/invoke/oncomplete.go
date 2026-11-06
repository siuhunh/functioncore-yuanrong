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

package invoke

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func waitSNFutureReady(taskCtx taskcontext.Context, requiredFuture *snfuture.Future) *common.Response {
	log.GetLogger().Debugf("wait sn future ready, funcName: %s, futureID: %s, traceID: %s, stateID: %s",
		requiredFuture.FuncName, requiredFuture.ID, requiredFuture.TraceID, requiredFuture.StateID)
	resp := requiredFuture.GetResponse()
	if resp.Code != 0 {
		return &common.Response{
			Code:    resp.Code,
			Message: resp.Message,
		}
	}
	log.GetLogger().Debugf("future done, code: %d, stateID: %s, traceID: %s, futureID: %s, funcName: %s", resp.Code,
		resp.StateID, requiredFuture.TraceID, requiredFuture.ID, requiredFuture.FuncName)
	return resp
}

func onComplete(taskCtx taskcontext.Context, req *OnCompleteRequest,
	requiredFuture *snfuture.Future, future *snfuture.Future) *common.Response {
	resp := waitSNFutureReady(taskCtx, requiredFuture)
	if resp.Code != 0 {
		return resp
	}
	request := &Request{
		RequestPb: RequestPb{
			FuncName: req.FuncName,
			Payload:  resp.Result,
			StateID:  req.StateID,
			Version:  req.Version,
		},
		create: req.create,
	}
	result := request.invokeLocal(taskCtx, future)
	return result
}

// OnComplete the function.
func OnComplete(taskCtx taskcontext.Context, req *OnCompleteRequest, requiredFuture *snfuture.Future) *Response {
	err := util.CreateStateIfNeeded(taskCtx, req)
	if err != nil {
		return &Response{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  req.FuncName,
		Version:   req.Version,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
	})
	log.GetLogger().Debugf("waitFuture, reqFutureID: %s, respFutureID: %s, funcName: %s, stateID: %s,"+
		"createStateQueue: %t", req.GetFutureID(), sncFuture.ID, req.GetFuncName(), req.GetStateID(),
		req.GetCreateState())
	taskCtx.FutureStore().Add(sncFuture)

	go func() {
		result := onComplete(taskCtx, req, requiredFuture, sncFuture)
		sncFuture.SetResponse(result)
	}()
	return &Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	}
}
