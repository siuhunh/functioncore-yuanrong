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

// Package triggerbykey implements triggerbykey api.
package triggerbykey

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// ByKey receives a request from triggerbykey with a key,
// and transforms it to state id by uuid v5 (state id will be created if not exist).
// Then function task will load state by state id and invoke function by funcName and args.
// After the function is completed, the result will be sent back to triggerbykey.
func ByKey(taskCtx taskcontext.Context, req *Request) (int32, string, string) {
	log.GetLogger().Debugf("triggerByKey, serviceID: %s, key: %s, funcName: %s, version: %s, traceID: %s",
		taskCtx.ServiceID(), req.Key, req.FuncName, req.Version, taskCtx.TraceID())
	invokeReq := &invoke.Request{RequestPb: invoke.RequestPb{
		FuncName: req.FuncName,
		Version:  req.Version,
		Payload:  req.Payload,
		Key:      req.Key,
		ByKey:    true,
	}}
	rawResp := invokeReq.Caller(taskCtx)
	return processInvokeResult(taskCtx, rawResp)
}

// ByStateID -
func ByStateID(taskCtx taskcontext.Context, req *Request) (int32, string, string) {
	log.GetLogger().Debugf("triggerByStateID, stateID: %s, stateKey: %s, traceID: %s",
		req.StateID, req.Key, taskCtx.TraceID())
	invokeReq := &invoke.Request{
		RequestPb: invoke.RequestPb{
			FuncName: req.FuncName,
			Version:  req.Version,
			Payload:  req.Payload,
			ByKey:    false,
		},
	}
	invokeReq.StateID = req.StateID
	rawResp := invokeReq.Caller(taskCtx)
	return processInvokeResult(taskCtx, rawResp)
}

func processInvokeResult(taskCtx taskcontext.Context, rawResp taskcontext.Serializable) (int32, string, string) {
	if errResp, ok := rawResp.(*common.ErrorResponsePb); ok {
		log.GetLogger().Errorf("triggerError, traceID: %s, code: %d, msg: %s", taskCtx.TraceID(),
			errResp.GetCode(), errResp.GetMessage())
		return errResp.Code, errResp.Message, ""
	}
	resp, ok := rawResp.(*invoke.Response)
	if !ok {
		return common.EFunctionTask, "invalid type: not *invoke.Response", ""
	}
	if resp.Code != common.EOK {
		log.GetLogger().Errorf("triggerError, traceID: %s, code: %d, msg: %s", taskCtx.TraceID(), resp.Code,
			resp.Message)
		return resp.Code, resp.Message, ""
	}
	log.GetLogger().Debugf("triggerFirstResult, traceID: %s, futureID: %s, stateID: %s", taskCtx.TraceID(),
		resp.GetFutureID(), resp.GetStateID())
	future := taskCtx.FutureStore().Get(resp.FutureID)
	r := future.GetResponse()
	taskCtx.FutureStore().Delete(resp.FutureID)
	log.GetLogger().Debugf("triggerFinalResult, traceID: %s, futureID: %s, stateID: %s, code: %d, message: %s",
		taskCtx.TraceID(), resp.GetFutureID(), resp.GetStateID(), r.Code, r.Message)
	if r.Code != common.EOK {
		return r.Code, r.Message, r.Log
	}
	return r.Code, r.Result, r.Log
}
