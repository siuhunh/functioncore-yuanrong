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

// Package invoke implements invoke api.
package invoke

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *Request) invokeLocal(taskCtx taskcontext.Context, future *snfuture.Future) *common.Response {
	if req.StateID == "" {
		cfg := taskCtx.InvokeConfig(req.Version, false, future.ID, req.RequestAttributes)
		return util.InvokeStateless(taskCtx, req.FuncName, req.Payload, req.InstanceID, cfg, req.ExtraMap, false)
	}
	request, err := util.InvokeStatefulLocal(taskCtx, &util.StatefulReq{
		FuncName:         req.FuncName,
		Version:          req.Version,
		Payload:          req.Payload,
		StateID:          req.StateID,
		CreateStateQueue: req.GetCreateState(),
	}, future)
	if err != nil {
		log.GetLogger().Errorf("invoke local, traceID: %s, futureID: %s, code: %d, err: %s", taskCtx.TraceID(),
			future.ID, err.Code(), err.Error())
		return common.SystematicError(err.Code(), err.Error())
	}
	result := request.GetResponse()
	return result
}

// Invoke the function.
func Invoke(taskCtx taskcontext.Context, req *Request) *Response {
	req.create = req.ByKey || req.StateID == taskcontext.NewState
	err := util.CreateStateIfNeeded(taskCtx, req)
	if err != nil {
		log.GetLogger().Errorf("createState, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			req.GetStateID(), err.Code(), err.Error())
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
	log.GetLogger().Debugf("invoke, funcName: %s, stateID: %s, traceID: %s, futureID: %s, createStateQueue: %t",
		req.GetFuncName(), req.GetStateID(), taskCtx.TraceID(), sncFuture.ID, req.GetCreateState())
	taskCtx.FutureStore().Add(sncFuture)

	go func() {
		result := req.invokeLocal(taskCtx, sncFuture)
		sncFuture.SetResponse(result)
	}()
	return &Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	}
}
