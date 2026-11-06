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
	"functioncore/pkg/functiontask/task/api/fsm"
	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *OnCompleteRequest) handlers() map[int]fsm.Handler {
	return map[int]fsm.Handler{
		statetype.Start:                        req.start,
		statetype.RecoverFromMemoryFutureStore: fsm.RecoverFromMemoryFutureStore,
		statetype.RecoverFromExtFutureStore:    fsm.RecoverFromExtFutureStore,
		statetype.Recovered:                    req.retrySuccess,
		statetype.SaveRequest:                  fsm.SaveRequest("", req.FutureID),
		statetype.NotRecovered:                 req.stateless,
		statetype.BeforeStateful:               fsm.BeforeStateful(req),
		statetype.Stateful:                     req.stateful,
	}
}

// Caller create the future and state for on complete.
func (req *OnCompleteRequest) Caller(taskCtx taskcontext.Context) (resp taskcontext.Serializable) {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  req.FuncName,
		Version:   req.Version,
		ReqIDList: taskCtx.ReqIDList(),
	})
	if config.Conf.PersistentFuture {
		c := taskCtx.Filter().Start(sncFuture.ID)
		if c != nil {
			return taskCtx.Filter().Get(c)
		}
		defer func() {
			taskCtx.Filter().Set(sncFuture.ID, resp)
		}()
	}
	req.create = req.StateID == taskcontext.NewState
	return fsm.Run(req.handlers(), statetype.Start, taskCtx, sncFuture)
}

func (req *OnCompleteRequest) start(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().Debugf("caller, funcName: %s, version: %s, stateID: %s, reqFutureID: %s, countOfFuncName: %d,"+
		"futureID: %s", req.GetFuncName(), req.GetVersion(), req.GetStateID(), req.GetFutureID(),
		req.GetCountOfFuncName(), sncFuture.ID)
	if req.StateID == "" {
		return fsm.StateRecoverFromMemoryFutureStore
	}
	return fsm.StateBeforeStateful
}

func (req *OnCompleteRequest) retrySuccess(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	})
}

func (req *OnCompleteRequest) stateless(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().Debugf("callerOnComplete, funcName: %s, stateID: %s, reqFutureID: %s, respFutureID: %s",
		req.GetFuncName(), req.GetStateID(), req.GetFutureID(), sncFuture.ID)
	requiredFuture := taskCtx.FutureStore().Get(req.FutureID)
	if requiredFuture == nil {
		log.GetLogger().Warnf("unknown futureID, futureID: %s", req.GetFutureID())
		return fsm.StateError(common.EInvalidParam, "unknown future id: "+req.FutureID)
	}
	go func() {
		result := onComplete(taskCtx, req, requiredFuture, sncFuture)
		sncFuture.SetResponse(result)
	}()
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	})
}

func (req *OnCompleteRequest) stateful(taskCtx taskcontext.Context, future *snfuture.Future) fsm.State {
	log.GetLogger().Debugf("callerOnComplete, create: %t, funcName: %s, stateID: %s, reqFutureID: %s, respFutureID: %s",
		req.GetCreateState(), req.GetFuncName(), req.GetStateID(), req.GetFutureID(), future.ID)
	requiredFuture := taskCtx.FutureStore().Get(req.FutureID)
	if requiredFuture == nil {
		log.GetLogger().Warnf("unknown futureID, futureID: %s", req.FutureID)
		return fsm.StateError(common.EInvalidParam, "unknown future id: "+req.FutureID)
	}
	go func() {
		reqFutureResp := waitSNFutureReady(taskCtx, requiredFuture)
		if reqFutureResp.Code != 0 {
			future.SetResponse(reqFutureResp)
			return
		}
		taskRequest := transformOnCompleteRequest(taskCtx, req, reqFutureResp.Result, future)
		taskRequest.invokeStateful(taskCtx, future)
	}()
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: future.ID,
		StateID:  req.StateID,
	})
}

func transformOnCompleteRequest(taskCtx taskcontext.Context, req *OnCompleteRequest,
	reqFutureValue common.Result, sncFuture *snfuture.Future) *TaskRequest {
	return &TaskRequest{
		FuncName:         req.FuncName,
		Version:          req.Version,
		StateID:          req.StateID,
		FutureID:         sncFuture.ID,
		CreateStateQueue: req.GetCreateState(),
		Payload:          reqFutureValue,
	}
}
