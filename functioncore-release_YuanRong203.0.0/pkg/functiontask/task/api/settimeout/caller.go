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

// Package settimeout implements settimeout api.
package settimeout

import (
	"fmt"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/fsm"
	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *Request) handlers() map[int]fsm.Handler {
	return map[int]fsm.Handler{
		statetype.RecoverFromMemoryFutureStore: fsm.RecoverFromMemoryFutureStore,
		statetype.RecoverFromExtFutureStore:    fsm.RecoverFromExtFutureStore,
		statetype.Recovered:                    req.recovered,
		statetype.SaveRequest:                  fsm.SaveRequest("", ""),
		statetype.NotRecovered:                 req.setTimeout,
	}
}

// Caller terminate the state.
func (req *Request) Caller(taskCtx taskcontext.Context) taskcontext.Serializable {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  common.SetTimeoutFuncName,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
	})
	log.GetLogger().Debugf("caller, traceID: %s, stateID: %s, timeout: %d, futureID: %s", taskCtx.TraceID(),
		req.GetStateID(), req.GetTimeout(), sncFuture.ID)

	return fsm.Run(req.handlers(), statetype.RecoverFromMemoryFutureStore, taskCtx, sncFuture)
}

func (req *Request) recovered(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	resp := req.setTimeout(taskCtx, sncFuture)
	if _, ok := resp.Result().(*Response); !ok {
		fmt.Printf("invalid setTimeout resp: %#v", resp)
	}
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
	})
}

func (req *Request) setTimeout(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	req.FutureID = sncFuture.ID
	resp := new(Response)
	util.CallTaskWrapper(taskCtx, common.MethodSetTimeout, req, resp)
	if resp.Code != common.EOK {
		sncFuture.SetResponse(&common.Response{
			Code:    resp.Code,
			Message: resp.Message,
		})
		log.GetLogger().Errorf("setTimeout, code: %d, err: %s", resp.GetCode(), resp.GetMessage())
		return fsm.StateError(resp.Code, resp.Message)
	}
	go func() {
		resp := getfuture.TaskGetFuture(taskCtx, sncFuture.ID, req.StateID)
		sncFuture.SetResponse(resp)
	}()
	log.GetLogger().Debugf("resp, futureID: %s", sncFuture.ID)
	return fsm.StateSuccess(&Response{
		Code:     resp.Code,
		Message:  resp.Message,
		FutureID: sncFuture.ID,
	})
}
