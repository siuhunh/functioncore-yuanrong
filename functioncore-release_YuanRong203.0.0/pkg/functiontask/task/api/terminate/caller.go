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

package terminate

import (
	"fmt"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/fsm"
	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *Request) handlers() map[int]fsm.Handler {
	return map[int]fsm.Handler{
		statetype.RecoverFromMemoryFutureStore: fsm.RecoverFromMemoryFutureStore,
		statetype.RecoverFromExtFutureStore:    fsm.RecoverFromExtFutureStore,
		statetype.Recovered:                    req.recovered,
		statetype.SaveRequest:                  fsm.SaveRequest("", ""),
		statetype.NotRecovered:                 req.terminate,
	}
}

// Caller terminate the state.
func (req *Request) Caller(taskCtx taskcontext.Context) taskcontext.Serializable {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  common.TerminateFuncName,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
	})
	log.GetLogger().Debugf("caller, traceID: %s, serviceID: %s, stateID: %s, futureID:%s",
		taskCtx.TraceID(), taskCtx.ServiceID(), req.GetStateID(), sncFuture.ID)

	return fsm.Run(req.handlers(), statetype.RecoverFromMemoryFutureStore, taskCtx, sncFuture)
}

func (req *Request) recovered(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	resp := req.terminate(taskCtx, sncFuture)
	if _, ok := resp.Result().(*Response); !ok {
		fmt.Printf("invalid terminate resp: %#v", resp)
	}
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
	})
}

func (req *Request) terminate(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	resp := new(Response)
	taskCtx.CallTask(common.MethodTerminate, req, resp)
	if resp.Code == common.ScheduleInsErr {
		log.GetLogger().Debugf("terminate, funcName: %s", sncFuture.FuncName)
		stateHashKey := metadata.GeneStateHashKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName)
		stateKey := metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, req.StateID)
		exist, err := taskCtx.StateClient().IsStateIDInHash(stateHashKey, req.StateID)
		if err != nil || !exist {
			resp.SetCode(common.EStateIDNotFound)
			resp.SetMessage("state ID " + req.GetStateID() + " recycled or not exists, failed to call")
		} else {
			// Delete stateID failed, ignore
			taskCtx.StateClient().DeleteState(stateKey, req.FuncName)
			resp.SetCode(common.EOK)
		}
	}
	if resp.Code != common.EOK {
		sncFuture.SetResponse(&common.Response{
			Code:    resp.Code,
			Message: resp.Message,
		})
		log.GetLogger().Errorf("terminate, traceID: %s, code: %d, msg: %s", taskCtx.TraceID(), resp.GetCode(),
			resp.GetMessage())
		return fsm.StateError(resp.Code, resp.Message)
	}
	go func() {
		resp := getfuture.TaskGetFuture(taskCtx, sncFuture.ID, req.StateID)
		if resp.Code == common.EStateIDNotFound {
			log.GetLogger().Warnf("terminate.TaskGetFuture, code: %d, msg: %s", resp.Code, resp.Message)
			resp = &common.Response{
				Code:   common.EOK,
				Result: `{"result":"DELETE SUCCESSFULLY"}`,
			}
		}
		sncFuture.SetResponse(resp)
	}()
	log.GetLogger().Debugf("resp, futureID: %s", sncFuture.ID)
	return fsm.StateSuccess(&Response{
		Code:     resp.Code,
		Message:  resp.Message,
		FutureID: sncFuture.ID,
	})
}
