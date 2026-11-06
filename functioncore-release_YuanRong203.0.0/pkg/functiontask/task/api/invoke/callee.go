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
	"functioncore/pkg/functiontask/task/api/fsm"
	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *TaskRequest) handlers() map[int]fsm.Handler {
	return map[int]fsm.Handler{
		statetype.RecoverFromMemoryFutureStore: fsm.RecoverFromMemoryFutureStore,
		statetype.RecoverFromExtFutureStore:    fsm.RecoverFromExtFutureStore,
		statetype.Recovered:                    req.recovered,
		statetype.SaveRequest:                  fsm.SaveRequestInMemory(),
		statetype.NotRecovered:                 req.invoke,
	}
}

// Callee for the the invoke request.
func (req *TaskRequest) Callee(taskCtx taskcontext.Context) (resp taskcontext.Serializable) {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  req.FuncName,
		Version:   req.Version,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
		ID:        req.FutureID,
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
	log.GetLogger().Debugf("callee, serviceID: %s, funcName: %s, version: %s, stateID: %s, traceID: %s, futureID: %s",
		taskCtx.ServiceID(), req.GetFuncName(), req.GetVersion(), req.GetStateID(), taskCtx.TraceID(), sncFuture.ID)

	return fsm.Run(req.handlers(), statetype.RecoverFromMemoryFutureStore, taskCtx, sncFuture)
}

func (req *TaskRequest) recovered(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	if !req.CreateStateQueue {
		return fsm.StateSuccess(&TaskResponse{
			Code: common.EOK,
		})
	}
	err := taskCtx.StateQueueMap().CreateStateQueue(&metadata.Metadata{
		TenantID:  taskCtx.TenantID(),
		ServiceID: taskCtx.ServiceID(),
		StateID:   req.StateID,
	})
	if err != nil {
		log.GetLogger().Errorf("request recovered, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			req.GetStateID(), err.Code(), err.Error())
		return fsm.StateError(err.Code(), err.Error())
	}
	return fsm.StateSuccess(&TaskResponse{
		Code: common.EOK,
	})
}

func (req *TaskRequest) invoke(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().Infof("invoke, funcName: %s, version: %s, stateID: %s, futureID: %s, traceID: %s",
		req.GetFuncName(), req.GetVersion(), req.GetStateID(), sncFuture.ID, taskCtx.TraceID())
	r, err := util.InvokeStatefulLocal(taskCtx, &util.StatefulReq{
		FuncName:         req.FuncName,
		Version:          req.Version,
		Payload:          req.Payload,
		StateID:          req.StateID,
		CreateStateQueue: req.CreateStateQueue,
		StateKey:         req.StateKey,
	}, sncFuture)

	// even if there is an err in Request, you can also GetResponse from request.
	// even if err occurs, you also need to set the future result.
	go util.ReturnResponseToTask(taskCtx, r, sncFuture)

	if err != nil {
		log.GetLogger().Errorf("invoke, funcName: %s, version: %s, stateID: %s, futureID: %s, traceID: %s, code: %d,"+
			"err: %s", req.GetFuncName(), req.GetVersion(), req.GetStateID(), sncFuture.ID, taskCtx.TraceID(),
			err.Code(), err.Error())
		return fsm.StateError(err.Code(), err.Error())
	}

	return fsm.StateSuccess(&TaskResponse{
		Code: common.EOK,
	})
}
