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
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functiontask/task/api/fsm"
	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (req *Request) handlers() map[int]fsm.Handler {
	return map[int]fsm.Handler{
		statetype.Start:                        req.start,
		statetype.RecoverFromMemoryFutureStore: fsm.RecoverFromMemoryFutureStore,
		statetype.RecoverFromExtFutureStore:    fsm.RecoverFromExtFutureStore,
		statetype.Recovered:                    req.recovered,
		statetype.SaveRequest:                  fsm.SaveRequest(req.Payload, ""),
		statetype.NotRecovered:                 req.stateless,
		statetype.BeforeStateful:               fsm.BeforeStateful(req),
		statetype.Stateful:                     req.stateful,
	}
}

// Caller for the the invoke request.
func (req *Request) Caller(taskCtx taskcontext.Context) (resp taskcontext.Serializable) {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  req.FuncName,
		Version:   req.Version,
		ReqIDList: taskCtx.ReqIDList(),
	})
	if !config.Conf.InitStateClient && (req.ByKey || req.StateID != "") {
		log.GetLogger().Errorf("traceID: %s, code: %d, msg: %s", taskCtx.TraceID(), common.FunctionWithNoState,
			common.CodeText[common.FunctionWithNoState])
		return &Response{
			Code:    common.FunctionWithNoState,
			Message: common.CodeText[common.FunctionWithNoState],
		}
	}
	if config.Conf.PersistentFuture {
		c := taskCtx.Filter().Start(sncFuture.ID)
		// it's a retry request if c != nil, we need to wait last request finished.
		if c != nil {
			return taskCtx.Filter().Get(c)
		}
		// set resp so that  next retry requests can return directly.
		defer func() {
			taskCtx.Filter().Set(sncFuture.ID, resp)
		}()
	}
	req.create = req.ByKey || req.StateID == taskcontext.NewState
	return fsm.Run(req.handlers(), statetype.Start, taskCtx, sncFuture)
}

func (req *Request) start(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	if req.ByKey {
		req.StateID = utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
	}
	log.GetLogger().ForDebugf("caller start, serviceID: %s, funcName: %s, version: %s, traceID: %s, stateID: %s,"+
		"countOfFuncName: %d, futureID: %s", taskCtx.ServiceID(), req.GetFuncName(), req.GetVersion(),
		taskCtx.TraceID(), req.GetStateID(), req.GetCountOfFuncName(), sncFuture.ID)

	if req.StateID == "" {
		return fsm.StateRecoverFromMemoryFutureStore
	}
	return fsm.StateBeforeStateful
}

func (req *Request) recovered(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().ForDebugf("request task resource recovered, traceId: %s, FutureID: %s, StateID: %s",
		taskCtx.TraceID(), sncFuture.ID, req.GetStateID())
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	})
}

func (req *Request) stateless(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().ForDebugf("stateless, traceID: %s, futureID: %s, isInvokeAll: %t",
		taskCtx.TraceID(), sncFuture.ID, req.IsInvokeAll)
	go func() {
		cfg := taskCtx.InvokeConfig(req.Version, req.CallKernel, sncFuture.ID, req.RequestAttributes)
		result := util.InvokeStateless(taskCtx, req.FuncName, req.Payload, req.InstanceID, cfg, req.ExtraMap, req.IsInvokeAll)
		err := taskCtx.EFS().SetResult(taskCtx.TenantID(), taskCtx.ServiceID(), sncFuture, result)
		if err != nil {
			log.GetLogger().Errorf("stateless invoke, traceID: %s, futureID: %s, code: %d, errMsg: %s",
				taskCtx.TraceID(), sncFuture.ID, err.Code(), err.Error())
			sncFuture.SetResponse(&common.Response{
				Code:    common.ESetValue,
				Message: err.Error(),
			})
			return
		}
		sncFuture.SetResponse(result)
		return
	}()
	return fsm.StateSuccess(&Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	})
}

// CreateState creates state for the request.
func (req *Request) stateful(taskCtx taskcontext.Context, sncFuture *snfuture.Future) fsm.State {
	log.GetLogger().ForDebugf("stateful, traceID: %s, futureID: %s", taskCtx.TraceID(), sncFuture.ID)
	result := transformRequest(req, sncFuture).invokeStateful(taskCtx, sncFuture)
	return fsm.StateSuccess(result)
}

func transformRequest(req *Request, sncFuture *snfuture.Future) *TaskRequest {
	return &TaskRequest{
		FuncName:         req.FuncName,
		Version:          req.Version,
		StateID:          req.StateID,
		FutureID:         sncFuture.ID,
		CreateStateQueue: req.GetCreateState(),
		Payload:          req.Payload,
		StateKey:         req.Key,
	}
}

func (req *TaskRequest) invokeStateful(taskCtx taskcontext.Context, sncFuture *snfuture.Future) *Response {
	resp := new(TaskResponse)
	taskCtx.CallTask(common.MethodTaskInvoke, req, resp)
	err := req.createStateMappingIfScheduleInsErr(taskCtx, resp)
	if err != nil {
		log.GetLogger().Infof("invoke stateful retry, stateID: %s, traceID: %s, code: %d, errMsg: %s",
			req.GetStateID(), taskCtx.TraceID(), err.Code(), err.Error())
		sncFuture.SetResponse(&common.Response{
			Code:    err.Code(),
			Message: err.Error(),
		})
		return &Response{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}

	go func() {
		resp := getfuture.TaskGetFuture(taskCtx, sncFuture.ID, req.StateID)
		if resp.Code == common.InitStateErr {
			resp = &common.Response{
				Code:    resp.Code,
				Message: resp.Message,
			}
			err := taskCtx.StateClient().DeleteStateValueAndStateMapping(req.StateID)
			if err != nil {
				log.GetLogger().Warnf("taskGetFuture, stateID: %s, traceID: %s, code: %d, errMsg: %s",
					req.GetStateID(), taskCtx.TraceID(), err.Code(), err.Error())
			}
		}
		sncFuture.SetResponse(resp)
	}()
	return &Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	}
}

func (req *TaskRequest) createStateMappingIfScheduleInsErr(
	taskCtx taskcontext.Context, resp *TaskResponse) common.TaskError {
	switch resp.Code {
	case common.EOK:
	case common.ScheduleInsErr:
		log.GetLogger().Infof("invokeStateful.retryStart, stateID: %s, traceID: %s, code: %d, errMsg: %s",
			req.GetStateID(), taskCtx.TraceID(), resp.GetCode(), resp.GetMessage())
		if !req.GetCreateStateQueue() {
			log.GetLogger().Infof("invokeStateful.StateExists, stateID: %s, traceID: %s",
				req.GetStateID(), taskCtx.TraceID())
			var exists bool
			exists, err := taskCtx.StateClient().StateExists(
				metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, req.StateID))
			if err != nil {
				return err
			}
			if !exists {
				err = common.NewTaskError(common.EStateIDNotFound,
					"state id "+req.StateID+" recycled or not exists, failed to call")
				return err
			}
		}
		log.GetLogger().Infof("invokeStateful.reCreateState, stateID: %s, traceID: %s",
			req.GetStateID(), taskCtx.TraceID())
		var createdStateID string
		createdStateID, err := util.CreateState(taskCtx, req.GetFuncName(), req.GetStateID())
		if err != nil {
			log.GetLogger().Errorf("createStateQueue, stateID: %s, traceID: %s, code: %d, errMsg: %s",
				req.GetStateID(), taskCtx.TraceID(), err.Code(), err.Error())
			return err
		}
		req.StateID = createdStateID
		taskCtx.CallTask(common.MethodTaskInvoke, req, resp)
		if resp.Code != common.EOK {
			err = common.NewTaskError(resp.Code, resp.Message)
		}
		return err
	default:
		err := common.NewTaskError(resp.Code, resp.Message)
		return err
	}
	return nil
}
