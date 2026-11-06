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

// Package util implements util api.
package util

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// ReturnResponseToTask returns response to task.
func ReturnResponseToTask(taskCtx taskcontext.Context, r *common.Request, sncFuture *snfuture.Future) {
	resp := r.GetResponse()
	sncFuture.SetResponse(resp)
	log.GetLogger().Debugf("returnResponseToTask, code: %d, msg: %s, stateID: %s, traceID: %s",
		resp.Code, resp.Message, resp.StateID, taskCtx.TraceID())
}

// CreateStateIfNeeded returns stateID, createStateQueue, errResp.
// stateID will be updated when createStateQueue is true.
func CreateStateIfNeeded(taskCtx taskcontext.Context, req taskcontext.CreateStateRequest) common.TaskError {
	if req.GetCreateState() {
		stateID := req.GetStateID()
		funcName := req.GetFuncName()
		createdStateID, err := CreateState(taskCtx, funcName, stateID)
		if err != nil {
			log.GetLogger().Errorf("createState, traceID: %s, stateID: %s, createdStateID: %s, code: %d, err: %s",
				taskCtx.TraceID(), stateID, createdStateID, err.Code(), err.Error())
			return err
		}
		req.SetStateID(createdStateID)
	}
	return nil
}

// CreateState returns stateID, taskError
func CreateState(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
	serviceID := taskCtx.ServiceID()
	log.GetLogger().Debugf("core.CreateState start, serviceID: %s, funcName: %s, stateID: %s, traceID: %s", serviceID,
		funcName, stateID, taskCtx.TraceID())
	createdStateID, _, err := taskCtx.CreateState(funcName, stateID)
	if err != nil {
		log.GetLogger().Errorf("core.createState error,"+
			"funcName: %s, serviceID: %s, stateID: %s, traceID: %s, code: %d, err: %s",
			funcName, serviceID, stateID, taskCtx.TraceID(), err.Code(), err.Error())
		return createdStateID, err
	}
	log.GetLogger().Debugf("core.createState finish, serviceID: %s, funcName: %s, stateID: %s, traceID: %s", serviceID,
		funcName, stateID, taskCtx.TraceID())
	return createdStateID, nil
}

// InvokeStateless invokes stateless at local task.
func InvokeStateless(taskCtx taskcontext.Context,
	funcName string, params common.Params, config interface{}) *common.Response {
	return taskCtx.Invoker().Invoke(&common.InvokeArgs{
		InvokeConfig: config,
		FuncName:     funcName,
		Params:       params,
		StateID:      "",
		State:        "",
		ReqIDList:    taskCtx.ReqIDList(),
	})
}

// StatefulReq represents stateful request.
type StatefulReq struct {
	FuncName         string
	Version          string
	Payload          string
	StateID          string
	CreateStateQueue bool
	StateKey         string
}

// InvokeStatefulLocal invokes stateful at local task.
func InvokeStatefulLocal(taskCtx taskcontext.Context,
	statefulReq *StatefulReq, future *snfuture.Future) (*common.Request, common.TaskError) {
	msgConfig := taskCtx.InvokeConfig(statefulReq.Version, false, future.ID, nil)
	req := common.NewRequest(common.RequestOptions{
		FutureID:  future.ID,
		FuncName:  statefulReq.FuncName,
		ReqIDList: taskCtx.ReqIDList(),
		Params:    statefulReq.Payload,
		Config:    msgConfig,
	})
	err := taskCtx.StateQueueMap().Request(&metadata.Metadata{
		TenantID:     taskCtx.TenantID(),
		ServiceID:    taskCtx.ServiceID(),
		StateID:      statefulReq.StateID,
		StateKey:     statefulReq.StateKey,
		FunctionName: statefulReq.FuncName,
	}, req, statefulReq.CreateStateQueue)
	return req, err
}

// GetFuture returns the future by future id.
func GetFuture(taskCtx taskcontext.Context, futureID string) *snfuture.Future {
	sncFuture := taskCtx.FutureStore().Get(futureID)
	if sncFuture == nil {
		log.GetLogger().Warnf("inMemoryFutureStore.get, traceID: %s, futureID: %s", taskCtx.TraceID(), futureID)
		sncFuture = taskCtx.EFS().GetRequest(taskCtx.TenantID(), taskCtx.ServiceID(), futureID, taskCtx.ReqIDList())
		if sncFuture == nil {
			log.GetLogger().Warnf("unknownFutureID, traceID: %s, futureID: %s", taskCtx.TraceID(), futureID)
		}
	}
	return sncFuture
}

// CallTaskWrapper is Wrapper for Context.CallTask, deal with resp.
func CallTaskWrapper(taskCtx taskcontext.Context,
	method int32, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
	taskCtx.CallTask(method, req, resp)
	if resp.GetCode() == common.ScheduleInsErr {
		resp.SetCode(common.EStateIDNotFound)
		resp.SetMessage("state id " + req.GetStateID() + " recycled or not exists, failed to call")
		return
	}
}

// GetFuncStateKey returns stateKey, taskError
func GetFuncStateKey(taskCtx taskcontext.Context, funcName string, clientInstance bridgetypes.ClientInstance) (string,
	common.TaskError) {
	serviceID := taskCtx.ServiceID()
	createdStateKey, err := taskCtx.GetFuncStateKey(funcName, clientInstance)
	if err != nil {
		log.GetLogger().Errorf("core.GetFuncStateKey, funcName: %s, serviceID: %s, traceID: %s, createdStateKey: %s,"+
			"isNewStateKeyScenario: %t, code: %d, err: %s", funcName, serviceID, taskCtx.TraceID(), createdStateKey,
			taskCtx.IsNewStateKeyScenario(), err.Code(), err.Error())
		return createdStateKey, err
	}
	log.GetLogger().Debugf("core.GetFuncStateKey, funcName: %s, serviceID: %s, traceID: %s, createdStateKey: %s,"+
		"isNewStateKeyScenario: %t", funcName, serviceID, taskCtx.TraceID(), createdStateKey,
		taskCtx.IsNewStateKeyScenario())
	return createdStateKey, nil
}

// Release returns stateKey, taskError
func Release(taskCtx taskcontext.Context, funcName string, clientInstance bridgetypes.ClientInstance) (bool,
	common.TaskError) {
	serviceID := taskCtx.ServiceID()
	isTerminate, err := taskCtx.Release(funcName, clientInstance)
	if err != nil {
		log.GetLogger().Debugf("core.Release, traceID: %s, serviceID: %s, funcName: %s, isTerminate: %t, code: %d,"+
			"err: %s", taskCtx.TraceID(), serviceID, funcName, isTerminate, err.Code(), err.Error())
		return isTerminate, err
	}
	log.GetLogger().Infof("core.Release, traceID: %s, serviceID: %s, funcName: %s, isTerminate: %t", taskCtx.TraceID(),
		serviceID, funcName, isTerminate)
	return isTerminate, nil
}
