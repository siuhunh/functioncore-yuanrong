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

package fsm

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// RecoverFromMemoryFutureStore recovers the request from the memory future store.
func RecoverFromMemoryFutureStore(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State {
	curFuture := taskCtx.FutureStore().Get(sncFuture.ID)
	log.GetLogger().ForDebugf("RecoverFromMemoryFutureStore, traceID: %s", taskCtx.TraceID())
	if curFuture != nil {
		// The future exists in the memory future store.
		resp := curFuture.PeekResponse()
		if resp != nil && resp.Code != 0 {
			// The future is completed but the result is incorrect.
			log.GetLogger().Infof("inMemoryFutureStore.notRecovered, traceID: %s, futureID: %s, code: %d",
				taskCtx.TraceID(), sncFuture.ID, resp.Code)
			curFuture.ClearResponse()
			return StateNotRecovered(curFuture)
		}
		// 1. The future is not completed.
		// 2. The future is completed and the result is correct.
		log.GetLogger().Infof("inMemoryFutureStore.recover, traceID: %s, futureID: %s",
			taskCtx.TraceID(), sncFuture.ID)
		return StateRecovered(curFuture)
	}
	// The future does not exist in the memory future store.
	return StateRecoverFromExtFutureStore
}

// RecoverFromExtFutureStore recovers the request from the external future store.
func RecoverFromExtFutureStore(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State {
	log.GetLogger().ForDebugf("RecoverFromExtFutureStore, traceId: %s, sncFuture ID: %v", taskCtx.TraceID(), sncFuture.ID)
	recoverFuture := taskCtx.EFS().GetRequest(taskCtx.TenantID(), taskCtx.ServiceID(),
		sncFuture.ID, taskCtx.ReqIDList())
	if recoverFuture != nil {
		// The future exists in the external future store.
		resp := recoverFuture.PeekResponse()
		if resp != nil && resp.Code == 0 {
			// The future is completed and the result is correct.
			log.GetLogger().Infof("efs.recover, traceID: %s, futureID: %s", taskCtx.TraceID(), sncFuture.ID)
			taskCtx.FutureStore().Add(recoverFuture)
			return StateRecovered(recoverFuture)
		}
		// 1. The future is not completed.
		// 2. The future is completed but the result is incorrect.
		log.GetLogger().Debugf("efs.notRecovered, traceID: %s, futureID: %s", taskCtx.TraceID(), sncFuture.ID)
		taskCtx.FutureStore().Add(sncFuture)
		return StateNotRecovered(sncFuture)
	}
	// The future does not exist in the external future store.
	return StateSaveRequest
}

// SaveRequest saves request in redis when first time call.
func SaveRequest(payload string, reqFutureID string) Handler {
	return func(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State {
		log.GetLogger().ForDebugf("efs.save, traceID: %s, futureID: %s", taskCtx.TraceID(), sncFuture.ID)
		err := taskCtx.EFS().SetRequest(taskCtx.TenantID(), taskCtx.ServiceID(), sncFuture, reqFutureID)
		if err != nil {
			return StateError(common.ESetMetadata, err.Error())
		}
		taskCtx.FutureStore().Add(sncFuture)
		return StateNotRecovered(sncFuture)
	}
}

// SaveRequestInMemory saves request in memory.
func SaveRequestInMemory() Handler {
	return func(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State {
		taskCtx.FutureStore().Add(sncFuture)
		return StateNotRecovered(sncFuture)
	}
}

func checkFuture(taskCtx taskcontext.Context, req AbstractRequest, recoverFuture *snfuture.Future) State {
	if recoverFuture == nil {
		return nil
	}
	if req.GetStateID() == taskcontext.NewState && recoverFuture.StateID != "" {
		log.GetLogger().Infof("recover.newState, traceID: %s, futureID: %s", taskCtx.TraceID(), recoverFuture.ID)
		req.SetStateID(recoverFuture.StateID)
		return nil
	}
	// The future exists in the future store.
	resp := recoverFuture.PeekResponse()
	if resp != nil && resp.Code == 0 {
		// The future is completed and the result is correct.
		log.GetLogger().Infof("recover, traceID: %s, futureID: %s", taskCtx.TraceID(), recoverFuture.ID)
		taskCtx.FutureStore().Add(recoverFuture)
		return StateRecovered(recoverFuture)
	}
	return nil
}

// BeforeStateful update the state id.
func BeforeStateful(req AbstractRequest) Handler {
	return func(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State {
		curFuture := taskCtx.FutureStore().Get(sncFuture.ID)
		log.GetLogger().ForDebugf("inMemoryFutureStore.check, stateID: %s, traceID: %s, futureID: %s", req.GetStateID(),
			taskCtx.TraceID(), sncFuture.ID)
		if s := checkFuture(taskCtx, req, curFuture); s != nil {
			return s
		}
		recoverFuture := taskCtx.EFS().GetRequest(taskCtx.TenantID(), taskCtx.ServiceID(),
			sncFuture.ID, taskCtx.ReqIDList())
		log.GetLogger().Debugf("efs.check, stateID: %s, traceID: %s, futureID: %s", req.GetStateID(),
			taskCtx.TraceID(), sncFuture.ID)
		if s := checkFuture(taskCtx, req, recoverFuture); s != nil {
			return s
		}

		err := util.CreateStateIfNeeded(taskCtx, req)
		if err != nil {
			return StateError(err.Code(), err.Error())
		}
		sncFuture.StateID = req.GetStateID()
		err = taskCtx.EFS().SetRequest(taskCtx.TenantID(), taskCtx.ServiceID(), sncFuture, req.GetReqFutureID())
		if err != nil {
			return StateError(err.Code(), err.Error())
		}
		log.GetLogger().Debugf("efs.save, stateID: %s, traceID: %s, futureID: %s", req.GetStateID(),
			taskCtx.TraceID(), sncFuture.ID)
		taskCtx.FutureStore().Add(sncFuture)
		return StateStateful
	}
}
