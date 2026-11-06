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

// Package readstate api
package readstate

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// ReadState by key or stateID and return state.
func (req *Request) ReadState(taskCtx taskcontext.Context) *Response {
	log.GetLogger().Debugf("readState, traceID: %s, serviceID: %s, stateID: %s", taskCtx.TraceID(),
		taskCtx.ServiceID(), req.GetStateID())
	if !config.Conf.PersistentState {
		rawState, err := taskCtx.StateQueueMap().GetState(&metadata.Metadata{
			TenantID:  taskCtx.TenantID(),
			ServiceID: taskCtx.ServiceID(),
			StateID:   req.StateID,
		})
		if err != nil {
			log.GetLogger().Errorf("readState, traceID: %s, serviceID: %s, stateID: %s, code: %d, err: %s",
				taskCtx.TraceID(), taskCtx.ServiceID(), req.GetStateID(), err.Code(), err.Error())
			return &Response{
				Code:    err.Code(),
				Message: err.Error(),
			}
		}
		return &Response{
			State: rawState,
		}
	}
	state, err := taskCtx.StateClient().Get(
		metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, req.StateID))
	if err != nil {
		log.GetLogger().Errorf("readState, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			req.GetStateID(), err.Code(), err.Error())
		return &Response{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	queueMap := taskCtx.StateQueueMap()
	if queueMap == nil {
		return &Response{
			Code:    common.EStateClient,
			Message: "state queue map is nil",
		}
	}
	queueMap.UpdateState(&metadata.Metadata{
		TenantID:  taskCtx.TenantID(),
		ServiceID: taskCtx.ServiceID(),
		StateID:   req.StateID,
	}, state)

	return &Response{
		State: state,
	}
}
