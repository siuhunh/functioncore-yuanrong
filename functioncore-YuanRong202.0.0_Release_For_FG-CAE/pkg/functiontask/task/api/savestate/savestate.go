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

// Package savestate api
package savestate

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// SaveState saves current state.
func (req *Request) SaveState(taskCtx taskcontext.Context) *Response {
	log.GetLogger().Infof("save state begin, traceID: %s, serviceID: %s, stateID: %s", taskCtx.TraceID(),
		taskCtx.ServiceID(), req.GetStateID())
	q := taskCtx.StateQueueMap().GetQueueWithRecover(&metadata.Metadata{
		TenantID:  taskCtx.TenantID(),
		ServiceID: taskCtx.ServiceID(),
		StateID:   req.StateID,
	})

	if q == nil {
		return &Response{
			Code:    common.EStateIDNotFound,
			Message: "state queue without such ID: " + req.StateID,
		}
	}
	ttl := q.GetTTL()
	q.SetState(req.State)
	saveStateErr := taskCtx.StateClient().Set(
		metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, req.StateID),
		req.State, ttl)
	if saveStateErr != nil {
		log.GetLogger().Errorf("save state failed, traceID: %s, serviceID: %s, stateID: %s, code: %d, err: %s",
			taskCtx.TraceID(), taskCtx.ServiceID(), req.GetStateID(), saveStateErr.Code(), saveStateErr.Error())
		return &Response{
			Code:    saveStateErr.Code(),
			Message: saveStateErr.Error(),
		}
	}
	log.GetLogger().Infof("save state end, traceID: %s, serviceID: %s, stateID: %s", taskCtx.TraceID(),
		taskCtx.ServiceID(), req.GetStateID())
	return &Response{
		Code: common.EOK,
	}
}
