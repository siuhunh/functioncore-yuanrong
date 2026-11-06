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
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// Caller readState.
func (req *Request) Caller(taskCtx taskcontext.Context) *Response {
	log.GetLogger().Debugf("caller, traceID: %s, serviceID: %s, stateID: %s", taskCtx.TraceID(), taskCtx.ServiceID(),
		req.GetStateID())

	if !config.Conf.PersistentState {
		resp := new(Response)
		util.CallTaskWrapper(taskCtx, common.MethodReadState, req, resp)
		return resp
	}

	state, err := taskCtx.StateClient().Get(metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(),
		metadata.StateIDPrefix, req.StateID))
	log.GetLogger().Debugf("Get state from redis, stateID: %s, state: %s", req.GetStateID(), state)
	if err != nil {
		log.GetLogger().Errorf("readState, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			req.GetStateID(), err.Code(), err.Error())
		return &Response{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	return &Response{
		State: state,
	}
}
