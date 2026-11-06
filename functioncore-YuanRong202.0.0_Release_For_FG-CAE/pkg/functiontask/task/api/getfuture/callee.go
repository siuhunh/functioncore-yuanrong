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

package getfuture

import (
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

const (
	getFutureInternalSleep = 50
	getFutureMaxReq        = 20
)

// Callee for the the get future request.
func (req *TaskRequest) Callee(taskCtx taskcontext.Context) *TaskResponse {
	log.GetLogger().Debugf("callee, serviceID: %s, stateID: %s, traceID: %s, futureID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())
	var sncFuture *snfuture.Future
	for i := 0; i < getFutureMaxReq; i++ {
		sncFuture = taskCtx.FutureStore().Get(req.FutureID)
		if sncFuture != nil {
			break
		}
		time.Sleep(getFutureInternalSleep * time.Millisecond)
	}
	if sncFuture == nil {
		log.GetLogger().Warnf("unknown futureID, serviceID: %s, stateID: %s, traceID: %s, futureID: %s",
			taskCtx.ServiceID(), req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())
		sncFuture = snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: taskCtx.ServiceID(),
			StateID:   req.StateID,
			ReqIDList: taskCtx.ReqIDList(),
			ID:        req.FutureID,
		})
		sncFuture.SetResponse(&common.Response{
			Code:    common.EOK,
			StateID: req.StateID,
			Message: common.TaskGetFutureForwardMsg,
		})
	}
	resp := sncFuture.GetResponse()
	taskCtx.FutureStore().Delete(req.FutureID)
	return &TaskResponse{
		Code:        resp.Code,
		Message:     resp.Message,
		DcCallStack: resp.DcCallStack,
		Result:      resp.Result,
		Log:         resp.Log,
	}
}
