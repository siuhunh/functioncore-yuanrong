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

package settimeout

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// SetTimeout set the timeout of the state by state id.
func (req *Request) SetTimeout(taskCtx taskcontext.Context) *Response {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  common.SetTimeoutFuncName,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
		ID:        req.FutureID,
	})
	log.GetLogger().Debugf("setTimeout, traceID: %s, stateID: %s, timeout: %d, futureID: %s", taskCtx.TraceID(),
		req.GetStateID(), req.GetTimeout(), req.GetFutureID())

	success := taskCtx.FutureStore().SetIfNotExist(sncFuture)
	if !success {
		return &Response{
			Code:     common.EOK,
			FutureID: sncFuture.ID,
		}
	}

	r := common.NewSetTimeoutRequest(int(req.Timeout), sncFuture.ID)
	err := taskCtx.StateQueueMap().Request(&metadata.Metadata{
		TenantID:  taskCtx.TenantID(),
		ServiceID: taskCtx.ServiceID(),
		StateID:   req.StateID,
	}, r, false)
	go util.ReturnResponseToTask(taskCtx, r, sncFuture)

	if err != nil {
		return &Response{
			Code:     err.Code(),
			Message:  err.Error(),
			FutureID: sncFuture.ID,
		}
	}

	return &Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
	}
}
