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
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// TaskGetFuture get the future from another task.
func TaskGetFuture(taskCtx taskcontext.Context, futureID string, stateID string) *common.Response {
	req := &TaskRequest{
		FutureID: futureID,
		StateID:  stateID,
	}
	resp := new(TaskResponse)
	util.CallTaskWrapper(taskCtx, common.MethodTaskGetFuture, req, resp)
	log.GetLogger().Debugf("result, traceID: %s, code: %d, message: %s, futureID: %s", taskCtx.TraceID(),
		resp.GetCode(), resp.GetMessage(), futureID)
	return &common.Response{
		Code:        resp.Code,
		Message:     resp.Message,
		Result:      resp.Result,
		Log:         resp.Log,
		DcCallStack: resp.DcCallStack,
	}
}
