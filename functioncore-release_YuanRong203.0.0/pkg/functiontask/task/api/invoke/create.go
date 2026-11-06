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
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// CreateStateQueue creates state queue.
func (req *TaskCrateStateQueueRequest) CreateStateQueue(taskCtx taskcontext.Context) *Response {
	createStateErr := taskCtx.StateQueueMap().CreateStateQueue(&metadata.Metadata{
		TenantID:  taskCtx.TenantID(),
		ServiceID: taskCtx.ServiceID(),
		StateID:   req.StateID,
	})
	if createStateErr != nil {
		log.GetLogger().Errorf("createState, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			req.GetStateID(), createStateErr.Code(), createStateErr.Error())
		return &Response{
			Code:    createStateErr.Code(),
			Message: createStateErr.Error(),
		}
	}
	return &Response{
		Code: common.EOK,
	}
}
