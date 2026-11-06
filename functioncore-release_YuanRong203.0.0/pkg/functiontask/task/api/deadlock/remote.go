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

package deadlock

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// RemoteChecker checks deadlock in remote function task.
type RemoteChecker struct {
	Checker
	TaskCtx taskcontext.Context
	StateID string
}

// Start starts to check deadlock.
func (c *RemoteChecker) Start(req *StartRequest) *StartResponse {
	log.GetLogger().Infof("remoteCheckerStart, req: %s", req.String())
	resp := new(StartResponse)
	util.CallTaskWrapper(c.TaskCtx, common.MethodTaskStartCheckAndResolveDeadlock, req, resp)

	log.GetLogger().Debugf("remoteCheckerStartDone, resp: %s", resp.String())
	return resp
}

// End ends to check deadlock.
func (c *RemoteChecker) End(req *EndRequest) *EndResponse {
	log.GetLogger().Infof("remoteCheckerEnd, req: %s", req.String())
	resp := new(EndResponse)
	util.CallTaskWrapper(c.TaskCtx, common.MethodTaskEndCheckAndResolveDeadlock, req, resp)
	log.GetLogger().Debugf("remoteCheckerEndDone, resp: %s", resp.String())
	return resp
}
