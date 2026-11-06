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
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/statequeue"
)

// LocalChecker checks deadlock in local function task.
type LocalChecker struct {
	StateQueueMap *statequeue.Map
	Checker
}

// Start starts to check deadlock.
func (c *LocalChecker) Start(req *StartRequest) *StartResponse {
	log.GetLogger().Infof("localCheckerStart, req: %s", req.String())
	deadlockID, err := c.StateQueueMap.StartCheckAndResolveDeadlock(&metadata.Metadata{
		TenantID:  req.TenantID,
		ServiceID: req.ServiceID,
		StateID:   req.StateID,
	}, req.State, ConvertLists2Array(req.FutureReqIDLists))
	if err != nil {
		log.GetLogger().Errorf("startCheckAndResolveDeadlock, err: %s", err.Error())
		return &StartResponse{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	log.GetLogger().Debugf("localCheckerStartDone, deadlockID: %s", deadlockID)
	return &StartResponse{
		Code:       common.EOK,
		DeadlockID: deadlockID,
	}
}

// End ends to check deadlock.
func (c *LocalChecker) End(req *EndRequest) *EndResponse {
	log.GetLogger().Infof("localCheckerEnd, req: %s", req.String())
	err := c.StateQueueMap.EndCheckAndResolveDeadlock(&metadata.Metadata{
		TenantID:  req.TenantID,
		ServiceID: req.ServiceID,
		StateID:   req.StateID,
	}, req.DeadlockID)
	if err != nil {
		log.GetLogger().Errorf("endCheckAndResolveDeadlock, err: %s", err.Error())
		return &EndResponse{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	rawState, err := c.StateQueueMap.GetState(&metadata.Metadata{
		TenantID:  req.TenantID,
		ServiceID: req.ServiceID,
		StateID:   req.StateID,
	})
	if err != nil {
		log.GetLogger().Errorf("getState, err: %s", err.Error())
		return &EndResponse{
			Code:    err.Code(),
			Message: err.Error(),
		}
	}
	log.GetLogger().Debugf("localCheckerEndDone, state: %s", rawState)
	return &EndResponse{
		Code:  common.EOK,
		State: rawState,
	}
}
