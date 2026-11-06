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

package adaptor

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func (a *Adaptor) checkerStart(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetDeadlockStartReq()
	log.GetLogger().ForDebugf("checkerStart, serviceID: %s, stateID: %s, traceID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID())

	checker := deadlock.LocalChecker{
		StateQueueMap: a.StateQueueMap,
	}
	resp := checker.Start(req)
	taskCtx.SetResult(resp)
}

func (a *Adaptor) checkerEnd(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetDeadlockEndReq()
	log.GetLogger().ForDebugf("checkerEnd, serviceID: %s, stateID: %s, traceID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID())

	checker := deadlock.LocalChecker{
		StateQueueMap: a.StateQueueMap,
	}
	resp := checker.End(req)
	taskCtx.SetResult(resp)
}

func (a *Adaptor) getDeadlockChecker(taskCtx taskcontext.Context, stateID string, local bool) deadlock.Checker {
	if !config.Conf.ResolveDeadlock {
		return new(deadlock.EmptyChecker)
	}
	if local {
		return &deadlock.LocalChecker{
			StateQueueMap: a.StateQueueMap,
		}
	}
	return &deadlock.RemoteChecker{
		StateID: stateID,
		TaskCtx: taskCtx,
	}
}
