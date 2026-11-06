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

// Package getfuture implements getfuture api.
package getfuture

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// Get the value of the future.
func (req *Request) Get(taskCtx taskcontext.Context, deadlockChecker deadlock.Checker) (*Response, string) {
	log.GetLogger().Debugf("get future, traceID: %s, futureID: %s, stateID: %s", taskCtx.TraceID(), req.GetFutureID(),
		req.GetStateID())
	resp, logResult := req.getFutureFromStore(taskCtx, deadlockChecker)
	return resp, logResult
}

func (req *Request) stateless(taskCtx taskcontext.Context, sncFuture *snfuture.Future) (*Response, string) {
	resp := sncFuture.GetResponse()
	return &Response{
		Code:    resp.Code,
		Message: resp.Message,
		Result:  resp.Result,
		State:   "",
	}, resp.Log
}
