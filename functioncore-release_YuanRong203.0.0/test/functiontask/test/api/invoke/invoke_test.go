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
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/test/functiontask/test/testcontext"
)

// TestInvoke checks when there is a panic.
func TestInvoke(t *testing.T) {
	taskCtx := testcontext.New(common.MethodInvoke)
	Convey("Invoke a function", t, func() {
		invokeReq := &invoke.Request{RequestPb: invoke.RequestPb{
			FuncName:        "",
			Version:         "",
			Payload:         "",
			ByKey:           false,
			Key:             "",
			StateID:         "",
			CountOfFuncName: 0,
		}}
		invokeResp := invoke.Invoke(taskCtx, invokeReq)
		getReq := &getfuture.RequestPb{
			FutureID: invokeResp.FutureID,
			StateID:  "",
			State:    "",
		}
		getResp, _ := getReq.Get(taskCtx, new(deadlock.EmptyChecker))
		So(getResp.Code, ShouldEqual, common.ETimeout)
		So(getResp.Message, ShouldEqual, "timeout")
	})

	Convey("Invoke a function", t, func() {
		invokeReq := &invoke.Request{RequestPb: invoke.RequestPb{
			FuncName:        "",
			Version:         "",
			Payload:         "",
			ByKey:           false,
			Key:             "",
			StateID:         "test",
			CountOfFuncName: 0,
		}}
		invokeResp := invoke.Invoke(taskCtx, invokeReq)
		getReq := &getfuture.RequestPb{
			FutureID: invokeResp.FutureID,
			StateID:  "",
			State:    "",
		}
		getResp, _ := getReq.Get(taskCtx, new(deadlock.EmptyChecker))
		So(getResp.Code, ShouldEqual, 4041)
	})
}
