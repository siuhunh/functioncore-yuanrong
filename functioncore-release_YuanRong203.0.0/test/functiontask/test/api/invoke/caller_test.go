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

	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

func init() {
	config.Conf.PersistentState = false
	config.Conf.PersistentFuture = false
	config.Conf.InitStateClient = false
}

// TestCallerNoState checks whether the caller works correctly.
func TestCallerNoState(t *testing.T) {
	Convey("Caller no state", t, func(c C) {
		taskCtx := testcontext.New(-1)
		taskCtx.SetReqIDList(common.ReqIDList{common.NewTraceID()})
		req := &invoke.Request{RequestPb: invoke.RequestPb{
			FuncName: "hello",
			Payload:  "",
			StateID:  taskcontext.NewState,
		}}
		c.So(config.Conf.InitStateClient, ShouldEqual, false)
		rawResp := req.Caller(taskCtx)
		c.So(rawResp, ShouldResemble, &invoke.Response{
			Code:    common.FunctionWithNoState,
			Message: common.CodeText[common.FunctionWithNoState],
		})
	})
}

// TestCallerWithState checks whether the caller works correctly.
func TestCallerWithState(t *testing.T) {
	Convey("Caller with state", t, func(c C) {
		config.Conf.InitStateClient = true
		taskCtx := testcontext.New(-1)
		taskCtx.SetReqIDList(common.ReqIDList{common.NewTraceID()})
		req := &invoke.Request{RequestPb: invoke.RequestPb{
			FuncName: "hello",
			Payload:  "",
			StateID:  taskcontext.NewState,
		}}
		c.So(config.Conf.InitStateClient, ShouldEqual, true)
		rawResp := req.Caller(taskCtx)
		c.So(rawResp, ShouldNotBeNil)
	})
}
