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

package reliability

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

func nodeGreen(c C) {

	taskCtx := testcontext.New(common.MethodOnComplete)
	taskCtx.DisableCreateState = false
	result := callbackForNodeTest(c, taskCtx, taskcontext.NewState)
	c.So(result.Code, ShouldEqual, common.EOK)
	c.So(result.Result, ShouldEqual, "hello")
}
func nodeLost(c C) {
	stateID := "xxx1"
	taskCtx := testcontext.New(common.MethodOnComplete)
	taskCtx.DisableCreateState = false
	_ = taskCtx.StateQueueMap().StateClient.Set(
		metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, stateID), stateID, 0)
	result := callbackForNodeTest(c, taskCtx, stateID)
	c.So(result.Code, ShouldEqual, common.EOK)
	c.So(result.Result, ShouldEqual, "hello")
}
func nodeLostAlways(c C) {
	createdStateID := "xxx2"
	taskCtx := testcontext.New(common.MethodOnComplete)
	taskCtx.DisableCreateState = true
	_ = taskCtx.StateQueueMap().StateClient.Set(
		metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, createdStateID),
		createdStateID, 0)
	result := callbackForNodeTest(c, taskCtx, createdStateID)
	c.So(result.Code, ShouldEqual, common.ScheduleInsErr)
}
func callbackForNodeTest(c C, taskCtx *testcontext.Context, stateID string) *getfuture.Response {
	taskCtx.SetSource(common.SourceRuntimeToLocalFunctionTask)
	taskCtx.SetFilter(taskcontext.NewFilter(), taskcontext.NewFilter())
	var funcName = "hello"
	f := snfuture.NewExternalEvent(funcName)
	taskCtx.FutureStore().Add(f)
	f.SetResponse(&common.Response{
		Code:    0,
		Message: "hello",
	})
	req := &invoke.OnCompleteRequest{
		OnCompleteRequestPb: invoke.OnCompleteRequestPb{
			FuncName:        funcName,
			Version:         "",
			FutureID:        f.ID,
			StateID:         stateID,
			CountOfFuncName: 0,
		},
	}
	rawResp := req.Caller(taskCtx)
	fmt.Printf("rawResp: %+v\n", rawResp)
	resp, ok := rawResp.(*invoke.Response)
	c.So(ok, ShouldBeTrue)
	getReq := &getfuture.RequestPb{
		FutureID: resp.FutureID,
		StateID:  "",
		State:    "",
	}
	result, _ := getReq.Get(taskCtx, new(deadlock.EmptyChecker))
	fmt.Printf("result: %+v\n", result)
	return result
}

// TestNodeLost test retry if node bent with state lost
func TestNodeLost(t *testing.T) {
	Convey("node lost test", t, func() {
		config.Conf.PersistentFuture = true
		config.Conf.PersistentState = true
		config.Conf.InitStateClient = false
		Convey("node is green", func(c C) {
			nodeGreen(c)
		})
		Convey("node is lost but can recover after retry", func(c C) {
			nodeLost(c)
		})
		Convey("node is lost always", func(c C) {
			nodeLostAlways(c)
		})
	})
}
