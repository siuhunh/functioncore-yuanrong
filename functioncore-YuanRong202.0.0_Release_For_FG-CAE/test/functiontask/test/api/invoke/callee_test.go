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
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestInvokeCallee(t *testing.T) {
	Convey("Test Invoke Callee", t, func() {
		config.Conf.ResolveDeadlock = false
		taskCtx := testcontext.New(common.MethodInvoke)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		var req = &invoke.TaskRequest{
			FuncName:         "testFunc",
			Version:          "",
			Payload:          "",
			StateID:          md.StateID,
			FutureID:         "testFutureID",
			CreateStateQueue: false,
		}

		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Callee(taskCtx)
		So(resp.GetCode(), ShouldEqual, common.EOK)
	})
}
func TestSetCallback(t *testing.T) {
	Convey("TestSetCallback, failed as not in state queue", t, func() {
		taskCtx := testcontext.New(common.MethodInvoke)
		stateID := "xx1"
		req := &invoke.OnCompleteRequest{
			OnCompleteRequestPb: invoke.OnCompleteRequestPb{
				StateID: "xx1",
			},
		}
		md := snfuture.Metadata{
			StateID: stateID,
		}
		f := snfuture.NewInvoke(md)
		f.SetResponse(&common.Response{})
		res := invoke.OnComplete(taskCtx, req, f)
		So(res.GetCode(), ShouldEqual, common.EOK)
		resp := taskCtx.FutureStore().Get(res.FutureID)
		So(resp.GetResponse().Code, ShouldEqual, common.EStateIDNotFound)
	})

}
