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

package terminate

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestTerminateCaller(t *testing.T) {
	Convey("Test terminate Caller", t, func() {
		taskCtx := testcontext.New(common.MethodTerminate)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &terminate.Request{
			StateID:         md.StateID,
			FutureID:        "testFut",
			CountOfFuncName: 0,
		}
		sncFuture := snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: md.ServiceID,
			FuncName:  common.SetTimeoutFuncName,
			StateID:   req.StateID,
			ReqIDList: taskCtx.ReqIDList(),
			ID:        req.FutureID,
		})
		taskCtx.FutureStore().Add(sncFuture)
		sncFuture.SetResponse(&common.Response{
			Code:    0,
			Message: "testMsg",
		})
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Caller(taskCtx)
		So(resp.GetCode(), ShouldEqual, 0)
	})
}

func TestTerminateRecovered(t *testing.T) {
	Convey("Test terminate recovered", t, func() {
		taskCtx := testcontext.New(common.MethodTerminate)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &terminate.Request{
			StateID: md.StateID,
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Caller(taskCtx)
		So(resp.GetCode(), ShouldEqual, common.EOK)
	})
}

func TestStruct(t *testing.T) {
	Convey("test terminate Fields", t, func() {
		req := &terminate.Response{
			FutureID: "testFut",
		}
		resp := req.Fields()
		So(resp[0].String, ShouldEqual, "testFut")
	})
}
