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

package getfuture

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestGetFutureCallee(t *testing.T) {
	Convey("Test GetFuture Calee", t, func() {
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		var req = &getfuture.TaskRequest{
			FutureID: "testFutureID",
			StateID:  md.StateID,
		}
		sncFuture := snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: md.ServiceID,
			FuncName:  common.SetTimeoutFuncName,
			StateID:   req.StateID,
			ReqIDList: taskCtx.ReqIDList(),
			ID:        req.FutureID,
		})
		sncFuture.SetResponse(&common.Response{
			Code:    0,
			Message: "test",
		})
		taskCtx.FutureStore().Add(sncFuture)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Callee(taskCtx)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.Message, ShouldEqual, "test")
	})
	Convey("Test GetFuture Calee without sncFuture", t, func() {
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		var req = &getfuture.TaskRequest{
			FutureID: "testFutureID",
			StateID:  md.StateID,
		}
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Callee(taskCtx)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.Message, ShouldEqual, common.TaskGetFutureForwardMsg)
	})
}
