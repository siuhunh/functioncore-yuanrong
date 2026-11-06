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
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestGetFutureCaller(t *testing.T) {
	Convey("Test GetFuture Caller", t, func() {
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &getfuture.Request{
			FutureID: "testFutureID",
			StateID:  md.StateID,
			State:    "testState",
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
			Message: "hello",
		})
		taskCtx.FutureStore().Add(sncFuture)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp, _ := req.Caller(taskCtx, new(deadlock.EmptyChecker))
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.Message, ShouldEqual, "hello")
	})
	Convey("Test GetFuture Caller without sncFuture", t, func() {
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &getfuture.Request{
			FutureID: "testFutureID",
			StateID:  md.StateID,
			State:    "testState",
		}
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp, _ := req.Caller(taskCtx, new(deadlock.EmptyChecker))
		So(resp.Code, ShouldEqual, common.EInvalidParam)
	})
	Convey("Test GetFuture Caller without stateID", t, func() {
		config.Conf.InitStateClient = false
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &getfuture.Request{
			FutureID: "testFutureID",
			State:    "testState",
		}
		sncFuture := snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: md.ServiceID,
			FuncName:  common.SetTimeoutFuncName,
			ReqIDList: taskCtx.ReqIDList(),
			ID:        req.FutureID,
		})
		sncFuture.SetResponse(&common.Response{
			Code:    0,
			Message: "hello",
		})
		taskCtx.FutureStore().Add(sncFuture)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp, _ := req.Caller(taskCtx, new(deadlock.EmptyChecker))
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.State, ShouldEqual, "")
	})
	Convey("Test GetFuture Caller without resp", t, func() {
		taskCtx := testcontext.New(common.MethodGetFuture)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &getfuture.Request{
			FutureID: "testFutureID",
			StateID:  md.StateID,
			State:    "testState",
		}
		sncFuture := snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: md.ServiceID,
			FuncName:  common.SetTimeoutFuncName,
			StateID:   req.StateID,
			ReqIDList: taskCtx.ReqIDList(),
			ID:        req.FutureID,
		})
		taskCtx.FutureStore().Add(sncFuture)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		go func() {
			time.Sleep(time.Second)
			sncFuture.SetResponse(&common.Response{
				Code: 0,
			})
		}()
		resp, _ := req.Caller(taskCtx, new(deadlock.EmptyChecker))
		So(resp.Code, ShouldEqual, common.EOK)
	})
}
