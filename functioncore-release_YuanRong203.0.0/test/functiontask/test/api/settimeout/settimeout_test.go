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

package settimeout

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/settimeout"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	_ "functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestSetTimeout(t *testing.T) {
	Convey("Test SetTimeout", t, func() {
		taskCtx := testcontext.New(common.MethodSetTimeout)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		req := &settimeout.Request{
			StateID:  md.StateID,
			FutureID: "testFutureID",
			Timeout:  30,
		}
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.SetTimeout(taskCtx)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureID, ShouldEqual, "testFutureID")
	})
	Convey("Test SetTimeout If Future Exist", t, func() {
		taskCtx := testcontext.New(common.MethodSetTimeout)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		req := &settimeout.Request{
			StateID:  md.StateID,
			FutureID: "testFutureID",
			Timeout:  30,
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
		resp := req.SetTimeout(taskCtx)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureID, ShouldEqual, "testFutureID")
	})
}
