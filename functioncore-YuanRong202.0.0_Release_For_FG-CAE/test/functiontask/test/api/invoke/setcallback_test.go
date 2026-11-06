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
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestInvokeSetCallBack(t *testing.T) {
	Convey("Test invoke set callback", t, func() {
		taskCtx := testcontext.New(common.MethodOnComplete)
		var md = &metadata.Metadata{
			TenantID:  "setCbTID",
			ServiceID: "setCbSID",
			StateID:   "setCbKey",
		}
		req := &invoke.OnCompleteRequest{
			OnCompleteRequestPb: invoke.OnCompleteRequestPb{
				FuncName:        "setCbFunc",
				Version:         "",
				StateID:         md.StateID,
				FutureID:        "setCbFut",
				CountOfFuncName: 0,
			},
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
		sncFuture.SetResponse(&common.Response{
			Code: 0,
		})
		resp := invoke.OnComplete(taskCtx, req, sncFuture)
		So(resp.Code, ShouldEqual, common.EOK)
	})
}
