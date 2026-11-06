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
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/settimeout"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/test/functiontask/test/testcontext"
)

func TestSetTimeoutCaller(t *testing.T) {
	Convey("Test SetTimeout Caller", t, func() {
		taskCtx := testcontext.New(common.MethodSetTimeout)
		var md = &metadata.Metadata{
			TenantID:  "testTenantID",
			ServiceID: "testServiceID",
			StateID:   "testKey",
		}
		req := &settimeout.Request{
			StateID:         md.StateID,
			FutureID:        "testFut",
			Timeout:         0,
			CountOfFuncName: 0,
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Caller(taskCtx)
		time.Sleep(time.Second)
		So(resp.GetCode(), ShouldEqual, 0)
	})
}
